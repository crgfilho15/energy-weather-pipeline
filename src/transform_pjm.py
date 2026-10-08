import io
import logging
from datetime import datetime, timezone

import pandas as pd

from src import config
from src.storage import get_s3_client, put_json

RAW_KEY = "raw/source=pjm/region=pjme/PJME_hourly.csv"
CURATED_PREFIX = "curated/source=pjm/dataset=load_hourly"
REPORT_KEY = "quality/source=pjm/dataset=load_hourly/report.json"
LOCAL_TZ = "America/New_York"
# heuristic threshold calibrated on this dataset: a flag is a deviation, not an error
LOW_RATIO = 0.75
# same weekday-hour: 4 weeks before + 4 weeks after + the row itself
BASELINE_WINDOW = 9


def read_raw(s3):
    body = s3.get_object(Bucket=config.S3_BUCKET, Key=RAW_KEY)["Body"].read()
    df = pd.read_csv(io.BytesIO(body))
    if list(df.columns) != ["Datetime", "PJME_MW"]:
        raise ValueError(f"unexpected columns: {list(df.columns)}")
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    df["row_id"] = range(len(df))
    return df


def to_utc(df):
    label = df["Datetime"]
    # in repeated labels, the first row in file order is daylight time (assumption)
    first_of_label = (df.groupby("Datetime")["row_id"].rank(method="first") == 1).to_numpy()
    # assumption: the label marks the END of the hour, so the hour starts 1h earlier
    start_local = (label - pd.Timedelta(hours=1)).dt.tz_localize(
        LOCAL_TZ, ambiguous=first_of_label, nonexistent="raise"
    )
    return pd.DataFrame(
        {
            "ts_utc": start_local.dt.tz_convert("UTC"),
            "local_label": label,
            "pjm_mw": df["PJME_MW"].astype("float64"),
            "row_id": df["row_id"],
            "dst_ambiguous": label.duplicated(keep=False).to_numpy(),
        }
    )


def run_checks(out):
    problems = []
    if out["ts_utc"].isna().any():
        problems.append("null timestamps after conversion")
    if out["ts_utc"].duplicated().any():
        problems.append("duplicated UTC instants")
    if out["pjm_mw"].isna().any():
        problems.append("null load values")
    if (out["pjm_mw"] <= 0).any():
        problems.append("non-positive load values")
    if problems:
        raise ValueError("; ".join(problems))


def find_gaps(out):
    full = pd.date_range(out["ts_utc"].min(), out["ts_utc"].max(), freq="h")
    missing = full.difference(pd.DatetimeIndex(out["ts_utc"]))
    if len(full) != len(out) + len(missing):
        raise ValueError("row counts do not reconcile")
    return full, missing


def add_deviation(out):
    out = out.sort_values("ts_utc").reset_index(drop=True)
    local = out["ts_utc"].dt.tz_convert(LOCAL_TZ)
    out["local_hour"] = local.dt.hour
    out["local_weekday"] = local.dt.dayofweek
    # baseline = median of the same weekday and hour in the neighbouring weeks
    out["baseline_mw"] = out.groupby(["local_weekday", "local_hour"])["pjm_mw"].transform(
        lambda s: s.rolling(BASELINE_WINDOW, center=True, min_periods=5).median()
    )
    out["ratio_to_baseline"] = out["pjm_mw"] / out["baseline_mw"]
    out["dq_low_deviation"] = out["ratio_to_baseline"] < LOW_RATIO
    return out


def write_curated(s3, out):
    out = out.copy()
    out["year"] = out["ts_utc"].dt.year
    keys = []
    for year, part in out.groupby("year"):
        buffer = io.BytesIO()
        part.drop(columns=["year"]).to_parquet(buffer, index=False)
        key = f"{CURATED_PREFIX}/year={year}/part-0.parquet"
        s3.put_object(Bucket=config.S3_BUCKET, Key=key, Body=buffer.getvalue())
        keys.append(key)
    return keys


def build_report(rows_raw, out, full, missing):
    low = out[out["dq_low_deviation"]]
    local_dates = low["ts_utc"].dt.tz_convert(LOCAL_TZ).dt.date
    top_days = local_dates.value_counts().head(10)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_raw": int(rows_raw),
        "rows_curated": int(len(out)),
        "period_utc": [out["ts_utc"].min().isoformat(), out["ts_utc"].max().isoformat()],
        "expected_hourly_instants": int(len(full)),
        "missing_hourly_instants": int(len(missing)),
        "missing_utc": [ts.isoformat() for ts in missing],
        "dst_ambiguous_rows": int(out["dst_ambiguous"].sum()),
        "low_deviation_rule": (
            f"pjm_mw < {LOW_RATIO} x median of the same local weekday-hour "
            "in the 4 weeks before and after"
        ),
        "low_deviation_rows": int(len(low)),
        "low_deviation_days": int(local_dates.nunique()),
        "top_days_by_flagged_hours": {d.isoformat(): int(n) for d, n in top_days.items()},
        "assumptions": [
            "label marks the end of the hour (confirmed by consistency test, not documented by source)",
            "first row of a repeated label is daylight time (assumption, affects 4 hours)",
            "low-deviation flags mark unusual hours vs neighbouring weeks (holidays, storms), not errors",
        ],
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s3 = get_s3_client(config.AWS_PROFILE, config.AWS_REGION)

    raw = read_raw(s3)
    out = to_utc(raw)
    run_checks(out)
    full, missing = find_gaps(out)
    out = add_deviation(out)

    keys = write_curated(s3, out)
    report = build_report(len(raw), out, full, missing)
    put_json(s3, config.S3_BUCKET, REPORT_KEY, report)

    logging.info("curated files written: %d", len(keys))
    logging.info(
        "rows raw=%d curated=%d | missing UTC hours=%d | low deviation rows=%d on %d days",
        len(raw), len(out), len(missing), report["low_deviation_rows"], report["low_deviation_days"],
    )
    print("\nflagged hours by local day (top 10):", report["top_days_by_flagged_hours"])
    print(
        out.nsmallest(8, "ratio_to_baseline")[
            ["ts_utc", "local_label", "pjm_mw", "baseline_mw", "ratio_to_baseline"]
        ].round({"baseline_mw": 0, "ratio_to_baseline": 2}).to_string(index=False)
    )


if __name__ == "__main__":
    main()
