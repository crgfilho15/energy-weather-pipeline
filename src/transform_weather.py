import json
import logging
import math
from datetime import datetime, timezone
import io

import pandas as pd

from src import config
from src.extract_weather import load_locations, s3_key
from src.storage import get_s3_client, put_json

CURATED_PREFIX = "curated/source=open_meteo/dataset=weather_hourly"
REPORT_KEY = "quality/source=open_meteo/dataset=weather_hourly/report.json"
START_YEAR, END_YEAR = 2002, 2018
COLUMNS = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "wind_speed_10m": "wind_kmh",
    "precipitation": "precip_mm",
}
# physical plausibility limits (hard checks)
LIMITS = {
    "temperature_c": (-60, 60),
    "humidity_pct": (0, 100),
    "wind_kmh": (0, 250),
    "precip_mm": (0, 500),
}


def distance_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def read_location_year(s3, location, year):
    key = s3_key(location["location_id"], year)
    body = json.loads(s3.get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read())
    data = body["response"]
    if data.get("timezone") != "GMT" or data.get("utc_offset_seconds") != 0:
        raise ValueError(f"{key}: expected GMT, got {data.get('timezone')}")

    hourly = pd.DataFrame(data["hourly"]).rename(columns=COLUMNS)
    hourly["ts_utc"] = pd.to_datetime(hourly.pop("time"), utc=True)
    numeric = list(COLUMNS.values())
    hourly[numeric] = hourly[numeric].astype("float64")
    hourly.insert(0, "location_id", location["location_id"])
    hourly["grid_lat"] = float(data["latitude"])
    hourly["grid_lon"] = float(data["longitude"])
    hourly["raw_key"] = key
    return hourly


def run_checks(df, locations):
    problems = []
    if df["ts_utc"].isna().any():
        problems.append("null timestamps")
    if df.duplicated(["location_id", "ts_utc"]).any():
        problems.append("duplicated (location, hour)")
    if df["temperature_c"].isna().any():
        problems.append("null temperatures")
    for col, (low, high) in LIMITS.items():
        values = df[col].dropna()
        if ((values < low) | (values > high)).any():
            problems.append(f"{col} outside [{low}, {high}]")

    expected = pd.date_range(
        f"{START_YEAR}-01-01", f"{END_YEAR}-12-31 23:00", freq="h", tz="UTC"
    )
    for location_id, part in df.groupby("location_id"):
        missing = expected.difference(pd.DatetimeIndex(part["ts_utc"]))
        if len(missing):
            problems.append(f"{location_id}: {len(missing)} missing hours")
    if len(df) != len(expected) * len(locations):
        problems.append("row count does not reconcile")

    if problems:
        raise ValueError("; ".join(problems))
    return expected


def write_curated(s3, df):
    df = df.copy()
    df["year"] = df["ts_utc"].dt.year
    keys = []
    for year, part in df.groupby("year"):
        buffer = io.BytesIO()
        part.drop(columns=["year"]).to_parquet(buffer, index=False)
        key = f"{CURATED_PREFIX}/year={year}/part-0.parquet"
        s3.put_object(Bucket=config.S3_BUCKET, Key=key, Body=buffer.getvalue())
        keys.append(key)
    return keys


def build_report(df, locations, expected):
    requested = {loc["location_id"]: loc for loc in locations}
    rows = []
    for location_id, part in df.groupby("location_id"):
        req = requested[location_id]
        grid_lat = float(part["grid_lat"].iloc[0])
        grid_lon = float(part["grid_lon"].iloc[0])
        rows.append(
            {
                "location_id": location_id,
                "requested_lat": float(req["latitude"]),
                "requested_lon": float(req["longitude"]),
                "grid_lat": grid_lat,
                "grid_lon": grid_lon,
                "grid_points_seen": int(part[["grid_lat", "grid_lon"]].drop_duplicates().shape[0]),
                "distance_km": round(
                    distance_km(float(req["latitude"]), float(req["longitude"]), grid_lat, grid_lon), 1
                ),
                "rows": int(len(part)),
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(df)),
        "expected_rows": int(len(expected) * len(locations)),
        "period_utc": [expected.min().isoformat(), expected.max().isoformat()],
        "null_counts": {c: int(df[c].isna().sum()) for c in COLUMNS.values()},
        "value_ranges": {c: [float(df[c].min()), float(df[c].max())] for c in COLUMNS.values()},
        "locations": rows,
        "assumptions": [
            "API timestamps are GMT (checked in every response)",
            "values are gridded estimates from weather models, not station observations (confirm wording in the Open-Meteo docs)",
        ],
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s3 = get_s3_client(config.AWS_PROFILE, config.AWS_REGION)
    locations = load_locations()

    frames = [
        read_location_year(s3, location, year)
        for location in locations
        for year in range(START_YEAR, END_YEAR + 1)
    ]
    df = pd.concat(frames, ignore_index=True)

    expected = run_checks(df, locations)
    keys = write_curated(s3, df)
    report = build_report(df, locations, expected)
    put_json(s3, config.S3_BUCKET, REPORT_KEY, report)

    logging.info("curated files written: %d", len(keys))
    logging.info("rows=%d (expected %d)", report["rows"], report["expected_rows"])
    print("\nnull counts:", report["null_counts"])
    print("value ranges:", report["value_ranges"])
    print(pd.DataFrame(report["locations"]).to_string(index=False))


if __name__ == "__main__":
    main()
