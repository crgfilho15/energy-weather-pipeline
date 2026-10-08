import io
import logging
from pathlib import Path

import pandas as pd
import psycopg

from src import config
from src.extract_weather import load_locations
from src.storage import get_s3_client

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "sql" / "001_schema.sql"
CALENDAR_KEY = "curated/source=calendar/dataset=dim_datetime/part-0.parquet"
PJM_PREFIX = "curated/source=pjm/dataset=load_hourly"
WEATHER_PREFIX = "curated/source=open_meteo/dataset=weather_hourly"
YEARS = range(2002, 2019)
REGION = "PJME"

# (table, frame name, primary key columns) in dependency order: dimensions first
TABLES = [
    ("dw.dim_datetime", "dim_datetime", ["ts_utc"]),
    ("dw.dim_location", "dim_location", ["location_id"]),
    ("dw.fact_load_hourly", "fact_load_hourly", ["region", "ts_utc"]),
    ("dw.fact_weather_hourly", "fact_weather_hourly", ["location_id", "ts_utc"]),
]


def read_parquet(s3, key):
    body = s3.get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def read_years(s3, prefix):
    parts = [read_parquet(s3, f"{prefix}/year={year}/part-0.parquet") for year in YEARS]
    return pd.concat(parts, ignore_index=True)


def build_frames(s3):
    calendar = read_parquet(s3, CALENDAR_KEY)
    dim_datetime = calendar[
        [
            "ts_utc", "local_ts", "local_date", "local_year", "local_month", "local_hour",
            "local_weekday", "is_weekend", "is_dst", "holiday_name", "is_holiday",
        ]
    ]

    load = read_years(s3, PJM_PREFIX)
    fact_load = load[
        [
            "ts_utc", "pjm_mw", "local_label", "row_id", "dst_ambiguous",
            "baseline_mw", "ratio_to_baseline", "dq_low_deviation",
        ]
    ].copy()
    fact_load.insert(0, "region", REGION)

    weather = read_years(s3, WEATHER_PREFIX)
    fact_weather = weather[
        ["location_id", "ts_utc", "temperature_c", "humidity_pct", "wind_kmh", "precip_mm", "raw_key"]
    ]
    grid = weather.groupby("location_id")[["grid_lat", "grid_lon"]].first().reset_index()
    requested = pd.DataFrame(load_locations()).rename(
        columns={"latitude": "requested_lat", "longitude": "requested_lon"}
    )
    requested[["requested_lat", "requested_lon"]] = requested[["requested_lat", "requested_lon"]].astype(float)
    dim_location = requested.merge(grid, on="location_id", how="left")[
        ["location_id", "name", "requested_lat", "requested_lon", "grid_lat", "grid_lon"]
    ]

    return {
        "dim_datetime": dim_datetime,
        "dim_location": dim_location,
        "fact_load_hourly": fact_load,
        "fact_weather_hourly": fact_weather,
    }


def upsert(conn, table, df, key_columns):
    """Bulk load through a temp table, then INSERT ... ON CONFLICT so reruns do not duplicate rows."""
    columns = list(df.columns)
    updates = [c for c in columns if c not in key_columns]
    tmp = "tmp_" + table.split(".")[-1]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        with cur.copy(f"COPY {tmp} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)") as copy:
            while chunk := buffer.read(1 << 20):
                copy.write(chunk)
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in updates)
        cur.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"SELECT {', '.join(columns)} FROM {tmp} "
            f"ON CONFLICT ({', '.join(key_columns)}) DO UPDATE SET {set_clause}"
        )


def audit(conn, table, rows_source):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        rows_in_table = cur.fetchone()[0]
        matched = rows_in_table == rows_source
        cur.execute(
            "INSERT INTO dw.load_audit (table_name, rows_source, rows_in_table, matched) "
            "VALUES (%s, %s, %s, %s)",
            (table, rows_source, rows_in_table, matched),
        )
    return matched


def load_all(conn, frames):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
    conn.commit()

    for table, name, key_columns in TABLES:
        upsert(conn, table, frames[name], key_columns)
        logging.info("upserted %s: %d rows", table, len(frames[name]))
    conn.commit()

    results = [audit(conn, table, len(frames[name])) for table, name, _ in TABLES]
    conn.commit()
    return all(results)


def connect():
    missing = [n for n in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD") if not getattr(config, n)]
    if missing:
        raise RuntimeError(f"missing settings in .env: {missing}")
    return psycopg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s3 = get_s3_client(config.AWS_PROFILE, config.AWS_REGION)
    frames = build_frames(s3)
    with connect() as conn:
        ok = load_all(conn, frames)
    logging.info("reconciliation %s", "OK" if ok else "FAILED")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
