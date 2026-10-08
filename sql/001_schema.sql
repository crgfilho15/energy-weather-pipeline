CREATE SCHEMA IF NOT EXISTS dw;

-- dimension: one row per UTC hour (key shared by all fact tables)
CREATE TABLE IF NOT EXISTS dw.dim_datetime (
    ts_utc        timestamptz PRIMARY KEY,
    local_ts      timestamp   NOT NULL,
    local_date    date        NOT NULL,
    local_year    smallint    NOT NULL,
    local_month   smallint    NOT NULL CHECK (local_month BETWEEN 1 AND 12),
    local_hour    smallint    NOT NULL CHECK (local_hour BETWEEN 0 AND 23),
    local_weekday smallint    NOT NULL CHECK (local_weekday BETWEEN 0 AND 6),
    is_weekend    boolean     NOT NULL,
    is_dst        boolean     NOT NULL,
    holiday_name  text,
    is_holiday    boolean     NOT NULL
);

-- dimension: one row per weather location (requested point and grid point actually returned)
CREATE TABLE IF NOT EXISTS dw.dim_location (
    location_id   text PRIMARY KEY,
    name          text NOT NULL,
    requested_lat double precision NOT NULL,
    requested_lon double precision NOT NULL,
    grid_lat      double precision NOT NULL,
    grid_lon      double precision NOT NULL
);

-- fact: electricity load, one row per region and UTC hour
CREATE TABLE IF NOT EXISTS dw.fact_load_hourly (
    region            text        NOT NULL,
    ts_utc            timestamptz NOT NULL REFERENCES dw.dim_datetime (ts_utc),
    pjm_mw            double precision NOT NULL CHECK (pjm_mw > 0),
    local_label       timestamp   NOT NULL,
    row_id            integer     NOT NULL,
    dst_ambiguous     boolean     NOT NULL,
    baseline_mw       double precision,
    ratio_to_baseline double precision,
    dq_low_deviation  boolean     NOT NULL,
    PRIMARY KEY (region, ts_utc)
);

-- fact: weather, one row per location and UTC hour
CREATE TABLE IF NOT EXISTS dw.fact_weather_hourly (
    location_id   text        NOT NULL REFERENCES dw.dim_location (location_id),
    ts_utc        timestamptz NOT NULL REFERENCES dw.dim_datetime (ts_utc),
    temperature_c double precision NOT NULL,
    humidity_pct  double precision CHECK (humidity_pct BETWEEN 0 AND 100),
    wind_kmh      double precision CHECK (wind_kmh >= 0),
    precip_mm     double precision CHECK (precip_mm >= 0),
    raw_key       text        NOT NULL,
    PRIMARY KEY (location_id, ts_utc)
);

-- reconciliation log: rows expected from the curated layer vs rows found in the table
CREATE TABLE IF NOT EXISTS dw.load_audit (
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    table_name    text        NOT NULL,
    rows_source   integer     NOT NULL,
    rows_in_table integer     NOT NULL,
    matched       boolean     NOT NULL
);
