-- secondary indexes on the time key: the primary keys start with location/region,
-- so a filter or join on ts_utc alone cannot use them efficiently
CREATE INDEX IF NOT EXISTS ix_fact_weather_ts ON dw.fact_weather_hourly (ts_utc);
CREATE INDEX IF NOT EXISTS ix_fact_load_ts    ON dw.fact_load_hourly (ts_utc);
ANALYZE dw.fact_weather_hourly;
ANALYZE dw.fact_load_hourly;
