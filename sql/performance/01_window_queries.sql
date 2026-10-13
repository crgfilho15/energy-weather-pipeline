\echo === Q1) weather for a 4-day window (all locations) ===
EXPLAIN (ANALYZE, BUFFERS)
SELECT w.location_id,
       round(avg(w.temperature_c)::numeric, 1) AS avg_temp_c,
       max(w.wind_kmh)                         AS max_wind_kmh
FROM dw.fact_weather_hourly w
WHERE w.ts_utc >= '2012-10-28 00:00+00' AND w.ts_utc < '2012-11-01 00:00+00'
GROUP BY w.location_id;

\echo
\echo === Q2) load joined with weather for the same window ===
EXPLAIN (ANALYZE, BUFFERS)
SELECT l.ts_utc, l.pjm_mw, round(avg(w.temperature_c)::numeric, 1) AS avg_temp_c
FROM dw.fact_load_hourly l
JOIN dw.fact_weather_hourly w USING (ts_utc)
WHERE l.ts_utc >= '2012-10-28 00:00+00' AND l.ts_utc < '2012-11-01 00:00+00'
GROUP BY l.ts_utc, l.pjm_mw;
