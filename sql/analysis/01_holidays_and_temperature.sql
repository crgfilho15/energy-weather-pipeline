\echo === A) holiday effect: load vs same weekday-hour in neighbouring weeks ===
SELECT d.holiday_name,
       count(DISTINCT d.local_date)                   AS days,
       round(avg(l.ratio_to_baseline)::numeric, 3)    AS avg_ratio,
       round(min(l.ratio_to_baseline)::numeric, 3)    AS min_ratio,
       round(100.0 * avg(l.dq_low_deviation::int), 1) AS pct_hours_flagged
FROM dw.fact_load_hourly l
JOIN dw.dim_datetime d USING (ts_utc)
WHERE d.is_holiday AND l.ratio_to_baseline IS NOT NULL
GROUP BY d.holiday_name
ORDER BY avg_ratio;

\echo
\echo === B) load by temperature, controlling for hour (15:00 local, working days) ===
WITH temp AS (
    SELECT ts_utc, avg(temperature_c) AS temp_c
    FROM dw.fact_weather_hourly
    GROUP BY ts_utc
)
SELECT floor(t.temp_c / 5) * 5          AS temp_bin_c,
       count(*)                         AS obs,
       round(avg(l.pjm_mw)::numeric)    AS avg_load_mw
FROM dw.fact_load_hourly l
JOIN dw.dim_datetime d USING (ts_utc)
JOIN temp t USING (ts_utc)
WHERE d.local_hour = 15 AND NOT d.is_weekend AND NOT d.is_holiday
GROUP BY 1
HAVING count(*) >= 20
ORDER BY 1;

\echo
\echo === C) does temperature relative to neighbouring weeks explain the deviation from baseline? ===
WITH daily AS (
    SELECT d.local_date, d.local_weekday, d.local_month,
           avg(w.temperature_c)     AS temp_c,
           avg(l.ratio_to_baseline) AS ratio
    FROM dw.dim_datetime d
    JOIN dw.fact_weather_hourly w USING (ts_utc)
    JOIN dw.fact_load_hourly l USING (ts_utc)
    WHERE NOT d.is_holiday
    GROUP BY 1, 2, 3
),
anom AS (
    SELECT *,
           temp_c - avg(temp_c) OVER (
               PARTITION BY local_weekday
               ORDER BY local_date
               ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING
           ) AS temp_anomaly_c
    FROM daily
)
SELECT CASE WHEN local_month IN (6, 7, 8)  THEN '1 summer (Jun-Aug)'
            WHEN local_month IN (12, 1, 2) THEN '2 winter (Dec-Feb)'
            ELSE '3 spring/autumn' END                AS season,
       count(*)                                       AS days,
       round(corr(temp_anomaly_c, ratio)::numeric, 2) AS corr_temp_anomaly_vs_ratio
FROM anom
GROUP BY 1
ORDER BY 1;
