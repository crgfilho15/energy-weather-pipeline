\echo === daily deviation from baseline, with and without adjusting for temperature ===
CREATE OR REPLACE TEMP VIEW daily_scored AS
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
    SELECT local_date, local_weekday, ratio,
           CASE WHEN local_month IN (6, 7, 8)  THEN 'summer'
                WHEN local_month IN (12, 1, 2) THEN 'winter'
                ELSE 'other' END AS season,
           temp_c - avg(temp_c) OVER (
               PARTITION BY local_weekday
               ORDER BY local_date
               ROWS BETWEEN 4 PRECEDING AND 4 FOLLOWING
           ) AS temp_anomaly_c
    FROM daily
    WHERE ratio IS NOT NULL
),
fit AS (
    SELECT *,
           regr_intercept(ratio, temp_anomaly_c) OVER (PARTITION BY season) AS a,
           regr_slope(ratio, temp_anomaly_c)     OVER (PARTITION BY season) AS b
    FROM anom
),
scored AS (
    SELECT local_date, local_weekday, season, ratio, temp_anomaly_c,
           ratio - (a + b * temp_anomaly_c) AS residual
    FROM fit
)
SELECT *,
       rank() OVER (ORDER BY ratio)    AS raw_rank,
       rank() OVER (ORDER BY residual) AS resid_rank
FROM scored;

\echo
\echo === D0) effect size: change in daily load ratio per +1 C vs neighbouring weeks ===
SELECT season,
       count(*)                                             AS days,
       round(regr_slope(ratio, temp_anomaly_c)::numeric, 4) AS ratio_per_degc,
       round(regr_r2(ratio, temp_anomaly_c)::numeric, 2)    AS r2
FROM daily_scored
GROUP BY season
ORDER BY season;

\echo
\echo === D1) of the 20 lowest raw-ratio days, how many stay among the 20 lowest after the temperature adjustment? ===
SELECT count(*) FILTER (WHERE raw_rank <= 20 AND resid_rank <= 20) AS still_in_top20,
       count(*) FILTER (WHERE raw_rank <= 20)                       AS lowest_raw_days
FROM daily_scored;

\echo
\echo === D2) 12 lowest days after the temperature adjustment ===
SELECT local_date,
       to_char(local_date, 'Dy')         AS dow,
       season,
       round(ratio::numeric, 3)          AS raw_ratio,
       raw_rank,
       round(temp_anomaly_c::numeric, 1) AS temp_anomaly_c,
       round(residual::numeric, 3)       AS residual,
       resid_rank
FROM daily_scored
WHERE resid_rank <= 12
ORDER BY resid_rank;
