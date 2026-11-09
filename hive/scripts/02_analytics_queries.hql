-- =============================================================================
-- hive/scripts/02_analytics_queries.hql
-- Stage 8B: Core Analytics — Populate aggregation tables from flights_clean
-- =============================================================================
-- Run after 01_create_tables.hql:
--   hive -f hive/scripts/02_analytics_queries.hql
-- Set execution engine to Tez for performance:
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET mapreduce.job.reduces=4;

USE airline_delay;

-- ===========================================================================
-- QUERY 1: Monthly Statistics → monthly_stats
-- ===========================================================================
INSERT OVERWRITE TABLE monthly_stats
SELECT
  MONTH(flight_date)                                               AS month,
  COUNT(*)                                                         AS total_flights,
  SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)                  AS cancelled_count,
  ROUND(AVG(CASE WHEN cancelled = 1 THEN 100.0 ELSE 0.0 END), 4)  AS cancel_rate_pct,
  ROUND(AVG(CASE WHEN cancelled = 0 THEN dep_delay END), 4)        AS mean_dep_delay,
  ROUND(PERCENTILE_APPROX(
        CASE WHEN cancelled = 0 THEN dep_delay END, 0.5), 4)       AS median_dep_delay,
  ROUND(PERCENTILE_APPROX(
        CASE WHEN cancelled = 0 THEN dep_delay END, 0.95), 4)      AS p95_dep_delay,
  ROUND(AVG(CASE WHEN cancelled = 0 AND diverted = 0
                 THEN arr_delay END), 4)                           AS mean_arr_delay,
  ROUND(AVG(CASE WHEN dep_delay >= 15 THEN 100.0 ELSE 0.0 END), 4) AS pct_delayed_15plus
FROM flights_clean
WHERE flight_date IS NOT NULL
GROUP BY MONTH(flight_date)
ORDER BY month;

SELECT 'monthly_stats populated.' AS status;

-- ===========================================================================
-- QUERY 2: Carrier Statistics → carrier_stats
-- ===========================================================================
INSERT OVERWRITE TABLE carrier_stats
SELECT
  airline,
  COUNT(*)                                                         AS total_flights,
  ROUND(AVG(CASE WHEN cancelled = 1 THEN 100.0 ELSE 0.0 END), 4)  AS cancel_rate_pct,
  ROUND(AVG(CASE WHEN cancelled = 0 THEN dep_delay END), 4)        AS mean_dep_delay,
  ROUND(AVG(CASE WHEN cancelled = 0 AND diverted = 0
                 THEN arr_delay END), 4)                           AS mean_arr_delay,
  ROUND(PERCENTILE_APPROX(
        CASE WHEN cancelled = 0 AND diverted = 0
             THEN arr_delay END, 0.95), 4)                         AS p95_arr_delay,
  ROUND(AVG(CASE WHEN dep_delay >= 15 THEN 100.0 ELSE 0.0 END), 4) AS pct_delayed_15plus,
  ROUND(AVG(carrier_delay), 4)                                     AS carrier_delay_avg,
  ROUND(AVG(weather_delay), 4)                                     AS weather_delay_avg,
  ROUND(AVG(nas_delay), 4)                                         AS nas_delay_avg,
  ROUND(AVG(late_aircraft_delay), 4)                               AS late_aircraft_avg
FROM flights_clean
WHERE airline IS NOT NULL
GROUP BY airline
ORDER BY total_flights DESC;

SELECT 'carrier_stats populated.' AS status;

-- ===========================================================================
-- QUERY 3: Route Performance → route_stats (Top 100 routes by volume)
-- ===========================================================================
INSERT OVERWRITE TABLE route_stats
SELECT
  CONCAT(origin, '-', dest)                                        AS route,
  origin,
  dest,
  COUNT(*)                                                         AS total_flights,
  ROUND(AVG(CASE WHEN cancelled = 0 AND diverted = 0
                 THEN arr_delay END), 4)                           AS mean_arr_delay,
  ROUND(AVG(CASE WHEN cancelled = 1 THEN 100.0 ELSE 0.0 END), 4)  AS cancel_rate_pct,
  ROUND(AVG(distance), 2)                                          AS mean_distance
FROM flights_clean
WHERE origin IS NOT NULL AND dest IS NOT NULL
GROUP BY CONCAT(origin, '-', dest), origin, dest
ORDER BY total_flights DESC
LIMIT 100;

SELECT 'route_stats populated.' AS status;

-- ===========================================================================
-- QUERY 4: Delay Cause Breakdown by Month → delay_cause_monthly
-- ===========================================================================
INSERT OVERWRITE TABLE delay_cause_monthly
SELECT
  MONTH(flight_date)                 AS month,
  ROUND(AVG(carrier_delay), 4)       AS carrier_delay_avg,
  ROUND(AVG(weather_delay), 4)       AS weather_delay_avg,
  ROUND(AVG(nas_delay), 4)           AS nas_delay_avg,
  ROUND(AVG(security_delay), 4)      AS security_delay_avg,
  ROUND(AVG(late_aircraft_delay), 4) AS late_aircraft_avg
FROM flights_clean
WHERE cancelled = 0
  AND flight_date IS NOT NULL
GROUP BY MONTH(flight_date)
ORDER BY month;

SELECT 'delay_cause_monthly populated.' AS status;

-- ===========================================================================
-- QUERY 5: Spot-check — Validation against R Stage 7 monthly profile
-- ===========================================================================
SELECT
  'Hive vs R Validation Check' AS check_name,
  MONTH(flight_date)           AS month,
  COUNT(*)                     AS hive_total_flights,
  ROUND(AVG(CASE WHEN cancelled = 1 THEN 100.0 ELSE 0.0 END), 2) AS hive_cancel_pct
FROM flights_clean
GROUP BY MONTH(flight_date)
ORDER BY month;
