-- =============================================================================
-- hive/scripts/01_create_tables.hql
-- Stage 8A: Create Hive External Tables pointing to Parquet files on HDFS
-- =============================================================================
-- Prerequisites:
--   1. HDFS path /data/airline_delay/parquet/ contains the 12 monthly Parquet files
--      uploaded via: hdfs dfs -put dataset/processed/parquet/* /data/airline_delay/parquet/
--   2. Hive Metastore running (hive --service metastore &)
--   3. Run: hive -f hive/scripts/01_create_tables.hql
-- =============================================================================

-- Use (or create) the working database
CREATE DATABASE IF NOT EXISTS airline_delay
  COMMENT 'BTS 2025 On-Time Performance Analytics'
  LOCATION '/data/airline_delay';

USE airline_delay;

-- ---------------------------------------------------------------------------
-- 1. MAIN FACT TABLE: flights_clean
--    External table — Hive reads Parquet directly (no data copy)
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS flights_clean (
  event_id              STRING    COMMENT 'SHA-256 surrogate key (16-char hex)',
  flight_date           STRING    COMMENT 'Flight date YYYY-MM-DD',
  airline               STRING    COMMENT 'Reporting carrier IATA code',
  flight_num            INT       COMMENT 'Flight number',
  tail_num              STRING    COMMENT 'Aircraft tail number',
  origin                STRING    COMMENT 'Origin airport IATA code',
  dest                  STRING    COMMENT 'Destination airport IATA code',
  crs_dep               INT       COMMENT 'Scheduled departure time HHMM',
  dep_time              INT       COMMENT 'Actual departure time HHMM',
  dep_delay             DOUBLE    COMMENT 'Departure delay in minutes',
  dep_delay_15          INT       COMMENT '1 if departure delay >= 15 min',
  crs_arr               INT       COMMENT 'Scheduled arrival time HHMM',
  arr_time              INT       COMMENT 'Actual arrival time HHMM',
  arr_delay             DOUBLE    COMMENT 'Arrival delay in minutes',
  arr_delay_15          INT       COMMENT '1 if arrival delay >= 15 min',
  cancelled             INT       COMMENT '1 = cancelled flight',
  cancellation_code     STRING    COMMENT 'A=Carrier B=Weather C=NAS D=Security',
  diverted              INT       COMMENT '1 = diverted flight',
  crs_elapsed           INT       COMMENT 'Scheduled elapsed time (minutes)',
  actual_elapsed        INT       COMMENT 'Actual elapsed time (minutes)',
  air_time              INT       COMMENT 'Air time (wheels-up to wheels-down)',
  distance              DOUBLE    COMMENT 'Flight distance in miles',
  carrier_delay         DOUBLE    COMMENT 'Minutes of carrier-caused delay',
  weather_delay         DOUBLE    COMMENT 'Minutes of weather-caused delay',
  nas_delay             DOUBLE    COMMENT 'Minutes of NAS-caused delay',
  security_delay        DOUBLE    COMMENT 'Minutes of security-caused delay',
  late_aircraft_delay   DOUBLE    COMMENT 'Minutes of late aircraft delay',
  dep_time_fixed        INT       COMMENT 'Corrected dep time (2400 rollover resolved)',
  is_2400_rollover      BOOLEAN   COMMENT 'TRUE if original CRSDepTime was 2400',
  quarantine_reason     STRING    COMMENT 'Blank for clean records'
)
STORED AS PARQUET
LOCATION '/data/airline_delay/parquet'
TBLPROPERTIES (
  'parquet.compress'      = 'SNAPPY',
  'serialization.null.format' = ''
);

-- ---------------------------------------------------------------------------
-- 2. MONTHLY AGGREGATION TABLE (populated by Stage 8B analytics query)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monthly_stats (
  month               INT,
  total_flights       BIGINT,
  cancelled_count     BIGINT,
  cancel_rate_pct     DOUBLE,
  mean_dep_delay      DOUBLE,
  median_dep_delay    DOUBLE,
  p95_dep_delay       DOUBLE,
  mean_arr_delay      DOUBLE,
  pct_delayed_15plus  DOUBLE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ---------------------------------------------------------------------------
-- 3. CARRIER AGGREGATION TABLE
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS carrier_stats (
  airline              STRING,
  total_flights        BIGINT,
  cancel_rate_pct      DOUBLE,
  mean_dep_delay       DOUBLE,
  mean_arr_delay       DOUBLE,
  p95_arr_delay        DOUBLE,
  pct_delayed_15plus   DOUBLE,
  carrier_delay_avg    DOUBLE,
  weather_delay_avg    DOUBLE,
  nas_delay_avg        DOUBLE,
  late_aircraft_avg    DOUBLE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ---------------------------------------------------------------------------
-- 4. ROUTE PERFORMANCE TABLE
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS route_stats (
  route               STRING,
  origin              STRING,
  dest                STRING,
  total_flights       BIGINT,
  mean_arr_delay      DOUBLE,
  cancel_rate_pct     DOUBLE,
  mean_distance       DOUBLE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ---------------------------------------------------------------------------
-- 5. DELAY CAUSE BREAKDOWN TABLE
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS delay_cause_monthly (
  month               INT,
  carrier_delay_avg   DOUBLE,
  weather_delay_avg   DOUBLE,
  nas_delay_avg       DOUBLE,
  security_delay_avg  DOUBLE,
  late_aircraft_avg   DOUBLE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

SELECT 'Hive tables created successfully.' AS status;
