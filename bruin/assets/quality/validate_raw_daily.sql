/* @bruin
name: quality.validate_raw_daily
type: postgres.sql
depends:
  - raw.ghcn_daily_ingest
materialization:
  type: table
  strategy: create+replace

description: >
  Data quality validation for the raw GHCN daily observations.
  Creates a summary table of quality metrics that downstream
  consumers and dashboards can monitor.

columns:
  - name: check_name
    type: string
    checks:
      - name: not_null
  - name: check_result
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - PASS
          - WARN
          - FAIL

custom_checks:
  - name: "no critical failures"
    description: "All quality checks should pass or warn, never fail"
    query: SELECT count(*) = 0 FROM quality.validate_raw_daily WHERE check_result = 'FAIL'
    value: 1
@bruin */

WITH

-- Check 1: Null station IDs
null_stations AS (
    SELECT
        'null_station_id' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS check_result,
        'Station ID should never be null' AS description
    FROM raw.ghcn_daily
    WHERE station_id IS NULL
),

-- Check 2: Null observation dates
null_dates AS (
    SELECT
        'null_obs_date' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS check_result,
        'Observation date should never be null' AS description
    FROM raw.ghcn_daily
    WHERE obs_date IS NULL
),

-- Check 3: Future dates
future_dates AS (
    SELECT
        'future_dates' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            WHEN COUNT(*) < 100 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Observation dates should not be in the future' AS description
    FROM raw.ghcn_daily
    WHERE obs_date > CURRENT_DATE
),

-- Check 4: Temperature range sanity (-90°C to 60°C → -900 to 600 tenths)
extreme_temps AS (
    SELECT
        'extreme_temperatures' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            WHEN COUNT(*) < 50 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Temperatures outside -90 to 60°C range' AS description
    FROM raw.ghcn_daily
    WHERE element IN ('TMAX', 'TMIN', 'TAVG')
      AND (data_value < -900 OR data_value > 600)
),

-- Check 5: Negative precipitation
negative_precip AS (
    SELECT
        'negative_precipitation' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS check_result,
        'Precipitation values should not be negative' AS description
    FROM raw.ghcn_daily
    WHERE element = 'PRCP'
      AND data_value < 0
),

-- Check 6: TMAX < TMIN for the same station/date
temp_inversion AS (
    SELECT
        'temp_max_less_than_min' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            WHEN COUNT(*) < 100 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'TMAX should be >= TMIN for same station and date' AS description
    FROM (
        SELECT station_id, obs_date,
            MAX(CASE WHEN element = 'TMAX' THEN data_value END) AS tmax,
            MAX(CASE WHEN element = 'TMIN' THEN data_value END) AS tmin
        FROM raw.ghcn_daily
        WHERE element IN ('TMAX', 'TMIN')
        GROUP BY station_id, obs_date
        HAVING MAX(CASE WHEN element = 'TMAX' THEN data_value END) <
               MAX(CASE WHEN element = 'TMIN' THEN data_value END)
    ) inversions
),

-- Check 7: Data freshness — most recent data should be within 7 days
data_freshness AS (
    SELECT
        'data_freshness' AS check_name,
        EXTRACT(DAY FROM CURRENT_DATE - MAX(obs_date))::INT AS violation_count,
        CASE
            WHEN MAX(obs_date) >= CURRENT_DATE - INTERVAL '7 days' THEN 'PASS'
            WHEN MAX(obs_date) >= CURRENT_DATE - INTERVAL '30 days' THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Most recent data should be within 7 days' AS description
    FROM raw.ghcn_daily
),

-- Check 8: Orphan stations (observations for stations not in metadata)
orphan_stations AS (
    SELECT
        'orphan_stations' AS check_name,
        COUNT(DISTINCT d.station_id) AS violation_count,
        CASE
            WHEN COUNT(DISTINCT d.station_id) = 0 THEN 'PASS'
            WHEN COUNT(DISTINCT d.station_id) < 50 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Observations should reference known stations' AS description
    FROM raw.ghcn_daily d
    LEFT JOIN raw.ghcn_stations s ON d.station_id = s.station_id
    WHERE s.station_id IS NULL
)

SELECT check_name, violation_count, check_result, description, NOW() AS checked_at
FROM null_stations
UNION ALL SELECT *, NOW() FROM null_dates
UNION ALL SELECT *, NOW() FROM future_dates
UNION ALL SELECT *, NOW() FROM extreme_temps
UNION ALL SELECT *, NOW() FROM negative_precip
UNION ALL SELECT *, NOW() FROM temp_inversion
UNION ALL SELECT *, NOW() FROM data_freshness
UNION ALL SELECT *, NOW() FROM orphan_stations
