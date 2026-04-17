/* @bruin
name: quality.validate_marts
type: duckdb.sql
connection: weather_warehouse
depends:
  - quality.validate_raw_daily
  - quality.validate_stations
  - marts.mart_daily_weather
  - marts.mart_monthly_summary
  - marts.mart_prediction_features
materialization:
  type: table
  strategy: create+replace

description: >
  Post-transform quality validation. Verifies mart tables meet
  downstream requirements for dashboards and ML feature engineering.

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
  - name: "all mart checks pass"
    description: "No critical failures in mart data quality"
    query: SELECT count(*) = 0 FROM quality.validate_marts WHERE check_result = 'FAIL'
    value: 1
@bruin */

WITH

daily_weather_populated AS (
    SELECT 'mart_daily_weather_populated' AS check_name,
        COUNT(*) AS violation_count,
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
        'mart_daily_weather should contain data' AS description
    FROM marts.mart_daily_weather
),

rolling_avgs_exist AS (
    SELECT 'rolling_averages_computed' AS check_name,
        COUNT(*) FILTER (WHERE tavg_7d_avg IS NOT NULL) AS violation_count,
        CASE WHEN COUNT(*) FILTER (WHERE tavg_7d_avg IS NOT NULL) > 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
        '7-day rolling averages should be computed' AS description
    FROM marts.mart_daily_weather
),

prediction_targets AS (
    SELECT 'prediction_targets_exist' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) > 1000 THEN 'PASS'
            WHEN COUNT(*) > 0    THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Prediction feature table should have 1000+ rows' AS description
    FROM marts.mart_prediction_features
),

monthly_coverage AS (
    SELECT 'monthly_summary_coverage' AS check_name,
        COUNT(DISTINCT obs_year * 100 + obs_month) AS violation_count,
        CASE
            WHEN COUNT(DISTINCT obs_year * 100 + obs_month) >= 12 THEN 'PASS'
            WHEN COUNT(DISTINCT obs_year * 100 + obs_month) >= 3  THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Monthly summaries should cover at least 12 months' AS description
    FROM marts.mart_monthly_summary
),

stations_with_data AS (
    SELECT 'stations_joined_correctly' AS check_name,
        COUNT(*) FILTER (WHERE station_name IS NULL) AS violation_count,
        CASE
            WHEN COUNT(*) FILTER (WHERE station_name IS NULL) = 0   THEN 'PASS'
            WHEN COUNT(*) FILTER (WHERE station_name IS NULL) < 100 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'All daily weather records should have a station name' AS description
    FROM marts.mart_daily_weather
)

SELECT check_name, violation_count, check_result, description, NOW() AS checked_at FROM daily_weather_populated
UNION ALL SELECT *, NOW() FROM rolling_avgs_exist
UNION ALL SELECT *, NOW() FROM prediction_targets
UNION ALL SELECT *, NOW() FROM monthly_coverage
UNION ALL SELECT *, NOW() FROM stations_with_data
