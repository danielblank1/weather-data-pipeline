/* @bruin
name: quality.validate_stations
type: duckdb.sql
connection: weather_warehouse
depends:
  - raw.ghcn_stations
materialization:
  type: table
  strategy: create+replace

description: >
  Data quality validation for GHCN station metadata.

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
@bruin */

WITH

lat_range AS (
    SELECT 'latitude_out_of_range' AS check_name,
        COUNT(*) AS violation_count,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
        'Latitude must be between -90 and 90' AS description
    FROM raw.ghcn_stations WHERE latitude < -90 OR latitude > 90
),

lon_range AS (
    SELECT 'longitude_out_of_range' AS check_name,
        COUNT(*) AS violation_count,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
        'Longitude must be between -180 and 180' AS description
    FROM raw.ghcn_stations WHERE longitude < -180 OR longitude > 180
),

duplicate_ids AS (
    SELECT 'duplicate_station_ids' AS check_name,
        COUNT(*) AS violation_count,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS check_result,
        'Station IDs should be unique' AS description
    FROM (
        SELECT station_id FROM raw.ghcn_stations
        GROUP BY station_id HAVING COUNT(*) > 1
    ) dupes
),

us_coverage AS (
    SELECT 'us_station_coverage' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) > 5000 THEN 'PASS'
            WHEN COUNT(*) > 1000 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'US should have 5000+ stations' AS description
    FROM raw.ghcn_stations WHERE LEFT(station_id, 2) = 'US'
),

elevation_range AS (
    SELECT 'elevation_out_of_range' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0  THEN 'PASS'
            WHEN COUNT(*) < 10 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Elevation should be between -500m and 9000m' AS description
    FROM raw.ghcn_stations
    WHERE elevation IS NOT NULL AND (elevation < -500 OR elevation > 9000)
)

SELECT check_name, violation_count, check_result, description, NOW() AS checked_at FROM lat_range
UNION ALL SELECT *, NOW() FROM lon_range
UNION ALL SELECT *, NOW() FROM duplicate_ids
UNION ALL SELECT *, NOW() FROM us_coverage
UNION ALL SELECT *, NOW() FROM elevation_range
