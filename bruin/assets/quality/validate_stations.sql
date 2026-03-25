/* @bruin
name: quality.validate_stations
type: postgres.sql
depends:
  - raw.ghcn_stations_ingest
materialization:
  type: table
  strategy: create+replace

description: >
  Data quality validation for GHCN station metadata.
  Ensures geographic data is sane and coverage is adequate.

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

-- Check 1: Latitude range (-90 to 90)
lat_range AS (
    SELECT
        'latitude_out_of_range' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS check_result,
        'Latitude must be between -90 and 90' AS description
    FROM raw.ghcn_stations
    WHERE latitude < -90 OR latitude > 90
),

-- Check 2: Longitude range (-180 to 180)
lon_range AS (
    SELECT
        'longitude_out_of_range' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS check_result,
        'Longitude must be between -180 and 180' AS description
    FROM raw.ghcn_stations
    WHERE longitude < -180 OR longitude > 180
),

-- Check 3: Duplicate station IDs
duplicate_ids AS (
    SELECT
        'duplicate_station_ids' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            ELSE 'FAIL'
        END AS check_result,
        'Station IDs should be unique' AS description
    FROM (
        SELECT station_id
        FROM raw.ghcn_stations
        GROUP BY station_id
        HAVING COUNT(*) > 1
    ) dupes
),

-- Check 4: US station coverage (should have thousands)
us_coverage AS (
    SELECT
        'us_station_coverage' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) > 5000 THEN 'PASS'
            WHEN COUNT(*) > 1000 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'US should have 5000+ stations' AS description
    FROM raw.ghcn_stations
    WHERE LEFT(station_id, 2) = 'US'
),

-- Check 5: Elevation sanity (-500m to 9000m)
elevation_range AS (
    SELECT
        'elevation_out_of_range' AS check_name,
        COUNT(*) AS violation_count,
        CASE
            WHEN COUNT(*) = 0 THEN 'PASS'
            WHEN COUNT(*) < 10 THEN 'WARN'
            ELSE 'FAIL'
        END AS check_result,
        'Elevation should be between -500m and 9000m' AS description
    FROM raw.ghcn_stations
    WHERE elevation IS NOT NULL
      AND (elevation < -500 OR elevation > 9000)
)

SELECT check_name, violation_count, check_result, description, NOW() AS checked_at
FROM lat_range
UNION ALL SELECT *, NOW() FROM lon_range
UNION ALL SELECT *, NOW() FROM duplicate_ids
UNION ALL SELECT *, NOW() FROM us_coverage
UNION ALL SELECT *, NOW() FROM elevation_range
