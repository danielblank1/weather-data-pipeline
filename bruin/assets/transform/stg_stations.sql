/* @bruin
name: staging.stg_stations
type: duckdb.sql
connection: weather_warehouse
depends:
  - raw.ghcn_stations
materialization:
  type: table
  strategy: create+replace

description: >
  Clean station metadata with geographic enrichment.
@bruin */

SELECT
    station_id,
    station_name,
    latitude,
    longitude,
    elevation,
    state,
    gsn_flag,
    hcn_flag,
    wmo_id,

    LEFT(station_id, 2) AS country_code,

    CASE WHEN latitude >= 0 THEN 'Northern' ELSE 'Southern' END AS hemisphere,

    CASE
        WHEN ABS(latitude) < 23.5 THEN 'Tropical'
        WHEN ABS(latitude) < 35.0 THEN 'Subtropical'
        WHEN ABS(latitude) < 55.0 THEN 'Temperate'
        WHEN ABS(latitude) < 66.5 THEN 'Subarctic'
        ELSE 'Polar'
    END AS climate_zone,

    CASE WHEN LEFT(station_id, 2) = 'US' THEN TRUE ELSE FALSE END AS is_us_station

FROM raw.ghcn_stations
WHERE latitude  IS NOT NULL
  AND longitude IS NOT NULL
