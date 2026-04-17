/* @bruin
name: staging.stg_daily_observations
type: duckdb.sql
connection: weather_warehouse
depends:
  - raw.ghcn_daily
materialization:
  type: table
  strategy: create+replace

description: >
  Pivot raw GHCN observations from long to wide format.
  One row per station per day with typed, unit-converted columns.
@bruin */

WITH source AS (
    SELECT
        station_id,
        obs_date,
        element,
        data_value,
        q_flag
    FROM raw.ghcn_daily
    WHERE q_flag IS NULL  -- exclude quality-flagged records
),

pivoted AS (
    SELECT
        station_id,
        obs_date,
        MAX(CASE WHEN element = 'TMAX' THEN data_value END) AS tmax_raw,
        MAX(CASE WHEN element = 'TMIN' THEN data_value END) AS tmin_raw,
        MAX(CASE WHEN element = 'TAVG' THEN data_value END) AS tavg_raw,
        MAX(CASE WHEN element = 'PRCP' THEN data_value END) AS prcp_raw,
        MAX(CASE WHEN element = 'SNOW' THEN data_value END) AS snow_raw,
        MAX(CASE WHEN element = 'SNWD' THEN data_value END) AS snwd_raw,
        MAX(CASE WHEN element = 'AWND' THEN data_value END) AS awnd_raw
    FROM source
    GROUP BY station_id, obs_date
)

SELECT
    station_id,
    obs_date,

    -- Temperatures: tenths of °C → °C
    tmax_raw / 10.0                                    AS tmax_celsius,
    tmin_raw / 10.0                                    AS tmin_celsius,
    tavg_raw / 10.0                                    AS tavg_celsius,
    COALESCE(tavg_raw / 10.0, (tmax_raw + tmin_raw) / 20.0) AS tavg_derived_celsius,

    -- Precipitation: tenths of mm → mm
    prcp_raw / 10.0                                    AS precipitation_mm,

    -- Snowfall / depth: already in mm
    snow_raw::DOUBLE                                   AS snowfall_mm,
    snwd_raw::DOUBLE                                   AS snow_depth_mm,

    -- Wind: tenths of m/s → m/s
    awnd_raw / 10.0                                    AS avg_wind_speed_ms,

    -- Derived
    (tmax_raw - tmin_raw) / 10.0                      AS temp_range_celsius

FROM pivoted
