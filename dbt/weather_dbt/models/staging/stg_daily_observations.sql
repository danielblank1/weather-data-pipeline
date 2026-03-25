-- ============================================================
-- Staging: Pivot raw GHCN observations from long to wide format
-- One row per station per day with typed columns
-- ============================================================

WITH source AS (
    SELECT
        station_id,
        obs_date,
        element,
        data_value,
        q_flag
    FROM {{ source('raw', 'ghcn_daily') }}
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

    -- Temperatures: stored as tenths of °C → convert to °C
    tmax_raw / 10.0                       AS tmax_celsius,
    tmin_raw / 10.0                       AS tmin_celsius,
    tavg_raw / 10.0                       AS tavg_celsius,
    COALESCE(
        tavg_raw / 10.0,
        (tmax_raw + tmin_raw) / 20.0
    )                                     AS tavg_derived_celsius,

    -- Precipitation: stored as tenths of mm → convert to mm
    prcp_raw / 10.0                       AS precipitation_mm,

    -- Snowfall & snow depth: already in mm
    snow_raw::NUMERIC                     AS snowfall_mm,
    snwd_raw::NUMERIC                     AS snow_depth_mm,

    -- Wind: stored as tenths of m/s → convert to m/s
    awnd_raw / 10.0                       AS avg_wind_speed_ms,

    -- Derived: temperature range
    (tmax_raw - tmin_raw) / 10.0          AS temp_range_celsius

FROM pivoted
