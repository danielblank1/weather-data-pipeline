-- ============================================================
-- Staging: Clean station metadata with geographic enrichment
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'ghcn_stations') }}
)

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

    -- Country code is the first 2 characters of station_id
    LEFT(station_id, 2)  AS country_code,

    -- Hemisphere flags
    CASE WHEN latitude >= 0 THEN 'Northern' ELSE 'Southern' END AS hemisphere,

    -- Rough climate zone based on latitude
    CASE
        WHEN ABS(latitude) < 23.5   THEN 'Tropical'
        WHEN ABS(latitude) < 35.0   THEN 'Subtropical'
        WHEN ABS(latitude) < 55.0   THEN 'Temperate'
        WHEN ABS(latitude) < 66.5   THEN 'Subarctic'
        ELSE 'Polar'
    END AS climate_zone,

    -- US-only flag (useful for filtering)
    CASE WHEN LEFT(station_id, 2) = 'US' THEN TRUE ELSE FALSE END AS is_us_station

FROM source
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
