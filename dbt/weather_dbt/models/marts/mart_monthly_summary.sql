-- ============================================================
-- Mart: Monthly weather summaries per station
-- Aggregated statistics useful for trend analysis and dashboards
-- ============================================================

WITH daily AS (
    SELECT * FROM {{ ref('mart_daily_weather') }}
)

SELECT
    station_id,
    station_name,
    latitude,
    longitude,
    state,
    country_code,
    climate_zone,
    obs_year,
    obs_month,

    -- Temperature statistics
    ROUND(AVG(tavg_derived_celsius)::NUMERIC, 2)   AS avg_temp_celsius,
    ROUND(MAX(tmax_celsius)::NUMERIC, 2)            AS max_temp_celsius,
    ROUND(MIN(tmin_celsius)::NUMERIC, 2)            AS min_temp_celsius,
    ROUND(STDDEV(tavg_derived_celsius)::NUMERIC, 2) AS temp_stddev,

    -- Precipitation
    ROUND(SUM(COALESCE(precipitation_mm, 0))::NUMERIC, 2) AS total_precip_mm,
    COUNT(*) FILTER (WHERE had_precipitation)              AS days_with_precip,
    ROUND(AVG(CASE WHEN had_precipitation
        THEN precipitation_mm END)::NUMERIC, 2)            AS avg_precip_per_rainy_day_mm,

    -- Snow
    ROUND(SUM(COALESCE(snowfall_mm, 0))::NUMERIC, 2)      AS total_snowfall_mm,
    COUNT(*) FILTER (WHERE had_snowfall)                   AS days_with_snow,

    -- Extremes
    COUNT(*) FILTER (WHERE is_extreme_heat)  AS extreme_heat_days,
    COUNT(*) FILTER (WHERE is_extreme_cold)  AS extreme_cold_days,

    -- Record count / completeness
    COUNT(*)                                 AS observation_count,
    COUNT(tavg_derived_celsius)              AS temp_observation_count

FROM daily
GROUP BY
    station_id, station_name, latitude, longitude,
    state, country_code, climate_zone,
    obs_year, obs_month
