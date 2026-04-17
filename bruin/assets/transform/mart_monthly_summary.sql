/* @bruin
name: marts.mart_monthly_summary
type: duckdb.sql
connection: weather_warehouse
depends:
  - marts.mart_daily_weather
materialization:
  type: table
  strategy: create+replace

description: >
  Monthly aggregated weather statistics per station.
@bruin */

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

    ROUND(AVG(tavg_derived_celsius), 2)                          AS avg_temp_celsius,
    ROUND(MAX(tmax_celsius), 2)                                  AS max_temp_celsius,
    ROUND(MIN(tmin_celsius), 2)                                  AS min_temp_celsius,
    ROUND(STDDEV(tavg_derived_celsius), 2)                       AS temp_stddev,

    ROUND(SUM(COALESCE(precipitation_mm, 0)), 2)                 AS total_precip_mm,
    COUNT(*) FILTER (WHERE had_precipitation)                    AS days_with_precip,
    ROUND(AVG(CASE WHEN had_precipitation THEN precipitation_mm END), 2) AS avg_precip_per_rainy_day_mm,

    ROUND(SUM(COALESCE(snowfall_mm, 0)), 2)                      AS total_snowfall_mm,
    COUNT(*) FILTER (WHERE had_snowfall)                         AS days_with_snow,

    COUNT(*) FILTER (WHERE is_extreme_heat)                      AS extreme_heat_days,
    COUNT(*) FILTER (WHERE is_extreme_cold)                      AS extreme_cold_days,

    COUNT(*)                                                     AS observation_count,
    COUNT(tavg_derived_celsius)                                  AS temp_observation_count

FROM marts.mart_daily_weather
GROUP BY
    station_id, station_name, latitude, longitude,
    state, country_code, climate_zone,
    obs_year, obs_month
