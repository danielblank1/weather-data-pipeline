/* @bruin
name: marts.mart_prediction_features
type: duckdb.sql
connection: weather_warehouse
depends:
  - marts.mart_daily_weather
materialization:
  type: table
  strategy: create+replace

description: >
  Feature-engineered table for weather prediction ML models.
  Includes lag features, rolling stats, and seasonal encodings.
  Target: next-day average temperature.
@bruin */

WITH daily AS (
    SELECT * FROM marts.mart_daily_weather
),

with_lags AS (
    SELECT
        station_id,
        obs_date,
        obs_year,
        obs_month,
        day_of_year,
        latitude,
        longitude,
        elevation,
        climate_zone,

        -- Target variable
        LEAD(tavg_derived_celsius, 1) OVER (
            PARTITION BY station_id ORDER BY obs_date
        ) AS target_tavg_next_day,

        -- Current features
        tavg_derived_celsius,
        tmax_celsius,
        tmin_celsius,
        temp_range_celsius,
        precipitation_mm,
        snowfall_mm,
        avg_wind_speed_ms,

        -- Lag features
        LAG(tavg_derived_celsius, 1) OVER w AS tavg_lag_1d,
        LAG(tavg_derived_celsius, 2) OVER w AS tavg_lag_2d,
        LAG(tavg_derived_celsius, 3) OVER w AS tavg_lag_3d,
        LAG(tavg_derived_celsius, 7) OVER w AS tavg_lag_7d,
        LAG(precipitation_mm, 1)     OVER w AS precip_lag_1d,
        LAG(precipitation_mm, 7)     OVER w AS precip_lag_7d,

        -- Rolling features
        tavg_7d_avg,
        tavg_30d_avg,
        precip_7d_avg,
        precip_30d_total,

        -- Trend features
        temp_change_1d,
        temp_change_7d,

        -- Seasonal encoding
        SIN(2 * PI() * day_of_year / 365.0) AS season_sin,
        COS(2 * PI() * day_of_year / 365.0) AS season_cos,

        -- Boolean features
        had_precipitation,
        had_snowfall,
        is_extreme_heat,
        is_extreme_cold

    FROM daily
    WINDOW w AS (PARTITION BY station_id ORDER BY obs_date)
)

SELECT *
FROM with_lags
WHERE target_tavg_next_day IS NOT NULL
  AND tavg_lag_7d          IS NOT NULL
