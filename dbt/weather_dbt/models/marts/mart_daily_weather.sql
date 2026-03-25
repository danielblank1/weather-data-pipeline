-- ============================================================
-- Mart: Daily weather with station info and rolling features
-- This is the primary analytics table for dashboards and ML
-- ============================================================

WITH observations AS (
    SELECT * FROM {{ ref('stg_daily_observations') }}
),

stations AS (
    SELECT * FROM {{ ref('stg_stations') }}
),

joined AS (
    SELECT
        o.station_id,
        s.station_name,
        s.latitude,
        s.longitude,
        s.elevation,
        s.state,
        s.country_code,
        s.climate_zone,

        o.obs_date,
        EXTRACT(YEAR FROM o.obs_date)::INT        AS obs_year,
        EXTRACT(MONTH FROM o.obs_date)::INT       AS obs_month,
        EXTRACT(DOW FROM o.obs_date)::INT         AS day_of_week,
        EXTRACT(DOY FROM o.obs_date)::INT         AS day_of_year,

        o.tmax_celsius,
        o.tmin_celsius,
        o.tavg_derived_celsius,
        o.temp_range_celsius,
        o.precipitation_mm,
        o.snowfall_mm,
        o.snow_depth_mm,
        o.avg_wind_speed_ms

    FROM observations o
    INNER JOIN stations s USING (station_id)
),

with_rolling AS (
    SELECT
        *,

        -- 7-day rolling averages
        AVG(tavg_derived_celsius) OVER (
            PARTITION BY station_id
            ORDER BY obs_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS tavg_7d_avg,

        AVG(precipitation_mm) OVER (
            PARTITION BY station_id
            ORDER BY obs_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS precip_7d_avg,

        -- 30-day rolling averages
        AVG(tavg_derived_celsius) OVER (
            PARTITION BY station_id
            ORDER BY obs_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS tavg_30d_avg,

        SUM(precipitation_mm) OVER (
            PARTITION BY station_id
            ORDER BY obs_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS precip_30d_total,

        -- Day-over-day temperature change
        tavg_derived_celsius - LAG(tavg_derived_celsius, 1) OVER (
            PARTITION BY station_id ORDER BY obs_date
        ) AS temp_change_1d,

        -- Week-over-week temperature change
        tavg_derived_celsius - LAG(tavg_derived_celsius, 7) OVER (
            PARTITION BY station_id ORDER BY obs_date
        ) AS temp_change_7d,

        -- Boolean flags
        CASE WHEN precipitation_mm > 0 THEN TRUE ELSE FALSE END AS had_precipitation,
        CASE WHEN snowfall_mm > 0 THEN TRUE ELSE FALSE END AS had_snowfall,
        CASE WHEN tmax_celsius > 35 THEN TRUE ELSE FALSE END AS is_extreme_heat,
        CASE WHEN tmin_celsius < -20 THEN TRUE ELSE FALSE END AS is_extreme_cold

    FROM joined
)

SELECT * FROM with_rolling
