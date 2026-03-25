-- ============================================================
-- Metabase-friendly views for dashboard creation
-- Run these after dbt models are built
-- ============================================================

-- View 1: Latest conditions per station (map visualization)
CREATE OR REPLACE VIEW marts.v_latest_conditions AS
WITH ranked AS (
    SELECT
        station_id,
        station_name,
        latitude,
        longitude,
        state,
        climate_zone,
        obs_date,
        tmax_celsius,
        tmin_celsius,
        tavg_derived_celsius,
        precipitation_mm,
        snowfall_mm,
        avg_wind_speed_ms,
        ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY obs_date DESC) AS rn
    FROM marts.mart_daily_weather
)
SELECT * FROM ranked WHERE rn = 1;


-- View 2: Temperature trend (line chart)
CREATE OR REPLACE VIEW marts.v_temp_trend AS
SELECT
    obs_date,
    station_name,
    state,
    tavg_derived_celsius,
    tavg_7d_avg,
    tavg_30d_avg
FROM marts.mart_daily_weather
WHERE obs_date >= CURRENT_DATE - INTERVAL '1 year';


-- View 3: Monthly heatmap data
CREATE OR REPLACE VIEW marts.v_monthly_heatmap AS
SELECT
    station_name,
    obs_year,
    obs_month,
    avg_temp_celsius,
    total_precip_mm,
    extreme_heat_days,
    extreme_cold_days
FROM marts.mart_monthly_summary
ORDER BY station_name, obs_year, obs_month;


-- View 4: Anomaly tracker (scatter plot)
CREATE OR REPLACE VIEW marts.v_anomalies AS
SELECT
    station_id,
    obs_date,
    tavg_derived_celsius,
    hist_normal_temp,
    temp_anomaly,
    is_temp_anomaly
FROM marts.weather_prediction_features
WHERE is_temp_anomaly = TRUE
ORDER BY obs_date DESC;


-- View 5: Precipitation summary (bar chart)
CREATE OR REPLACE VIEW marts.v_precip_summary AS
SELECT
    station_name,
    state,
    obs_year,
    obs_month,
    total_precip_mm,
    days_with_precip,
    total_snowfall_mm,
    days_with_snow
FROM marts.mart_monthly_summary;
