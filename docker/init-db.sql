-- ============================================================
-- PostgreSQL initialization script
-- Creates schemas and raw tables for the weather pipeline
-- ============================================================

-- Metabase needs its own database
CREATE DATABASE metabase;

-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS quality;

-- ============================================================
-- RAW layer — mirrors the NOAA GHCN-Daily CSV structure
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.ghcn_daily (
    station_id   VARCHAR(11)   NOT NULL,   -- GHCN station ID (e.g., USW00094728)
    obs_date     DATE          NOT NULL,   -- observation date
    element      VARCHAR(4)    NOT NULL,   -- TMAX, TMIN, PRCP, SNOW, SNWD …
    data_value   INTEGER,                  -- value in tenths (°C or mm)
    m_flag       VARCHAR(1),               -- measurement flag
    q_flag       VARCHAR(1),               -- quality flag
    s_flag       VARCHAR(1),               -- source flag
    obs_time     VARCHAR(4),               -- observation time (HHMM)
    loaded_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ghcn_station_date
    ON raw.ghcn_daily (station_id, obs_date);

CREATE INDEX IF NOT EXISTS idx_ghcn_element
    ON raw.ghcn_daily (element);

CREATE INDEX IF NOT EXISTS idx_ghcn_obs_date
    ON raw.ghcn_daily (obs_date);

-- ============================================================
-- RAW layer — station metadata
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.ghcn_stations (
    station_id   VARCHAR(11)   PRIMARY KEY,
    latitude     NUMERIC(8,4),
    longitude    NUMERIC(8,4),
    elevation    NUMERIC(7,1),
    state        VARCHAR(2),
    station_name VARCHAR(64),
    gsn_flag     VARCHAR(3),
    hcn_flag     VARCHAR(3),
    wmo_id       VARCHAR(5),
    loaded_at    TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- RAW layer — Kafka streaming landing table
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.weather_stream (
    event_id     SERIAL PRIMARY KEY,
    station_id   VARCHAR(11)   NOT NULL,
    obs_date     DATE          NOT NULL,
    element      VARCHAR(4)    NOT NULL,
    data_value   INTEGER,
    received_at  TIMESTAMP DEFAULT NOW()
);

-- Grant usage
GRANT USAGE ON SCHEMA raw      TO weather;
GRANT USAGE ON SCHEMA staging  TO weather;
GRANT USAGE ON SCHEMA marts    TO weather;
GRANT USAGE ON SCHEMA quality  TO weather;
GRANT ALL ON ALL TABLES IN SCHEMA raw      TO weather;
GRANT ALL ON ALL TABLES IN SCHEMA staging  TO weather;
GRANT ALL ON ALL TABLES IN SCHEMA marts    TO weather;
GRANT ALL ON ALL TABLES IN SCHEMA quality  TO weather;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw      GRANT ALL ON TABLES TO weather;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging  GRANT ALL ON TABLES TO weather;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts    GRANT ALL ON TABLES TO weather;
ALTER DEFAULT PRIVILEGES IN SCHEMA quality  GRANT ALL ON TABLES TO weather;
