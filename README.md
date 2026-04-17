# Weather Data Pipeline

Ingests NOAA GHCN-Daily weather data into DuckDB, transforms it with Bruin, and serves a Streamlit dashboard supporting any US state, country, or city with rolling features and ML-ready prediction tables.

## About

NOAA's Global Historical Climatology Network is one of the richest public weather datasets in the world — daily observations from over 100,000 stations across the globe. This project makes that data accessible through a single pipeline that handles the full journey: pick any location, download the data, transform it through staging and mart layers with built-in quality checks, and explore it through an interactive dashboard — all running locally with Docker.

## What You Get

- **Pick any location** — select a US state, country, or city from the dashboard and ingest weather data on demand
- **Analytics-ready tables** — daily weather with rolling averages, monthly summaries, and ML feature tables with lag/seasonal encoding
- **Data quality at every layer** — 18 automated checks across raw, staging, and mart tables (null detection, range validation, freshness, referential integrity)
- **ML-ready output** — `mart_prediction_features` provides lag features, rolling stats, seasonal encoding, and a next-day temperature target column ready for scikit-learn

## Architecture

```
NOAA GHCN-Daily (.dly files)
        |
        v
  +-----------+       +-------------+       +------------+
  | Ingestion | ----> |   DuckDB    | ----> | Streamlit  |
  | (Python)  |       | raw/staging |       | Dashboard  |
  +-----------+       |   /marts    |       +------------+
                      +-------------+            :8501
                           ^
                           |
                      +----------+
                      |  Bruin   |
                      | transform|
                      | + quality|
                      +----------+

              Orchestrated by Kestra (:8080)
```

**Data flow:** `raw` -> `staging` -> `marts` -> `quality`

## Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Database | DuckDB | Embedded columnar analytics (no server needed) |
| Ingestion + Transforms | Bruin | Python ingestion assets + SQL transforms + quality checks |
| Dashboard | Streamlit | Interactive UI with location selector and ingestion controls |
| Orchestration | Kestra | Scheduling and pipeline chaining |
| Dependencies | Poetry | Python dependency management with lock file |
| CI/CD | GitHub Actions | Ruff linting, Bruin validation, Docker build checks |
| Containerization | Docker Compose | Run all services locally |

## Data Source

[NOAA Global Historical Climatology Network - Daily (GHCN-Daily)](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily) — daily observations including:

- `TMAX` / `TMIN` — max/min temperature (tenths of degrees C)
- `PRCP` — precipitation (tenths of mm)
- `SNOW` / `SNWD` — snowfall / snow depth (mm)
- `AWND` — average wind speed (tenths of m/s)

Ingestion downloads per-station `.dly` files (~200KB each) rather than full yearly CSVs (~170MB+), completing in seconds per station.

## Mart Tables

| Table | Description |
|-------|-------------|
| `marts.mart_daily_weather` | Daily observations joined with station info and rolling features |
| `marts.mart_monthly_summary` | Monthly aggregates per station |
| `marts.mart_prediction_features` | ML features: lag, rolling stats, seasonal encoding, next-day target |

## Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose
- [Python 3.12+](https://www.python.org/)
- [Poetry](https://python-poetry.org/docs/#installation)
- [Bruin CLI](https://getbruin.com/) (optional, for local validation)

## Quick Start

### 1. Install dependencies

```bash
poetry install --with ingestion,streamlit
```

### 2. Start services

```bash
docker compose up -d --build
```

This starts:

| Service | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| Kestra UI | http://localhost:8080 |

### 3. Ingest data

Use the CLI to ingest weather data for a location:

```bash
# By US state
poetry run python ingestion/ingest_ghcn.py --years 2020 2021 2022 2023 2024 2025 --state AK

# By country (NOAA FIPS code)
poetry run python ingestion/ingest_ghcn.py --years 2020 2021 2022 2023 2024 2025 --country CA

# By city (geocoded, with radius in km)
poetry run python ingestion/ingest_ghcn.py --years 2020 2021 2022 2023 2024 2025 --city "Seattle, WA" --radius 50

# By specific station IDs
poetry run python ingestion/ingest_ghcn.py --years 2024 2025 --stations USW00026451 USW00026411
```

Or use the Streamlit dashboard to select a location and ingest directly from the UI.

### 4. Run transforms and quality checks

```bash
make bruin-run
```

The dashboard also runs transforms automatically after ingestion.

## Make Targets

Run `make help` to see all available targets:

| Target | Description |
|--------|-------------|
| `make up` | Start all services |
| `make up-build` | Rebuild images and start services |
| `make down` | Stop all services |
| `make ingest` | Ingest by state (`STATE=AK YEARS="2024 2025"`) |
| `make ingest-country` | Ingest by country (`COUNTRY=CA YEARS="2024 2025"`) |
| `make ingest-city` | Ingest by city (`CITY="Seattle, WA" RADIUS=50 YEARS="2024 2025"`) |
| `make bruin-run` | Run full Bruin pipeline (transforms + quality) |
| `make bruin-validate` | Validate Bruin pipeline definitions |
| `make bruin-quality` | Run quality checks only |
| `make bruin-transform` | Run transforms only |
| `make duckdb-shell` | Open a DuckDB SQL shell |
| `make clean` | Remove all volumes and data |

## Project Structure

```
ingestion/
  ingest_ghcn.py                # Standalone ingestion script (CLI + Kestra)

bruin/
  pipeline.yml                  # Bruin pipeline definition
  assets/
    ingestion/
      ingest_stations.py        # Station metadata -> DuckDB
      ingest_ghcn_yearly.py     # Per-station .dly download -> DuckDB
    transform/
      stg_daily_observations.sql    # Pivot raw long->wide, unit conversions
      stg_stations.sql              # Station metadata + geographic enrichment
      mart_daily_weather.sql        # Primary analytics table + rolling features
      mart_monthly_summary.sql      # Monthly aggregations per station
      mart_prediction_features.sql  # ML feature table (lag, rolling, seasonal)
    quality/
      validate_raw_daily.sql    # 8 quality checks on raw data
      validate_stations.sql     # 5 quality checks on station metadata
      validate_marts.sql        # 5 quality checks on mart tables

streamlit/
  app.py                        # Dashboard: 4 tabs + station map + ingestion UI
  pipeline.py                   # Ingestion + transform logic

kestra/flows/
  weather_daily_pipeline.yml    # Daily scheduled pipeline
  weather_backfill.yml          # On-demand multi-year backfill
```

## Data Lineage

```
raw.ghcn_daily ----> stg_daily_observations ----> mart_daily_weather ----> mart_prediction_features
raw.ghcn_stations -> stg_stations ----------/            |
                                                         +-----------> mart_monthly_summary
```

## Quality Checks

Bruin SQL assets in `bruin/assets/quality/` validate data at every layer:

- **`validate_raw_daily`** — null station/date checks, future dates, temperature extremes, negative precipitation, TMAX/TMIN inversions, data freshness, orphan stations
- **`validate_stations`** — coordinate ranges, duplicates, US state coverage, elevation range
- **`validate_marts`** — tables populated, rolling averages computed, prediction targets exist, monthly coverage, station joins

Each check produces a PASS/WARN/FAIL result stored in the `quality` schema.

## CI/CD

GitHub Actions runs on every push to `main` and on pull requests:

| Job | Description |
|-----|-------------|
| **Ruff Lint & Format** | Python linting and format checking |
| **Bruin Validate** | Pipeline definition validation |
| **Docker Build** | Smoke-test image builds for Streamlit and Bruin |

## Development

```bash
# Install all dependencies (including dev tools)
poetry install --with ingestion,streamlit

# Lint
poetry run ruff check .

# Auto-fix lint issues
poetry run ruff check --fix .

# Format
poetry run ruff format .

# Validate Bruin pipeline
bruin validate bruin/

# Export requirements for Docker (after changing deps)
poetry export --only=main,streamlit --without-hashes -f requirements.txt -o streamlit/requirements.txt
poetry export --only=main --without-hashes -f requirements.txt -o bruin/assets/ingestion/requirements.txt
```

## Roadmap

- **ML prediction model** — train scikit-learn model on `mart_prediction_features`, write predictions to `marts.mart_predictions`, display in Streamlit
- **Kestra end-to-end** — test orchestration flows with DuckDB volume mounts
