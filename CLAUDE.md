# Weather Data Pipeline — Project Context

## What this project does
Ingests NOAA GHCN-Daily weather data for any user-selected location (US state or country), transforms it into analytics-ready mart tables, and serves a weather prediction dashboard. The Streamlit dashboard includes a location selector — users pick a US state or country, choose a year range, and trigger ingestion + transforms directly from the UI. Designed to eventually support an ML model for next-day temperature prediction.

## Stack
| Component | Role |
|-----------|------|
| **DuckDB** | Analytical database (file-based, no server) |
| **Bruin** | Ingestion + SQL transforms + quality checks |
| **Streamlit** | Dashboard at `http://localhost:8501` |
| **Kestra** | Orchestration/scheduling at `http://localhost:8080` |
| **Poetry** | Python dependency management (`pyproject.toml` + `poetry.lock`) |


## Critical operational notes
- DuckDB file lives in a named Docker volume (`duckdb_data`) mounted at `/data/weather.duckdb` inside containers.
- **Poetry** manages all Python dependencies. Use `poetry install --with ingestion,streamlit` to set up the local venv, and `poetry run` or `poetry shell` to execute scripts.
- Docker containers still use pip — `poetry export` generates per-container `requirements.txt` files from the lock file.

## Data
- **Source:** NOAA GHCN-Daily per-station `.dly` files (not yearly CSVs — that approach was too slow)
- **Location:** User-selectable from dashboard — any US state or country available in NOAA stations metadata
- **No default location** — user must always specify `--state`, `--country`, or `--stations` when running the CLI script, or set `state`/`country` parameters in Bruin
- **DB schema:** `raw` → `staging` → `marts` → `quality`


## How to run the pipeline

### Start services (PowerShell)
```powershell
docker compose up -d --build   # first time or after code changes
docker compose up -d           # subsequent starts
docker compose down -v         # full reset including data
```

### Install Python dependencies (WSL, first time)
```bash
poetry install --with ingestion,streamlit
```

### Run ingestion (WSL)
```bash
poetry run python ingestion/ingest_ghcn.py --years 2020 2021 2022 2023 2024 2025 --state AK
poetry run python ingestion/ingest_ghcn.py --years 2020 2021 2022 2023 2024 2025 --country CA
```

### Run Bruin transforms + quality (WSL)
```bash
make bruin-run
```

### Export requirements for Docker (after changing deps)
```bash
poetry export --only=main,streamlit --without-hashes -f requirements.txt -o streamlit/requirements.txt
poetry export --only=main --without-hashes -f requirements.txt -o bruin/assets/ingestion/requirements.txt
```

### Full pipeline order
1. `docker compose up -d`
2. `poetry run python ingestion/ingest_ghcn.py --years ... --state XX` (or use the dashboard's Ingest Location button)
3. `make bruin-run` (or dashboard runs transforms automatically after ingestion)
4. Dashboard at `http://localhost:8501`

## Project structure
```
ingestion/
  ingest_ghcn.py          # Standalone ingestion script (used by Kestra + manual runs)

bruin/
  assets/
    ingestion/
      ingest_stations.py  # Bruin asset: downloads station metadata → DuckDB
      ingest_ghcn_yearly.py  # Bruin asset: per-station .dly download → DuckDB
    transform/
      stg_daily_observations.sql  # Pivot raw long→wide, unit conversions
      stg_stations.sql            # Station metadata + geographic enrichment
      mart_daily_weather.sql      # Primary analytics table + rolling features
      mart_monthly_summary.sql    # Monthly aggregations per station
      mart_prediction_features.sql # ML feature table (lag, rolling, seasonal)
    quality/
      validate_raw_daily.sql   # 8 quality checks on raw data
      validate_stations.sql    # 5 quality checks on station metadata
      validate_marts.sql       # 5 quality checks on mart tables

streamlit/
  app.py                  # Dashboard: 4 tabs + station map + location ingestion UI
  pipeline.py             # Ingestion + transform logic (used by app.py)

kestra/flows/
  weather_daily_pipeline.yml  # Daily schedule: ingest + bruin run
  weather_backfill.yml        # On-demand: multi-year ingest + bruin run
```

## Mart tables (in DuckDB)
| Table | Description |
|-------|-------------|
| `marts.mart_daily_weather` | Daily obs + station info + rolling features |
| `marts.mart_monthly_summary` | Monthly aggregates per station |
| `marts.mart_prediction_features` | ML features: lag, rolling, seasonal encoding, next-day target |

## Remaining work
1. **ML prediction model** — read `mart_prediction_features`, train scikit-learn model, write predictions to `marts.mart_predictions`, display in Streamlit tab 4
2. **Kestra configuration** — flows are written but not yet tested end-to-end with DuckDB volume mounts

## Design decisions (don't revisit without good reason)
- **Per-station .dly files over yearly CSVs:** Yearly files are 170MB+ and take ~1hr to stream/filter. Per-station files are ~200KB each and complete in seconds.
- **DuckDB over PostgreSQL:** No server needed, columnar analytics, embedded in containers via shared volume.
- **Bruin over dbt:** Consolidates ingestion + transforms + quality into one tool. dbt models were ported as Bruin SQL assets in `bruin/assets/transform/`.
- **Streamlit over Metabase:** Metabase had auth/state issues; Streamlit gives full control and will integrate with the ML model.
