# Weather Data Pipeline

An end-to-end data engineering project that ingests NOAA weather data, transforms it through a modern data stack, and produces prediction-ready features — all running locally with Docker.

Built following the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) curriculum.

## Architecture

```
NOAA GHCN-Daily (CSV)
        │
        ▼
┌───────────────┐     ┌─────────────┐
│  Bruin        │     │  Kafka      │
│  Ingestion    │     │  Producer   │
│  (Python)     │     │  (stream)   │
└──────┬────────┘     └──────┬──────┘
       │                     │
       ▼                     ▼
┌──────────────────────────────────┐
│       PostgreSQL (Warehouse)     │
│  raw → staging → marts schemas  │
└──────┬───────────────────┬──────┘
       │                   │
       ▼                   ▼
┌─────────────┐    ┌──────────────┐
│  dbt        │    │  Spark       │
│  (transform)│    │  (batch ML   │
│             │    │   features)  │
└──────┬──────┘    └──────┬───────┘
       │                  │
       ├──────────────────┘
       ▼
┌──────────────────────────────────┐
│  Bruin Quality Checks            │
│  (raw + staging + marts)         │
└──────┬───────────────────────────┘
       │
       ▼
┌──────────────────────────────────┐
│         Metabase (Dashboard)     │
└──────────────────────────────────┘

       Orchestrated by Kestra + Bruin
```

## Tech Stack

| Layer              | Tool              | Purpose                              |
|--------------------|-------------------|--------------------------------------|
| Containerization   | Docker Compose    | Run all services locally             |
| IaC                | Terraform         | Manage Docker resources declaratively|
| Orchestration      | Kestra            | Schedule and chain pipeline tasks    |
| Ingestion          | Bruin (Python)    | Download NOAA data with quality gates|
| Quality            | Bruin (SQL)       | Data validation at every layer       |
| Warehouse          | PostgreSQL        | Store raw, staging, and mart data    |
| Transformation     | dbt               | SQL-based staging → mart models      |
| Batch Processing   | Apache Spark      | Compute heavy ML features            |
| Streaming          | Apache Kafka      | Simulate real-time weather data      |
| Dashboard          | Metabase          | Interactive visualizations           |

## Data Source

**NOAA Global Historical Climatology Network — Daily (GHCN-D)**

Over 100,000 weather stations worldwide, providing daily observations of:
- `TMAX` / `TMIN` — max/min temperature (tenths of °C)
- `PRCP` — precipitation (tenths of mm)
- `SNOW` / `SNWD` — snowfall / snow depth (mm)
- `AWND` — average wind speed (tenths of m/s)

Data is freely available from [NCEI](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily).

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- [Bruin CLI](https://github.com/bruin-data/bruin) (`curl -LsSf https://getbruin.com/install/cli | sh`)
- Make (optional, for convenience commands)

### 1. Start services

```bash
cp .env.example .env
make up
# or: docker compose up -d
```

### 2. Ingest data with Bruin

```bash
# Validate Bruin pipeline definitions
make bruin-validate

# Run Bruin ingestion (stations + yearly data)
make bruin-ingest

# Or use the original Python ingestion for backfill
make ingest-backfill
```

### 3. Run transformations

```bash
make dbt
make dbt-test
```

### 4. Run Bruin quality checks

```bash
# Validate raw, staging, and mart data quality
make bruin-quality
```

### 5. Run Spark features

```bash
make spark
```

### 6. Set up dashboard

```bash
make dashboard-views
```

Then open Metabase at http://localhost:3000, connect to PostgreSQL, and explore the pre-built views.

### 7. Try streaming (optional)

```bash
# Terminal 1: start consumer
make kafka-consume

# Terminal 2: start producer
make kafka-produce
```

## Access Points

| Service       | URL                          |
|---------------|------------------------------|
| Kestra UI     | http://localhost:8080         |
| Spark Master  | http://localhost:8082         |
| Metabase      | http://localhost:3000         |
| PostgreSQL    | `localhost:5432`              |
| Kafka         | `localhost:9092`              |

## Project Structure

```
weather-data-pipeline/
├── docker-compose.yml          # All services
├── Makefile                    # Convenience commands
├── .env.example                # Environment template
├── docker/
│   └── init-db.sql             # PostgreSQL schema setup
├── bruin/
│   ├── .bruin.yml              # Bruin project config + connections
│   ├── Dockerfile              # Bruin Docker image
│   ├── pipeline.yml            # Pipeline definition (schedule, defaults)
│   └── assets/
│       ├── ingestion/
│       │   ├── ingest_ghcn_yearly.py    # Python: download + load GHCN data
│       │   ├── ingest_stations.py       # Python: download station metadata
│       │   └── requirements.txt
│       └── quality/
│           ├── validate_raw_daily.sql   # 8 quality checks on raw data
│           ├── validate_stations.sql    # 5 checks on station metadata
│           └── validate_marts.sql       # 5 checks on dbt mart outputs
├── ingestion/
│   ├── Dockerfile
│   ├── ingest_ghcn.py          # Standalone NOAA downloader (backfill)
│   └── requirements.txt
├── kestra/
│   └── flows/
│       ├── weather_daily_pipeline.yml    # Daily orchestration
│       └── weather_backfill.yml          # Historical backfill
├── dbt/
│   └── weather_dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       └── models/
│           ├── sources/sources.yml
│           ├── staging/
│           │   ├── stg_daily_observations.sql
│           │   ├── stg_stations.sql
│           │   └── schema.yml
│           └── marts/
│               ├── mart_daily_weather.sql
│               ├── mart_monthly_summary.sql
│               ├── mart_prediction_features.sql
│               └── schema.yml
├── spark/
│   └── weather_features.py     # Batch ML feature engineering
├── kafka/
│   ├── weather_producer.py     # Simulated real-time feed
│   ├── weather_consumer.py     # Stream-to-DB consumer
│   └── requirements.txt
├── dashboard/
│   └── metabase_setup.sql      # Pre-built views
└── terraform/
    ├── main.tf                 # Docker resource management
    └── variables.tf
```

## dbt Model Lineage

```
raw.ghcn_daily ──► stg_daily_observations ──► mart_daily_weather ──► mart_prediction_features
raw.ghcn_stations ──► stg_stations ──────────┘       │
                                                      └──► mart_monthly_summary
```

## Dashboard Suggestions

Pre-built SQL views in `dashboard/metabase_setup.sql` support these visualizations:

1. **Latest Conditions Map** — pin map of current station readings
2. **Temperature Trend** — line chart of daily temps with 7/30-day averages
3. **Monthly Heatmap** — grid of station × month temperature/precipitation
4. **Anomaly Tracker** — scatter plot highlighting unusual temperature days
5. **Precipitation Summary** — bar chart of monthly rain/snow totals

## Bruin Pipeline

Bruin handles two responsibilities alongside dbt: data ingestion and data quality validation.

### Ingestion assets (Python)

Bruin Python assets in `bruin/assets/ingestion/` download NOAA data and return DataFrames. Bruin handles writing to PostgreSQL automatically via the connection config, with built-in column-level quality checks that run after every load.

### Quality check assets (SQL)

Three SQL validation assets in `bruin/assets/quality/` run checks across the entire pipeline:

- `validate_raw_daily.sql` — 8 checks on raw data (nulls, future dates, temperature sanity, negative precipitation, TMAX/TMIN inversions, data freshness, orphan stations)
- `validate_stations.sql` — 5 checks on station metadata (coordinate ranges, duplicates, US coverage, elevation range)
- `validate_marts.sql` — 5 checks on dbt outputs (table populated, rolling averages computed, prediction targets exist, monthly coverage, station joins)

Each check produces a PASS/WARN/FAIL result stored in the `quality` schema, which Metabase can display as a data quality dashboard.

### Bruin commands

```bash
bruin validate .          # Check pipeline definitions
bruin run .               # Run full pipeline
bruin run --only <asset>  # Run a single asset
```

## Extending the Project

- **Add more NOAA elements**: Edit `CORE_ELEMENTS` in `ingest_ghcn.py`
- **Filter to specific stations**: Use `--stations USW00094728` flag
- **Add ML models**: The `mart_prediction_features` table is ready for scikit-learn / XGBoost
- **Scale Spark**: Add more workers in `docker-compose.yml`
- **Production deployment**: Swap PostgreSQL for BigQuery, add GCS for data lake
