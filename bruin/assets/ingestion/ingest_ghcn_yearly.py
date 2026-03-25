"""@bruin
name: raw.ghcn_daily_ingest
type: python
connection: weather_warehouse
depends: []

parameters:
  year: 2025

columns:
  - name: station_id
    type: string
    description: "11-character GHCN station ID"
    checks:
      - name: not_null
  - name: obs_date
    type: date
    description: "Observation date"
    checks:
      - name: not_null
  - name: element
    type: string
    description: "Weather element type (TMAX, TMIN, PRCP, etc.)"
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - TMAX
          - TMIN
          - PRCP
          - SNOW
          - SNWD
          - AWND
          - TAVG
  - name: data_value
    type: integer
    description: "Observation value (tenths of °C or mm)"

custom_checks:
  - name: "ingested records exist"
    description: "Ensure the ingestion loaded at least some records"
    query: SELECT count(*) FROM raw.ghcn_daily WHERE obs_date >= CURRENT_DATE - INTERVAL '365 days'
    value: 1
@bruin"""

import gzip
import io
import os
from datetime import datetime

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GHCN_BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year"

COLUMN_NAMES = [
    "station_id",
    "obs_date",
    "element",
    "data_value",
    "m_flag",
    "q_flag",
    "s_flag",
    "obs_time",
]

CORE_ELEMENTS = {"TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "TAVG"}


def materialize(context):
    """
    Download a year of GHCN-Daily data and return as a DataFrame.
    Bruin handles writing to PostgreSQL via the connection config.
    """
    year = context.params.get("year", datetime.now().year)
    url = f"{GHCN_BASE_URL}/{year}.csv.gz"

    print(f"Downloading GHCN data for year {year} from {url} ...")
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()

    buf = io.BytesIO(resp.content)
    with gzip.open(buf, "rt") as f:
        df = pd.read_csv(
            f,
            header=None,
            names=COLUMN_NAMES,
            dtype={
                "station_id": str,
                "obs_date": str,
                "element": str,
                "data_value": "Int64",
                "m_flag": str,
                "q_flag": str,
                "s_flag": str,
                "obs_time": str,
            },
            na_values=[""],
        )

    print(f"Downloaded {len(df):,} raw records for {year}")

    # Filter to core weather elements
    df = df[df["element"].isin(CORE_ELEMENTS)].copy()

    # Parse date
    df["obs_date"] = pd.to_datetime(df["obs_date"], format="%Y%m%d")

    # Add load timestamp
    df["loaded_at"] = datetime.now()

    print(f"After filtering: {len(df):,} records for {year}")

    return df
