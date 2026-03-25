#!/usr/bin/env python3
"""
NOAA GHCN-Daily data ingestion script.

Downloads yearly CSV files from NOAA's GHCN-Daily dataset and loads them into
the PostgreSQL raw layer.  Supports incremental loads (skip years already
fully ingested) and parallel chunk loading via pandas.

Usage:
    python ingest_ghcn.py --years 2020 2021 2022 2023 2024 2025
    python ingest_ghcn.py --years 2024 --stations USW00094728 USW00012839
"""

import argparse
import gzip
import io
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GHCN_BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year"
GHCN_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

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

COLUMN_DTYPES = {
    "station_id": str,
    "obs_date": str,
    "element": str,
    "data_value": "Int64",
    "m_flag": str,
    "q_flag": str,
    "s_flag": str,
    "obs_time": str,
}

# Core elements relevant for weather prediction
CORE_ELEMENTS = {"TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "TAVG"}

CHUNK_SIZE = 100_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


def get_engine():
    """Create a SQLAlchemy engine from environment variables."""
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://weather:weather_pipeline_2026@localhost:5432/weather_warehouse",
    )
    return create_engine(db_url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Station metadata
# ---------------------------------------------------------------------------

def ingest_stations(engine):
    """Download and load GHCN station metadata into raw.ghcn_stations."""
    logger.info("Downloading station metadata …")
    resp = requests.get(GHCN_STATIONS_URL, timeout=120)
    resp.raise_for_status()

    rows = []
    for line in resp.text.splitlines():
        if len(line) < 72:
            continue
        rows.append(
            {
                "station_id": line[0:11].strip(),
                "latitude": float(line[12:20].strip()) if line[12:20].strip() else None,
                "longitude": float(line[21:30].strip()) if line[21:30].strip() else None,
                "elevation": float(line[31:37].strip()) if line[31:37].strip() else None,
                "state": line[38:40].strip() or None,
                "station_name": line[41:71].strip(),
                "gsn_flag": line[72:75].strip() or None,
                "hcn_flag": line[76:79].strip() or None,
                "wmo_id": line[80:85].strip() or None,
            }
        )

    df = pd.DataFrame(rows)
    logger.info("Loaded %d stations into DataFrame", len(df))

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.ghcn_stations"))
    df.to_sql(
        "ghcn_stations", engine, schema="raw", if_exists="append", index=False
    )
    logger.info("Station metadata loaded successfully.")


# ---------------------------------------------------------------------------
# Yearly observation data
# ---------------------------------------------------------------------------

def download_year(year: int) -> pd.DataFrame:
    """Download a single year of GHCN-Daily data, return as DataFrame."""
    url = f"{GHCN_BASE_URL}/{year}.csv.gz"
    logger.info("Downloading %s …", url)
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()

    buf = io.BytesIO(resp.content)
    with gzip.open(buf, "rt") as f:
        df = pd.read_csv(
            f,
            header=None,
            names=COLUMN_NAMES,
            dtype=COLUMN_DTYPES,
            na_values=[""],
        )
    logger.info("Year %d: %d raw records downloaded.", year, len(df))
    return df


def filter_data(df: pd.DataFrame, stations: list[str] | None = None) -> pd.DataFrame:
    """Keep only core weather elements and optionally filter by station list."""
    df = df[df["element"].isin(CORE_ELEMENTS)].copy()
    if stations:
        df = df[df["station_id"].isin(stations)]
    # Parse date
    df["obs_date"] = pd.to_datetime(df["obs_date"], format="%Y%m%d")
    logger.info("After filtering: %d records.", len(df))
    return df


def load_to_postgres(df: pd.DataFrame, engine):
    """Load a DataFrame into raw.ghcn_daily in chunks."""
    total = len(df)
    logger.info("Loading %d records into PostgreSQL …", total)

    for start in tqdm(range(0, total, CHUNK_SIZE), desc="Loading"):
        chunk = df.iloc[start : start + CHUNK_SIZE]
        chunk.to_sql(
            "ghcn_daily", engine, schema="raw", if_exists="append", index=False
        )
    logger.info("Load complete.")


def year_already_loaded(engine, year: int) -> bool:
    """Check if a year's data has already been loaded."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM raw.ghcn_daily "
                "WHERE obs_date >= :start AND obs_date < :end"
            ),
            {"start": f"{year}-01-01", "end": f"{year + 1}-01-01"},
        )
        count = result.scalar()
    return count > 0


# ---------------------------------------------------------------------------
# Main entry-point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest NOAA GHCN-Daily data")
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        required=True,
        help="Years to ingest (e.g., 2020 2021 2022)",
    )
    parser.add_argument(
        "--stations",
        nargs="*",
        default=None,
        help="Optional: filter to specific station IDs",
    )
    parser.add_argument(
        "--skip-stations-meta",
        action="store_true",
        help="Skip downloading station metadata",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest even if year is already loaded",
    )
    args = parser.parse_args()

    engine = get_engine()

    # Station metadata
    if not args.skip_stations_meta:
        ingest_stations(engine)

    # Yearly data
    for year in sorted(args.years):
        if not args.force and year_already_loaded(engine, year):
            logger.info("Year %d already loaded — skipping (use --force to reload).", year)
            continue

        df = download_year(year)
        df = filter_data(df, args.stations)

        if df.empty:
            logger.warning("No data after filtering for year %d.", year)
            continue

        load_to_postgres(df, engine)

    logger.info("Ingestion pipeline finished.")


if __name__ == "__main__":
    main()
