#!/usr/bin/env python3
"""
NOAA GHCN-Daily data ingestion script.

Downloads per-station .dly files from NOAA (one small file per station)
and loads into DuckDB. Dramatically faster than downloading full yearly CSVs.

A location must always be specified — there are no defaults.

Usage:
    python ingest_ghcn.py --years 2024 2025 --state AK
    python ingest_ghcn.py --years 2024 2025 --country CA
    python ingest_ghcn.py --years 2024 2025 --city "Seattle, WA" --radius 50
    python ingest_ghcn.py --years 2024 2025 --stations USW00026451 USW00026411
"""

import argparse
import datetime
import logging
import math
import os

import duckdb
import pandas as pd
import requests
from geopy.geocoders import Nominatim
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GHCN_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
GHCN_STATION_FILE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/weather.duckdb")

CORE_ELEMENTS = {"TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "TAVG"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


def _http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1,  # waits 1s, 2s, 4s, 8s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


# ---------------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------------


def get_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DUCKDB_PATH)


def init_db(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    conn.execute("CREATE SCHEMA IF NOT EXISTS quality")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ghcn_stations (
            station_id   VARCHAR,
            latitude     DOUBLE,
            longitude    DOUBLE,
            elevation    DOUBLE,
            state        VARCHAR,
            station_name VARCHAR,
            gsn_flag     VARCHAR,
            hcn_flag     VARCHAR,
            wmo_id       VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ghcn_daily (
            station_id  VARCHAR NOT NULL,
            obs_date    DATE    NOT NULL,
            element     VARCHAR NOT NULL,
            data_value  INTEGER,
            m_flag      VARCHAR,
            q_flag      VARCHAR,
            s_flag      VARCHAR,
            obs_time    VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# Station metadata
# ---------------------------------------------------------------------------


def ingest_stations(conn):
    logger.info("Downloading station metadata …")
    session = _http_session()
    resp = session.get(GHCN_STATIONS_URL, timeout=120)
    resp.raise_for_status()

    rows = []
    for line in resp.text.splitlines():
        if len(line) < 72:
            continue
        rows.append(
            {
                "station_id": line[0:11].strip(),
                "latitude": _safe_float(line[12:20]),
                "longitude": _safe_float(line[21:30]),
                "elevation": _safe_float(line[31:37]),
                "state": line[38:40].strip() or None,
                "station_name": line[41:71].strip(),
                "gsn_flag": line[72:75].strip() or None,
                "hcn_flag": line[76:79].strip() or None if len(line) > 76 else None,
                "wmo_id": line[80:85].strip() or None if len(line) > 80 else None,
            }
        )

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["latitude", "longitude"])
    logger.info("Loaded %d stations", len(df))

    conn.execute("DELETE FROM raw.ghcn_stations")
    conn.register("stations_df", df)
    conn.execute("INSERT INTO raw.ghcn_stations SELECT * FROM stations_df")
    conn.unregister("stations_df")
    logger.info("Station metadata loaded successfully.")


def _safe_float(s: str):
    s = s.strip()
    if not s or s == "-999.9":
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Per-station .dly download and parse
# ---------------------------------------------------------------------------


def fetch_station_dly(station_id: str) -> str:
    url = GHCN_STATION_FILE_URL.format(station_id=station_id)
    logger.info("Fetching %s", url)
    session = _http_session()
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


def parse_dly(content: str, station_id: str, years: set) -> list[dict]:
    rows = []
    for line in content.splitlines():
        if len(line) < 21:
            continue
        year = int(line[11:15])
        if year not in years:
            continue
        element = line[17:21].strip()
        if element not in CORE_ELEMENTS:
            continue
        month = int(line[15:17])
        for day in range(1, 32):
            pos = 21 + (day - 1) * 8
            if pos + 5 > len(line):
                break
            raw_value = line[pos : pos + 5].strip()
            if not raw_value or raw_value == "-9999":
                continue
            try:
                obs_date = datetime.date(year, month, day)
            except ValueError:
                continue
            rows.append(
                {
                    "station_id": station_id,
                    "obs_date": obs_date,
                    "element": element,
                    "data_value": int(raw_value),
                    "m_flag": line[pos + 5 : pos + 6].strip() or None,
                    "q_flag": line[pos + 6 : pos + 7].strip() or None,
                    "s_flag": line[pos + 7 : pos + 8].strip() or None,
                    "obs_time": None,
                }
            )
    return rows


def ingest_station(station_id: str, years: set, conn) -> int:
    try:
        content = fetch_station_dly(station_id)
    except requests.HTTPError as e:
        logger.warning("Could not fetch %s: %s", station_id, e)
        return 0

    rows = parse_dly(content, station_id, years)
    if not rows:
        logger.warning("No data for station %s in years %s", station_id, sorted(years))
        return 0

    df = pd.DataFrame(rows)
    df["obs_date"] = pd.to_datetime(df["obs_date"])

    for year in years:
        conn.execute(
            "DELETE FROM raw.ghcn_daily WHERE station_id = ? AND YEAR(obs_date) = ?",
            [station_id, year],
        )

    conn.register("daily_df", df)
    conn.execute("INSERT INTO raw.ghcn_daily SELECT * FROM daily_df")
    conn.unregister("daily_df")
    logger.info("Station %s: %d rows loaded", station_id, len(df))
    return len(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _resolve_stations(args, conn) -> list[str]:
    """Return station IDs based on --stations, --state, --country, or --city."""
    if args.stations:
        return args.stations
    if args.state:
        rows = conn.execute(
            "SELECT station_id FROM raw.ghcn_stations WHERE state = ? ORDER BY station_id",
            [args.state],
        ).fetchall()
        return [r[0] for r in rows]
    if args.country:
        rows = conn.execute(
            "SELECT station_id FROM raw.ghcn_stations WHERE LEFT(station_id, 2) = ? ORDER BY station_id",
            [args.country],
        ).fetchall()
        return [r[0] for r in rows]
    if args.city:
        geolocator = Nominatim(user_agent="weather-data-pipeline")
        location = geolocator.geocode(args.city, timeout=10)
        if location is None:
            logger.error("Could not geocode '%s'", args.city)
            return []
        lat, lon = location.latitude, location.longitude
        radius = args.radius
        logger.info("Geocoded '%s' to (%.4f, %.4f), searching %d km radius", args.city, lat, lon, radius)
        deg_margin = radius / 111.0 + 0.5
        rows = conn.execute(
            """
            SELECT station_id, latitude, longitude
            FROM raw.ghcn_stations
            WHERE latitude  BETWEEN ? AND ?
              AND longitude BETWEEN ? AND ?
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """,
            [lat - deg_margin, lat + deg_margin, lon - deg_margin, lon + deg_margin],
        ).fetchall()
        return [sid for sid, slat, slon in rows if _haversine_km(lat, lon, slat, slon) <= radius]
    return []


def main():
    parser = argparse.ArgumentParser(description="Ingest NOAA GHCN-Daily data (per-station)")
    parser.add_argument("--years", nargs="+", type=int, required=True)

    location = parser.add_mutually_exclusive_group(required=True)
    location.add_argument("--stations", nargs="+", help="Explicit station IDs")
    location.add_argument("--state", help="US state code (e.g. AK, CA, TX)")
    location.add_argument("--country", help="NOAA country code prefix (e.g. CA, GM, NO)")
    location.add_argument("--city", help="City name to geocode (e.g. 'Seattle, WA')")

    parser.add_argument("--radius", type=int, default=50, help="Search radius in km when using --city (default: 50)")

    parser.add_argument("--skip-stations-meta", action="store_true")
    parser.add_argument("--force", action="store_true", help="Truncate all data before loading")
    args = parser.parse_args()

    years = set(args.years)

    conn = get_conn()
    init_db(conn)

    if args.force:
        conn.execute("DELETE FROM raw.ghcn_daily")
        logger.info("Cleared raw.ghcn_daily")

    if not args.skip_stations_meta:
        ingest_stations(conn)

    stations = _resolve_stations(args, conn)
    if not stations:
        logger.error(
            "No stations found for the specified location. "
            "Run with --skip-stations-meta=false to load station metadata first."
        )
        return

    logger.info("Ingesting %d stations for years %s", len(stations), sorted(years))

    total = 0
    for sid in stations:
        total += ingest_station(sid, years, conn)

    conn.close()
    logger.info("Ingestion complete. Total rows loaded: %d", total)


if __name__ == "__main__":
    main()
