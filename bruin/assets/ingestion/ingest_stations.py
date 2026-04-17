"""@bruin
name: raw.ghcn_stations
type: python
connection: weather_warehouse
depends: []
materialization:
  type: none

description: "Download GHCN station metadata and load into DuckDB raw layer."
@bruin"""

import os

import duckdb
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GHCN_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/weather.duckdb")


def _ensure_schema(conn):
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


def _safe_float(s: str):
    s = s.strip()
    if not s or s == "-999.9":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def materialize(context=None):
    print("Downloading GHCN station metadata ...")
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
    print(f"Parsed {len(df):,} stations with valid coordinates")

    conn = duckdb.connect(DUCKDB_PATH)
    _ensure_schema(conn)
    conn.execute("DELETE FROM raw.ghcn_stations")
    conn.register("stations_df", df)
    conn.execute("INSERT INTO raw.ghcn_stations SELECT * FROM stations_df")
    conn.unregister("stations_df")
    conn.close()
    print(f"Loaded {len(df):,} stations into DuckDB")
