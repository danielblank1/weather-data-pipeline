"""@bruin
name: raw.ghcn_daily
type: python
connection: weather_warehouse
depends:
  - raw.ghcn_stations
materialization:
  type: none

parameters:
  year: 2025
  state: ""
  country: ""

description: >
  Download per-station GHCN .dly files and load into DuckDB raw layer.
  Requires either 'state' (US state code, e.g. AK) or 'country' (NOAA
  country prefix, e.g. CA) to be set — no default location.
@bruin"""

import datetime
import os

import duckdb
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GHCN_STATION_FILE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/weather.duckdb")

CORE_ELEMENTS = {"TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "TAVG"}


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


def _fetch_dly(station_id: str) -> str:
    url = GHCN_STATION_FILE_URL.format(station_id=station_id)
    session = _http_session()
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


def _parse_dly(content: str, station_id: str, year: int) -> list[dict]:
    rows = []
    for line in content.splitlines():
        if len(line) < 21:
            continue
        if int(line[11:15]) != year:
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


def _get_stations(context, conn) -> list[str]:
    params = context.params if (context and hasattr(context, "params")) else {}
    state = params.get("state", "").strip()
    country = params.get("country", "").strip()
    if state:
        rows = conn.execute(
            "SELECT station_id FROM raw.ghcn_stations WHERE state = ? ORDER BY station_id",
            [state],
        ).fetchall()
        return [r[0] for r in rows]
    if country:
        rows = conn.execute(
            "SELECT station_id FROM raw.ghcn_stations WHERE LEFT(station_id, 2) = ? ORDER BY station_id",
            [country],
        ).fetchall()
        return [r[0] for r in rows]
    return []


def materialize(context=None):
    year = datetime.datetime.now().year
    if context and hasattr(context, "params"):
        year = int(context.params.get("year", year))

    conn = duckdb.connect(DUCKDB_PATH)
    stations = _get_stations(context, conn)
    conn.close()

    if not stations:
        print("ERROR: No location specified. Set 'state' or 'country' parameter.")
        return

    print(f"Ingesting year {year} for {len(stations)} stations ...")

    all_rows = []
    for sid in stations:
        try:
            content = _fetch_dly(sid)
            rows = _parse_dly(content, sid, year)
            all_rows.extend(rows)
            print(f"  {sid}: {len(rows)} rows")
        except requests.HTTPError as e:
            print(f"  {sid}: WARN — {e}")

    if not all_rows:
        print(f"No data found for year {year}")
        return

    df = pd.DataFrame(all_rows)
    df["obs_date"] = pd.to_datetime(df["obs_date"])

    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("DELETE FROM raw.ghcn_daily WHERE YEAR(obs_date) = ?", [year])
    conn.register("daily_df", df)
    conn.execute("INSERT INTO raw.ghcn_daily SELECT * FROM daily_df")
    conn.unregister("daily_df")
    conn.close()
    print(f"Year {year}: loaded {len(df):,} rows into DuckDB")
