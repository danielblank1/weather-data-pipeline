"""@bruin
name: raw.ghcn_stations_ingest
type: python
connection: weather_warehouse
depends: []
materialization:
  type: table
  strategy: create+replace

columns:
  - name: station_id
    type: string
    description: "11-character GHCN station ID"
    checks:
      - name: not_null
      - name: unique
  - name: latitude
    type: float
    description: "Station latitude"
    checks:
      - name: not_null
  - name: longitude
    type: float
    description: "Station longitude"
    checks:
      - name: not_null
  - name: station_name
    type: string
    description: "Human-readable station name"
    checks:
      - name: not_null

custom_checks:
  - name: "station count is reasonable"
    description: "GHCN has 100k+ stations globally"
    query: SELECT count(*) > 50000 FROM raw.ghcn_stations
    value: 1
@bruin"""

import pandas as pd
import requests

GHCN_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"


def materialize(context):
    """
    Download GHCN station metadata and return as DataFrame.
    The fixed-width format is parsed positionally per NOAA spec.
    """
    print("Downloading GHCN station metadata ...")
    resp = requests.get(GHCN_STATIONS_URL, timeout=120)
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
    print(f"Parsed {len(df):,} stations")

    # Filter out stations with missing coordinates
    df = df.dropna(subset=["latitude", "longitude"])
    print(f"After filtering: {len(df):,} stations with valid coordinates")

    return df


def _safe_float(s: str):
    """Safely parse a float from a fixed-width field."""
    s = s.strip()
    if not s or s == "-999.9":
        return None
    try:
        return float(s)
    except ValueError:
        return None
