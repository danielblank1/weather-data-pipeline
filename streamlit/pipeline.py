"""
pipeline.py — ingestion and transform logic for Streamlit.

Runs ingestion (NOAA .dly per-station files → DuckDB) and
executes the Bruin transform SQL directly against DuckDB.
"""

import datetime
import math
import os
import re
from typing import Callable

import duckdb
import pandas as pd
import requests
from geopy.geocoders import Nominatim
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/weather.duckdb")
TRANSFORMS_DIR = os.getenv("TRANSFORMS_DIR", "/transforms")

GHCN_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
GHCN_STATION_FILE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"

CORE_ELEMENTS = {"TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "TAVG"}

# Transform SQL files executed in dependency order
TRANSFORM_FILES = [
    "stg_daily_observations.sql",
    "stg_stations.sql",
    "mart_daily_weather.sql",
    "mart_monthly_summary.sql",
    "mart_prediction_features.sql",
]

# ---------------------------------------------------------------------------
# US state code → full name
# ---------------------------------------------------------------------------

US_STATES = {
    "AK": "Alaska",
    "AL": "Alabama",
    "AR": "Arkansas",
    "AZ": "Arizona",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}

# NOAA FIPS country code prefixes → full name
# NOAA uses FIPS codes (not ISO 3166) for the first 2 chars of station IDs
COUNTRY_NAMES = {
    "AA": "Aruba",
    "AC": "Antigua and Barbuda",
    "AE": "United Arab Emirates",
    "AF": "Afghanistan",
    "AG": "Algeria",
    "AJ": "Azerbaijan",
    "AL": "Albania",
    "AM": "Armenia",
    "AO": "Angola",
    "AQ": "American Samoa",
    "AR": "Argentina",
    "AS": "Australia",
    "AU": "Austria",
    "AY": "Antarctica",
    "BA": "Bahrain",
    "BB": "Barbados",
    "BC": "Botswana",
    "BD": "Bangladesh",
    "BE": "Belgium",
    "BF": "Bahamas",
    "BG": "Bangladesh",
    "BH": "Belize",
    "BK": "Bosnia and Herzegovina",
    "BL": "Bolivia",
    "BM": "Burma (Myanmar)",
    "BN": "Benin",
    "BO": "Belarus",
    "BP": "Solomon Islands",
    "BR": "Brazil",
    "BU": "Bulgaria",
    "BX": "Brunei",
    "BY": "Burundi",
    "CA": "Canada",
    "CB": "Cambodia",
    "CD": "Chad",
    "CE": "Sri Lanka",
    "CF": "Congo (Brazzaville)",
    "CG": "Congo (Kinshasa)",
    "CH": "China",
    "CI": "Chile",
    "CJ": "Cayman Islands",
    "CK": "Cocos Islands",
    "CM": "Cameroon",
    "CN": "Comoros",
    "CO": "Colombia",
    "CQ": "N. Mariana Islands",
    "CS": "Costa Rica",
    "CT": "Central African Republic",
    "CU": "Cuba",
    "CV": "Cape Verde",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "DA": "Denmark",
    "DJ": "Djibouti",
    "DO": "Dominica",
    "DR": "Dominican Republic",
    "EC": "Ecuador",
    "EG": "Egypt",
    "EI": "Ireland",
    "EK": "Equatorial Guinea",
    "EN": "Estonia",
    "ER": "Eritrea",
    "ES": "El Salvador",
    "ET": "Ethiopia",
    "EZ": "Czech Republic",
    "FG": "French Guiana",
    "FI": "Finland",
    "FJ": "Fiji",
    "FK": "Falkland Islands",
    "FM": "Micronesia",
    "FO": "Faroe Islands",
    "FP": "French Polynesia",
    "FR": "France",
    "GA": "Gambia",
    "GB": "Gabon",
    "GG": "Georgia",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GJ": "Grenada",
    "GK": "Guernsey",
    "GL": "Greenland",
    "GM": "Germany",
    "GP": "Guadeloupe",
    "GQ": "Guam",
    "GR": "Greece",
    "GT": "Guatemala",
    "GV": "Guinea",
    "GY": "Guyana",
    "HA": "Haiti",
    "HK": "Hong Kong",
    "HO": "Honduras",
    "HR": "Croatia",
    "HU": "Hungary",
    "IC": "Iceland",
    "ID": "Indonesia",
    "IM": "Isle of Man",
    "IN": "India",
    "IO": "British Indian Ocean Territory",
    "IR": "Iran",
    "IS": "Israel",
    "IT": "Italy",
    "IV": "Ivory Coast",
    "JA": "Japan",
    "JM": "Jamaica",
    "JN": "Jan Mayen",
    "JO": "Jordan",
    "KE": "Kenya",
    "KG": "Kyrgyzstan",
    "KN": "North Korea",
    "KR": "Kiribati",
    "KS": "South Korea",
    "KU": "Kuwait",
    "KZ": "Kazakhstan",
    "LA": "Laos",
    "LE": "Lebanon",
    "LG": "Latvia",
    "LH": "Lithuania",
    "LI": "Liberia",
    "LO": "Slovakia",
    "LS": "Liechtenstein",
    "LT": "Lesotho",
    "LU": "Luxembourg",
    "LY": "Libya",
    "MA": "Madagascar",
    "MB": "Martinique",
    "MC": "Macau",
    "MD": "Moldova",
    "MF": "Mayotte",
    "MG": "Mongolia",
    "MH": "Montserrat",
    "MI": "Malawi",
    "MJ": "Montenegro",
    "MK": "North Macedonia",
    "ML": "Mali",
    "MN": "Monaco",
    "MO": "Morocco",
    "MP": "Mauritius",
    "MR": "Mauritania",
    "MT": "Malta",
    "MU": "Oman",
    "MV": "Maldives",
    "MX": "Mexico",
    "MY": "Malaysia",
    "MZ": "Mozambique",
    "NC": "New Caledonia",
    "NE": "Niue",
    "NG": "Niger",
    "NH": "Vanuatu",
    "NI": "Nigeria",
    "NL": "Netherlands",
    "NO": "Norway",
    "NP": "Nepal",
    "NR": "Nauru",
    "NS": "Suriname",
    "NU": "Nicaragua",
    "NZ": "New Zealand",
    "PA": "Paraguay",
    "PC": "Pitcairn Islands",
    "PE": "Peru",
    "PK": "Pakistan",
    "PL": "Poland",
    "PM": "Panama",
    "PO": "Portugal",
    "PP": "Papua New Guinea",
    "PS": "Palau",
    "PU": "Guinea-Bissau",
    "QA": "Qatar",
    "RE": "Reunion",
    "RI": "Serbia",
    "RM": "Marshall Islands",
    "RO": "Romania",
    "RP": "Philippines",
    "RQ": "Puerto Rico",
    "RS": "Russia",
    "RW": "Rwanda",
    "SA": "Saudi Arabia",
    "SB": "St. Pierre and Miquelon",
    "SC": "St. Kitts and Nevis",
    "SE": "Seychelles",
    "SF": "South Africa",
    "SG": "Senegal",
    "SH": "St. Helena",
    "SI": "Slovenia",
    "SL": "Sierra Leone",
    "SM": "San Marino",
    "SN": "Singapore",
    "SO": "Somalia",
    "SP": "Spain",
    "ST": "St. Lucia",
    "SU": "Sudan",
    "SV": "Svalbard",
    "SW": "Sweden",
    "SX": "South Georgia",
    "SY": "Syria",
    "SZ": "Switzerland",
    "TD": "Trinidad and Tobago",
    "TH": "Thailand",
    "TI": "Tajikistan",
    "TK": "Turks and Caicos",
    "TL": "Tokelau",
    "TN": "Tonga",
    "TO": "Togo",
    "TP": "Sao Tome and Principe",
    "TS": "Tunisia",
    "TU": "Turkey",
    "TV": "Tuvalu",
    "TW": "Taiwan",
    "TX": "Turkmenistan",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "UK": "United Kingdom",
    "UP": "Ukraine",
    "US": "United States",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VC": "St. Vincent and the Grenadines",
    "VE": "Venezuela",
    "VI": "British Virgin Islands",
    "VM": "Vietnam",
    "VQ": "US Virgin Islands",
    "WA": "Namibia",
    "WE": "West Bank",
    "WF": "Wallis and Futuna",
    "WI": "Western Sahara",
    "WS": "Samoa",
    "WZ": "Eswatini",
    "YM": "Yemen",
    "ZA": "Zambia",
    "ZI": "Zimbabwe",
}


# NOAA FIPS 2-letter → ISO 3166-1 alpha-3
FIPS_TO_ISO3 = {
    "AA": "ABW",
    "AC": "ATG",
    "AE": "ARE",
    "AF": "AFG",
    "AG": "DZA",
    "AJ": "AZE",
    "AL": "ALB",
    "AM": "ARM",
    "AO": "AGO",
    "AQ": "ASM",
    "AR": "ARG",
    "AS": "AUS",
    "AU": "AUT",
    "AY": "ATA",
    "BA": "BHR",
    "BB": "BRB",
    "BC": "BWA",
    "BD": "BGD",
    "BE": "BEL",
    "BF": "BHS",
    "BG": "BGD",
    "BH": "BLZ",
    "BK": "BIH",
    "BL": "BOL",
    "BM": "MMR",
    "BN": "BEN",
    "BO": "BLR",
    "BP": "SLB",
    "BR": "BRA",
    "BU": "BGR",
    "BX": "BRN",
    "BY": "BDI",
    "CA": "CAN",
    "CB": "KHM",
    "CD": "TCD",
    "CE": "LKA",
    "CF": "COG",
    "CG": "COD",
    "CH": "CHN",
    "CI": "CHL",
    "CJ": "CYM",
    "CK": "CCK",
    "CM": "CMR",
    "CN": "COM",
    "CO": "COL",
    "CQ": "MNP",
    "CS": "CRI",
    "CT": "CAF",
    "CU": "CUB",
    "CV": "CPV",
    "CY": "CYP",
    "CZ": "CZE",
    "DA": "DNK",
    "DJ": "DJI",
    "DO": "DMA",
    "DR": "DOM",
    "EC": "ECU",
    "EG": "EGY",
    "EI": "IRL",
    "EK": "GNQ",
    "EN": "EST",
    "ER": "ERI",
    "ES": "SLV",
    "ET": "ETH",
    "EZ": "CZE",
    "FG": "GUF",
    "FI": "FIN",
    "FJ": "FJI",
    "FK": "FLK",
    "FM": "FSM",
    "FO": "FRO",
    "FP": "PYF",
    "FR": "FRA",
    "GA": "GMB",
    "GB": "GAB",
    "GG": "GEO",
    "GH": "GHA",
    "GI": "GIB",
    "GJ": "GRD",
    "GK": "GGY",
    "GL": "GRL",
    "GM": "DEU",
    "GP": "GLP",
    "GQ": "GUM",
    "GR": "GRC",
    "GT": "GTM",
    "GV": "GIN",
    "GY": "GUY",
    "HA": "HTI",
    "HK": "HKG",
    "HO": "HND",
    "HR": "HRV",
    "HU": "HUN",
    "IC": "ISL",
    "ID": "IDN",
    "IM": "IMN",
    "IN": "IND",
    "IO": "IOT",
    "IR": "IRN",
    "IS": "ISR",
    "IT": "ITA",
    "IV": "CIV",
    "JA": "JPN",
    "JM": "JAM",
    "JN": "SJM",
    "JO": "JOR",
    "KE": "KEN",
    "KG": "KGZ",
    "KN": "PRK",
    "KR": "KIR",
    "KS": "KOR",
    "KU": "KWT",
    "KZ": "KAZ",
    "LA": "LAO",
    "LE": "LBN",
    "LG": "LVA",
    "LH": "LTU",
    "LI": "LBR",
    "LO": "SVK",
    "LS": "LIE",
    "LT": "LSO",
    "LU": "LUX",
    "LY": "LBY",
    "MA": "MDG",
    "MB": "MTQ",
    "MC": "MAC",
    "MD": "MDA",
    "MF": "MYT",
    "MG": "MNG",
    "MH": "MSR",
    "MI": "MWI",
    "MJ": "MNE",
    "MK": "MKD",
    "ML": "MLI",
    "MN": "MCO",
    "MO": "MAR",
    "MP": "MUS",
    "MR": "MRT",
    "MT": "MLT",
    "MU": "OMN",
    "MV": "MDV",
    "MX": "MEX",
    "MY": "MYS",
    "MZ": "MOZ",
    "NC": "NCL",
    "NE": "NIU",
    "NG": "NER",
    "NH": "VUT",
    "NI": "NGA",
    "NL": "NLD",
    "NO": "NOR",
    "NP": "NPL",
    "NR": "NRU",
    "NS": "SUR",
    "NU": "NIC",
    "NZ": "NZL",
    "PA": "PRY",
    "PC": "PCN",
    "PE": "PER",
    "PK": "PAK",
    "PL": "POL",
    "PM": "PAN",
    "PO": "PRT",
    "PP": "PNG",
    "PS": "PLW",
    "PU": "GNB",
    "QA": "QAT",
    "RE": "REU",
    "RI": "SRB",
    "RM": "MHL",
    "RO": "ROU",
    "RP": "PHL",
    "RQ": "PRI",
    "RS": "RUS",
    "RW": "RWA",
    "SA": "SAU",
    "SB": "SPM",
    "SC": "KNA",
    "SE": "SYC",
    "SF": "ZAF",
    "SG": "SEN",
    "SH": "SHN",
    "SI": "SVN",
    "SL": "SLE",
    "SM": "SMR",
    "SN": "SGP",
    "SO": "SOM",
    "SP": "ESP",
    "ST": "LCA",
    "SU": "SDN",
    "SV": "SJM",
    "SW": "SWE",
    "SX": "SGS",
    "SY": "SYR",
    "SZ": "CHE",
    "TD": "TTO",
    "TH": "THA",
    "TI": "TJK",
    "TK": "TCA",
    "TL": "TKL",
    "TN": "TON",
    "TO": "TGO",
    "TP": "STP",
    "TS": "TUN",
    "TU": "TUR",
    "TV": "TUV",
    "TW": "TWN",
    "TX": "TKM",
    "TZ": "TZA",
    "UG": "UGA",
    "UK": "GBR",
    "UP": "UKR",
    "US": "USA",
    "UY": "URY",
    "UZ": "UZB",
    "VC": "VCT",
    "VE": "VEN",
    "VI": "VGB",
    "VM": "VNM",
    "VQ": "VIR",
    "WA": "NAM",
    "WE": "PSE",
    "WF": "WLF",
    "WI": "ESH",
    "WS": "WSM",
    "WZ": "SWZ",
    "YM": "YEM",
    "ZA": "ZMB",
    "ZI": "ZWE",
}


def country_display(code: str) -> str:
    name = COUNTRY_NAMES.get(code, code)
    iso3 = FIPS_TO_ISO3.get(code, code)
    return f"{name} ({iso3})"


# ---------------------------------------------------------------------------
# Location helpers
# ---------------------------------------------------------------------------


def get_us_states_available(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return {code: 'Full Name (XX)'} for US states present in raw.ghcn_stations."""
    rows = conn.execute("""
        SELECT DISTINCT state
        FROM raw.ghcn_stations
        WHERE LEFT(station_id, 2) = 'US'
          AND state IS NOT NULL AND state != ''
        ORDER BY state
    """).fetchall()
    result = {}
    for (code,) in rows:
        name = US_STATES.get(code, code)
        result[code] = f"{name} ({code})"
    # Sort by full display name (alphabetical)
    return dict(sorted(result.items(), key=lambda x: x[1]))


def get_countries_available(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return {code: 'Country Name (ISO3)'} for all countries in raw.ghcn_stations."""
    rows = conn.execute("""
        SELECT DISTINCT LEFT(station_id, 2) AS country_code
        FROM raw.ghcn_stations
        ORDER BY 1
    """).fetchall()
    result = {code: country_display(code) for (code,) in rows}
    # Sort by full country name (the display value) instead of code
    return dict(sorted(result.items(), key=lambda x: x[1]))


def get_station_ids_for_location(
    conn: duckdb.DuckDBPyConnection,
    location_type: str,
    location_code: str,
) -> list[str]:
    """Return all station IDs for a given US state or country code."""
    if location_type == "US State":
        rows = conn.execute(
            "SELECT station_id FROM raw.ghcn_stations WHERE state = ? ORDER BY station_id",
            [location_code],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT station_id FROM raw.ghcn_stations WHERE LEFT(station_id, 2) = ? ORDER BY station_id",
            [location_code],
        ).fetchall()
    return [r[0] for r in rows]


def get_station_names_for_location(
    conn: duckdb.DuckDBPyConnection,
    country_code: str,
    state_code: str | None = None,
) -> list[tuple[str, str]]:
    """Return [(station_id, station_name)] for a country/state, sorted by name."""
    if state_code:
        rows = conn.execute(
            """
            SELECT station_id, station_name
            FROM raw.ghcn_stations
            WHERE state = ? AND LEFT(station_id, 2) = 'US'
            ORDER BY station_name
        """,
            [state_code],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT station_id, station_name
            FROM raw.ghcn_stations
            WHERE LEFT(station_id, 2) = ?
            ORDER BY station_name
        """,
            [country_code],
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_cities_for_location(
    conn: duckdb.DuckDBPyConnection,
    country_code: str,
    state_code: str | None = None,
) -> list[dict]:
    """
    Return distinct station names with coordinates for a country/state.
    Returns [{"name": ..., "latitude": ..., "longitude": ...}] sorted by name.
    """
    if state_code:
        rows = conn.execute(
            """
            SELECT DISTINCT station_name, latitude, longitude
            FROM raw.ghcn_stations
            WHERE state = ? AND LEFT(station_id, 2) = 'US'
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY station_name
        """,
            [state_code],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT DISTINCT station_name, latitude, longitude
            FROM raw.ghcn_stations
            WHERE LEFT(station_id, 2) = ?
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY station_name
        """,
            [country_code],
        ).fetchall()

    return [{"name": r[0], "latitude": r[1], "longitude": r[2]} for r in rows]


def search_stations_by_name(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    country_code: str,
    state_code: str | None = None,
) -> list[dict]:
    """Search station names matching query within a country/state. Returns up to 50."""
    like_pattern = f"%{query.upper()}%"
    if state_code:
        rows = conn.execute(
            """
            SELECT DISTINCT station_name, latitude, longitude,
                   state, LEFT(station_id, 2) AS country_code
            FROM raw.ghcn_stations
            WHERE state = ? AND LEFT(station_id, 2) = 'US'
              AND UPPER(station_name) LIKE ?
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY station_name
            LIMIT 50
        """,
            [state_code, like_pattern],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT DISTINCT station_name, latitude, longitude,
                   state, LEFT(station_id, 2) AS country_code
            FROM raw.ghcn_stations
            WHERE LEFT(station_id, 2) = ?
              AND UPPER(station_name) LIKE ?
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY station_name
            LIMIT 50
        """,
            [country_code, like_pattern],
        ).fetchall()

    results = []
    for name, lat, lon, st, cc in rows:
        # Build location string: "Name, State, Country"
        parts = [name]
        if st:
            state_name = US_STATES.get(st, st)
            parts.append(state_name)
        country_name = COUNTRY_NAMES.get(cc, cc)
        parts.append(country_name)
        results.append(
            {
                "name": name,
                "display": ", ".join(parts),
                "latitude": lat,
                "longitude": lon,
            }
        )
    return results


_geocode_cache: dict[str, tuple[float, float, str] | None] = {}


def geocode_city(city_name: str) -> tuple[float, float, str] | None:
    """Geocode a city name to (lat, lon, display_name). Returns None on failure."""
    key = city_name.strip().lower()
    if key in _geocode_cache:
        return _geocode_cache[key]
    try:
        geolocator = Nominatim(user_agent="weather-data-pipeline")
        location = geolocator.geocode(city_name, timeout=10)
    except Exception:
        return _geocode_cache.get(key)
    result = (location.latitude, location.longitude, location.address) if location else None
    _geocode_cache[key] = result
    return result


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_stations_near_point(
    conn: duckdb.DuckDBPyConnection,
    lat: float,
    lon: float,
    radius_km: float,
) -> list[dict]:
    """Return stations within radius_km of (lat, lon), sorted by distance."""
    # Use a bounding box to narrow the SQL query, then filter precisely
    deg_margin = radius_km / 111.0 + 0.5  # rough degrees
    rows = conn.execute(
        """
        SELECT station_id, station_name, latitude, longitude
        FROM raw.ghcn_stations
        WHERE latitude  BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
          AND latitude IS NOT NULL AND longitude IS NOT NULL
    """,
        [lat - deg_margin, lat + deg_margin, lon - deg_margin, lon + deg_margin],
    ).fetchall()

    results = []
    for sid, name, slat, slon in rows:
        dist = _haversine_km(lat, lon, slat, slon)
        if dist <= radius_km:
            results.append(
                {
                    "station_id": sid,
                    "station_name": name,
                    "latitude": slat,
                    "longitude": slon,
                    "distance_km": round(dist, 1),
                }
            )
    results.sort(key=lambda r: r["distance_km"])
    return results


def already_ingested_stations(
    conn: duckdb.DuckDBPyConnection,
    station_ids: list[str],
    years: list[int],
) -> set[str]:
    """Return station IDs that already have data for ALL requested years."""
    if not station_ids or not years:
        return set()
    ids_sql = ", ".join(f"'{s}'" for s in station_ids)
    rows = conn.execute(f"""
        SELECT station_id
        FROM raw.ghcn_daily
        WHERE station_id IN ({ids_sql})
          AND YEAR(obs_date) IN ({", ".join(str(y) for y in years)})
        GROUP BY station_id
        HAVING COUNT(DISTINCT YEAR(obs_date)) = {len(years)}
    """).fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


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


def _parse_dly(content: str, station_id: str, years: set) -> list[dict]:
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


def ingest_location(
    conn: duckdb.DuckDBPyConnection,
    station_ids: list[str],
    years: list[int],
    on_progress: Callable[[str], None] | None = None,
) -> dict:
    """
    Download and load NOAA .dly data for the given stations and years.
    Returns a summary dict with loaded/skipped/error counts.
    """
    year_set = set(years)
    loaded = skipped = errors = 0
    session = _http_session()

    for i, sid in enumerate(station_ids):
        if on_progress:
            on_progress(f"[{i + 1}/{len(station_ids)}] Fetching {sid} …")
        try:
            resp = session.get(
                GHCN_STATION_FILE_URL.format(station_id=sid),
                timeout=60,
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            if on_progress:
                on_progress(f"  ⚠️ {sid}: {e}")
            errors += 1
            continue

        rows = _parse_dly(resp.text, sid, year_set)
        if not rows:
            if on_progress:
                on_progress(f"  — {sid}: no data for selected years")
            skipped += 1
            continue

        df = pd.DataFrame(rows)
        df["obs_date"] = pd.to_datetime(df["obs_date"])

        for year in years:
            conn.execute(
                "DELETE FROM raw.ghcn_daily WHERE station_id = ? AND YEAR(obs_date) = ?",
                [sid, year],
            )
        conn.register("_ingest_df", df)
        conn.execute("INSERT INTO raw.ghcn_daily SELECT * FROM _ingest_df")
        conn.unregister("_ingest_df")

        if on_progress:
            on_progress(f"  ✓ {sid}: {len(df):,} rows")
        loaded += 1

    return {"loaded": loaded, "skipped": skipped, "errors": errors}


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

_BRUIN_HEADER_RE = re.compile(r"/\*\s*@bruin.*?@bruin\s*\*/", re.DOTALL)
_BRUIN_NAME_RE = re.compile(r"name:\s*(\S+)")


def _load_transform_sql(filename: str) -> str:
    path = os.path.join(TRANSFORMS_DIR, filename)
    with open(path) as f:
        sql = f.read()

    # Extract the target table name from the Bruin header (e.g. "staging.stg_daily_observations")
    header_match = _BRUIN_HEADER_RE.search(sql)
    table_name = None
    if header_match:
        name_match = _BRUIN_NAME_RE.search(header_match.group())
        if name_match:
            table_name = name_match.group(1)

    # Strip the Bruin header, leaving just the SELECT
    select_sql = _BRUIN_HEADER_RE.sub("", sql).strip()

    # Wrap in CREATE OR REPLACE TABLE so the mart/staging tables actually get created
    if table_name:
        schema = table_name.split(".")[0] if "." in table_name else "public"
        return f"CREATE SCHEMA IF NOT EXISTS {schema};\nCREATE OR REPLACE TABLE {table_name} AS (\n{select_sql}\n);"
    return select_sql


def run_transforms(
    conn: duckdb.DuckDBPyConnection,
    on_progress: Callable[[str], None] | None = None,
):
    """Execute all transform SQL files in dependency order."""
    for fname in TRANSFORM_FILES:
        label = fname.replace(".sql", "").replace("_", ".")
        if on_progress:
            on_progress(f"Running {label} …")
        sql = _load_transform_sql(fname)
        conn.execute(sql)
        if on_progress:
            on_progress(f"  ✓ {label}")
