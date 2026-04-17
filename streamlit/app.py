import os
import random

import duckdb
import numpy as np
import pandas as pd
import pipeline
import plotly.express as px
import plotly.graph_objects as go

import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/weather.duckdb")
CURRENT_YEAR = 2025

st.set_page_config(
    page_title="Weather Dashboard",
    page_icon="🌨️",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Temperature unit helpers
# ---------------------------------------------------------------------------


def c_to_f(val):
    """Convert Celsius to Fahrenheit. Handles scalars, Series, and None/NaN."""
    if val is None:
        return None
    return val * 9 / 5 + 32


def temp(val):
    """Convert a temperature value based on the current unit toggle."""
    if not use_fahrenheit:
        return val
    if isinstance(val, (pd.Series, np.ndarray)):
        return c_to_f(val)
    if isinstance(val, (int, float)) and not np.isnan(val):
        return c_to_f(val)
    return val


def temp_unit() -> str:
    return "°F" if use_fahrenheit else "°C"


# ---------------------------------------------------------------------------
# Connection  (read-write — needed for ingestion)
# ---------------------------------------------------------------------------


@st.cache_resource
def get_conn():
    return duckdb.connect(DUCKDB_PATH)


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60)
def load_available_stations() -> pd.DataFrame:
    conn = get_conn()
    try:
        return conn.execute("""
            SELECT station_id, station_name, latitude, longitude,
                   elevation, climate_zone, has_temp
            FROM (
                SELECT DISTINCT station_id, station_name, latitude, longitude,
                       elevation, climate_zone,
                       CASE WHEN COUNT(tavg_derived_celsius) OVER (PARTITION BY station_id) > 0
                            THEN true ELSE false END AS has_temp
                FROM marts.mart_daily_weather
            )
            ORDER BY has_temp DESC, station_name
        """).df()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_daily(station_ids: tuple, year_range: tuple = (1900, 2100)) -> pd.DataFrame:
    conn = get_conn()
    ids = ", ".join(f"'{s}'" for s in station_ids)
    df = conn.execute(f"""
        SELECT station_id, station_name, obs_date, obs_year, obs_month,
               tmax_celsius, tmin_celsius, tavg_derived_celsius,
               temp_range_celsius, precipitation_mm, snowfall_mm,
               avg_wind_speed_ms, tavg_7d_avg, tavg_30d_avg,
               precip_7d_avg, precip_30d_total,
               temp_change_1d, had_precipitation, had_snowfall,
               is_extreme_cold, is_extreme_heat
        FROM marts.mart_daily_weather
        WHERE station_id IN ({ids})
          AND obs_year BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY station_id, obs_date
    """).df()
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    return df


@st.cache_data(ttl=60)
def load_monthly(station_ids: tuple, year_range: tuple = (1900, 2100)) -> pd.DataFrame:
    conn = get_conn()
    ids = ", ".join(f"'{s}'" for s in station_ids)
    df = conn.execute(f"""
        SELECT * FROM marts.mart_monthly_summary
        WHERE station_id IN ({ids})
          AND avg_temp_celsius IS NOT NULL
          AND obs_year BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY station_id, obs_year, obs_month
    """).df()
    df["month_label"] = df["obs_year"].astype(str) + "-" + df["obs_month"].astype(str).str.zfill(2)
    return df


@st.cache_data(ttl=60)
def load_prediction_features(station_ids: tuple, year_range: tuple = (1900, 2100)) -> pd.DataFrame:
    conn = get_conn()
    ids = ", ".join(f"'{s}'" for s in station_ids)
    df = conn.execute(f"""
        SELECT station_id, obs_date,
               tavg_derived_celsius, target_tavg_next_day,
               tavg_lag_1d, tavg_lag_7d,
               tavg_7d_avg, tavg_30d_avg,
               precipitation_mm, precip_lag_1d,
               temp_change_1d, temp_change_7d,
               season_sin, season_cos
        FROM marts.mart_prediction_features
        WHERE station_id IN ({ids})
          AND EXTRACT(YEAR FROM obs_date) BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY station_id, obs_date
    """).df()
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    return df


def _estimate_climate_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """Estimate climate features for all stations at once using numpy vectorization."""
    out = df.copy()
    lat = df["latitude"].values
    elev = np.nan_to_num(df["elevation"].values, nan=0)
    abs_lat = np.abs(lat)
    elev_km = np.maximum(elev, 0) / 1000.0

    base_temp = 27 - 0.45 * abs_lat - 6.5 * elev_km
    seasonal_amp = 1.5 + 0.35 * abs_lat
    summer_temp = base_temp + seasonal_amp * 0.5
    winter_temp = base_temp - seasonal_amp * 0.5

    variability = np.maximum(2 + 0.15 * abs_lat - 0.001 * abs_lat**2, 1)

    # Precipitation by latitude band
    precip = np.where(
        abs_lat < 15,
        1800 - 30 * abs_lat,
        np.where(
            abs_lat < 35,
            400 + 10 * (abs_lat - 15),
            np.where(abs_lat < 60, 600 + 15 * (60 - abs_lat), 300 - 3 * (abs_lat - 60)),
        ),
    )
    precip = np.maximum(precip, 50)

    # Snowfall
    snowfall = np.where(
        winter_temp < -5, 800 + np.abs(winter_temp) * 40, np.where(winter_temp < 3, 200 + (3 - winter_temp) * 80, 0)
    )

    heat_days = np.where(base_temp > 25, (base_temp - 25) * 8, 0)
    cold_days = np.where(winter_temp < -15, (-15 - winter_temp) * 6, 0)

    out["avg_temp_c"] = np.round(base_temp, 1)
    out["avg_summer_temp_c"] = np.round(summer_temp, 1)
    out["avg_winter_temp_c"] = np.round(winter_temp, 1)
    out["temp_variability"] = np.round(variability, 1)
    out["annual_precip_mm"] = np.round(precip, 0)
    out["annual_snowfall_mm"] = np.round(snowfall, 0)
    out["heat_days_per_year"] = np.round(heat_days, 0)
    out["cold_days_per_year"] = np.round(cold_days, 0)
    out["data_source"] = "estimated"
    return out


@st.cache_data(ttl=600)
def load_climate_profiles() -> pd.DataFrame:
    """
    Build global climate profiles for ALL stations.
    Uses real observed data where available, geographic estimates elsewhere.
    Vectorized for speed — no Python loops over 130k rows.
    """
    conn = get_conn()

    try:
        all_stations = conn.execute("""
            SELECT station_id, station_name, latitude, longitude,
                   COALESCE(elevation, 0) AS elevation,
                   LEFT(station_id, 2) AS country_code,
                   CASE
                       WHEN ABS(latitude) < 23.5 THEN 'Tropical'
                       WHEN ABS(latitude) < 35.0 THEN 'Subtropical'
                       WHEN ABS(latitude) < 55.0 THEN 'Temperate'
                       WHEN ABS(latitude) < 66.5 THEN 'Subarctic'
                       ELSE 'Polar'
                   END AS climate_zone
            FROM raw.ghcn_stations
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """).df()
    except Exception:
        return pd.DataFrame()

    if all_stations.empty:
        return pd.DataFrame()

    # Vectorized estimates for all stations
    result = _estimate_climate_vectorized(all_stations)

    # Overlay real observed data where available
    try:
        real = conn.execute("""
            SELECT
                station_id,
                AVG(avg_temp_celsius) AS avg_temp_c,
                AVG(CASE WHEN obs_month IN (6,7,8) THEN avg_temp_celsius END) AS avg_summer_temp_c,
                AVG(CASE WHEN obs_month IN (12,1,2) THEN avg_temp_celsius END) AS avg_winter_temp_c,
                AVG(temp_stddev) AS temp_variability,
                SUM(total_precip_mm) / COUNT(DISTINCT obs_year) AS annual_precip_mm,
                SUM(total_snowfall_mm) / COUNT(DISTINCT obs_year) AS annual_snowfall_mm,
                SUM(extreme_heat_days) * 1.0 / COUNT(DISTINCT obs_year) AS heat_days_per_year,
                SUM(extreme_cold_days) * 1.0 / COUNT(DISTINCT obs_year) AS cold_days_per_year
            FROM marts.mart_monthly_summary
            WHERE avg_temp_celsius IS NOT NULL
            GROUP BY station_id
            HAVING COUNT(DISTINCT obs_year) >= 1 AND COUNT(*) >= 6
        """).df()
        if not real.empty:
            real["data_source"] = "observed"
            # Update matching rows in result
            result = result.set_index("station_id")
            real = real.set_index("station_id")
            result.update(real)
            result = result.reset_index()
    except Exception:
        pass  # no mart data yet — estimates only

    return result


CLIMATE_FEATURES = [
    "avg_temp_c",
    "avg_summer_temp_c",
    "avg_winter_temp_c",
    "temp_variability",
    "annual_precip_mm",
    "annual_snowfall_mm",
    "heat_days_per_year",
    "cold_days_per_year",
]


def compute_climate_similarity(profiles_df: pd.DataFrame, user_prefs: dict) -> pd.DataFrame:
    """Compute cosine similarity between user preferences and each station's climate profile."""
    df = profiles_df.copy()

    # Fill NaN with 0 for similarity computation
    for col in CLIMATE_FEATURES:
        df[col] = df[col].fillna(0)

    # Build the feature matrix and normalize each feature to [0, 1]
    feature_cols = CLIMATE_FEATURES
    mins = df[feature_cols].min()
    maxs = df[feature_cols].max()
    ranges = maxs - mins
    ranges = ranges.replace(0, 1)  # avoid division by zero

    norm_df = (df[feature_cols] - mins) / ranges

    # Normalize user preferences the same way
    user_vec = np.array([(user_prefs[c] - mins[c]) / ranges[c] for c in feature_cols])

    # Cosine similarity
    station_matrix = norm_df.values
    dot_products = station_matrix @ user_vec
    station_norms = np.linalg.norm(station_matrix, axis=1)
    user_norm = np.linalg.norm(user_vec)
    denom = station_norms * user_norm
    denom[denom == 0] = 1  # avoid division by zero

    df["similarity"] = dot_products / denom
    df["similarity_pct"] = (df["similarity"] * 100).round(1)

    return df.sort_values("similarity", ascending=False)


def _get_diverse_top_results(results: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """Get top results with geographic diversity — max 3 stations per country."""
    top = []
    country_counts: dict[str, int] = {}
    for _, row in results.iterrows():
        cc = row.get("country_code", "??")
        if country_counts.get(cc, 0) < 3:
            top.append(row)
            country_counts[cc] = country_counts.get(cc, 0) + 1
        if len(top) >= n:
            break
    return pd.DataFrame(top)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_station_coords(conn, station_id: str) -> tuple[float, float]:
    """Get (lat, lon) for a station ID."""
    row = conn.execute(
        "SELECT latitude, longitude FROM raw.ghcn_stations WHERE station_id = ?",
        [station_id],
    ).fetchone()
    return (row[0], row[1]) if row else (0.0, 0.0)


def raw_stations_loaded() -> bool:
    """Check whether raw.ghcn_stations has been populated."""
    try:
        count = get_conn().execute("SELECT COUNT(*) FROM raw.ghcn_stations").fetchone()[0]
        return count > 0
    except Exception:
        return False


def marts_populated() -> bool:
    try:
        count = get_conn().execute("SELECT COUNT(*) FROM marts.mart_daily_weather").fetchone()[0]
        return count > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Sidebar — Location
# ---------------------------------------------------------------------------

st.sidebar.title("🌨️ Weather Dashboard")
temp_mode = st.sidebar.radio("Temperature", ["°C", "°F"], horizontal=True, key="temp_mode")
use_fahrenheit = temp_mode == "°F"
st.sidebar.markdown("---")
st.sidebar.subheader("📍 Location")

conn = get_conn()

# Ensure schemas exist before anything else
try:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    conn.execute("CREATE SCHEMA IF NOT EXISTS quality")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ghcn_stations (
            station_id VARCHAR, latitude DOUBLE, longitude DOUBLE,
            elevation DOUBLE, state VARCHAR, station_name VARCHAR,
            gsn_flag VARCHAR, hcn_flag VARCHAR, wmo_id VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ghcn_daily (
            station_id VARCHAR NOT NULL, obs_date DATE NOT NULL,
            element VARCHAR NOT NULL, data_value INTEGER,
            m_flag VARCHAR, q_flag VARCHAR, s_flag VARCHAR, obs_time VARCHAR
        )
    """)
except Exception:
    pass

# Check whether station metadata is available
station_ids = []
year_range = (2020, CURRENT_YEAR)
stations_meta_loaded = raw_stations_loaded()

if not stations_meta_loaded:
    # Seed station metadata from bundled DuckDB
    _seed_path = os.path.join(os.path.dirname(__file__), "ghcnd-stations.duckdb")
    try:
        conn.execute(f"ATTACH '{_seed_path}' AS seed (READ_ONLY)")
        conn.execute("INSERT INTO raw.ghcn_stations SELECT * FROM seed.raw.ghcn_stations")
        conn.execute("DETACH seed")
        stations_meta_loaded = True
    except Exception as e:
        st.sidebar.error(f"Failed to load station metadata: {e}")

if stations_meta_loaded:
    # ── Unified location flow: Country → State → City/Station ──

    station_ids = []
    selected_display = ""

    available_countries = pipeline.get_countries_available(conn)
    if not available_countries:
        st.sidebar.info("No countries found in station metadata.")
    else:
        # Country dropdown
        us_key = next((k for k, v in available_countries.items() if k == "US"), None)
        us_index = list(available_countries.values()).index(available_countries[us_key]) if us_key else 0

        selected_country_display = st.sidebar.selectbox(
            "Country",
            options=list(available_countries.values()),
            index=us_index,
        )
        selected_country_code = next(k for k, v in available_countries.items() if v == selected_country_display)
        selected_display = selected_country_display

        # State dropdown (US only)
        selected_state_code = None
        if selected_country_code == "US":
            available_states = pipeline.get_us_states_available(conn)
            if available_states:
                selected_state_display = st.sidebar.selectbox(
                    "State (optional)", options=["All states"] + list(available_states.values())
                )
                if selected_state_display != "All states":
                    selected_state_code = next(k for k, v in available_states.items() if v == selected_state_display)
                    selected_display = selected_state_display

        # City search — type to filter, results appear in dropdown
        city_search = st.sidebar.text_input("City", placeholder="Type to search…", key="city_search")

        if city_search and len(city_search) >= 2:
            matches = pipeline.search_stations_by_name(conn, city_search, selected_country_code, selected_state_code)
            if matches:
                city_info = matches[0]  # use the best match
                radius_km = st.sidebar.slider("Radius (km)", min_value=10, max_value=500, value=50, step=10)
                nearby = pipeline.get_stations_near_point(
                    conn, city_info["latitude"], city_info["longitude"], radius_km
                )
                station_ids = [s["station_id"] for s in nearby]
                if nearby:
                    st.sidebar.caption(f"📍 {city_info['display']} — {len(nearby)} stations within {radius_km} km")
                    selected_display = f"{city_info['name']} ({radius_km} km)"
                else:
                    st.sidebar.warning(f"No stations within {radius_km} km. Try a larger radius.")
            else:
                st.sidebar.caption("No matches found.")
                station_ids = pipeline.get_station_ids_for_location(
                    conn,
                    "US State" if selected_state_code else "Country",
                    selected_state_code or selected_country_code,
                )
        else:
            # No city search — use all stations for country/state
            station_ids = pipeline.get_station_ids_for_location(
                conn,
                "US State" if selected_state_code else "Country",
                selected_state_code or selected_country_code,
            )

    if station_ids:
        total_available = len(station_ids)
        st.sidebar.caption(f"{total_available:,} stations available")

        # Station limit slider
        if total_available > 10:
            max_stations = st.sidebar.slider(
                "Station limit",
                min_value=5,
                max_value=min(total_available, 2000),
                value=min(50, total_available),
                step=5,
                help="Maximum number of stations to download data for.",
            )
            if max_stations < total_available:
                # Prioritise stations likely to have temperature data:
                # USW* (airport/weather) and USC* (cooperative) report temp;
                # US1* (community observer) typically report only precip/snow.
                # Non-US stations generally have temp.
                priority = [s for s in station_ids if not s.startswith("US1")]
                others = [s for s in station_ids if s.startswith("US1")]
                rng = random.Random(42)
                rng.shuffle(priority)
                rng.shuffle(others)
                station_ids = sorted((priority + others)[:max_stations])
                st.sidebar.caption(f"Will download {len(station_ids)} of {total_available:,} stations")

        # Year range (slider to avoid freeze from keystroke reruns)
        year_range = st.sidebar.slider(
            "Year range",
            min_value=1900,
            max_value=CURRENT_YEAR,
            value=(2020, CURRENT_YEAR),
            step=1,
        )
        years = list(range(year_range[0], year_range[1] + 1))

        if len(station_ids) > 100:
            st.sidebar.warning(
                f"{len(station_ids):,} stations — this may take several minutes.",
                icon="⏱️",
            )

        if st.sidebar.button("🔨 Create Dashboard", type="primary"):
            log_area = st.sidebar.empty()
            messages: list[str] = []

            def on_progress(msg: str):
                messages.append(msg)
                log_area.code("\n".join(messages[-20:]))

            with st.spinner(f"Building {selected_display} ({len(station_ids)} stations, {years[0]}–{years[-1]})…"):
                result = pipeline.ingest_location(conn, station_ids, years, on_progress)

            on_progress("Running transforms…")
            try:
                pipeline.run_transforms(conn, on_progress)
                on_progress("✓ All transforms complete.")
            except Exception as e:
                on_progress(f"⚠️ Transform error: {e}")

            log_area.code("\n".join(messages[-20:]))
            st.sidebar.success(
                f"Done — {result['loaded']} stations loaded, {result['skipped']} skipped, {result['errors']} errors."
            )
            st.cache_data.clear()
            st.rerun()

st.sidebar.markdown("---")

# ---------------------------------------------------------------------------
# Sidebar — Run transforms (standalone)
# ---------------------------------------------------------------------------


def _raw_data_count() -> int:
    try:
        return get_conn().execute("SELECT COUNT(*) FROM raw.ghcn_daily").fetchone()[0]
    except Exception:
        return 0


raw_count = _raw_data_count()

if raw_count > 0 and not marts_populated():
    st.sidebar.subheader("⚙️ Transforms")
    st.sidebar.info(
        f"Raw data has {raw_count:,} rows but mart tables are empty. Run transforms to build the charts.",
        icon="ℹ️",
    )
    if st.sidebar.button("▶️ Run Transforms", type="primary"):
        log_area = st.sidebar.empty()
        messages: list[str] = []

        def on_transform_progress(msg: str):
            messages.append(msg)
            log_area.code("\n".join(messages[-20:]))

        try:
            pipeline.run_transforms(conn, on_transform_progress)
            on_transform_progress("✓ All transforms complete.")
            st.sidebar.success("Transforms complete!")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            on_transform_progress(f"⚠️ Transform error: {e}")
            st.sidebar.error(f"Transform failed: {e}")

    st.sidebar.markdown("---")

# ---------------------------------------------------------------------------
# Sidebar — Station selector
# ---------------------------------------------------------------------------

st.sidebar.subheader("📊 Visualise Stations")

stations_df = load_available_stations()

# Filter to only stations matching the current location selection
if station_ids and not stations_df.empty:
    stations_df = stations_df[stations_df["station_id"].isin(station_ids)]

has_station_data = not stations_df.empty
selected_ids = ()
viz_years = year_range if station_ids else (2020, CURRENT_YEAR)

if has_station_data:
    station_options = dict(zip(stations_df["station_name"], stations_df["station_id"]))
    # Default to stations with temperature data first
    temp_stations = stations_df[stations_df["has_temp"]]["station_name"].tolist()
    default_names = temp_stations[:3] if temp_stations else list(station_options.keys())[:3]
    selected_names = st.sidebar.multiselect(
        "Stations",
        options=list(station_options.keys()),
        default=default_names,
    )

    if selected_names:
        selected_ids = tuple(station_options[n] for n in selected_names)

# ---------------------------------------------------------------------------
# Sidebar — Weather preferences (for Find Your Climate)
# ---------------------------------------------------------------------------

st.sidebar.markdown("---")
st.sidebar.subheader("🌍 Weather Preferences")
st.sidebar.caption("Set your ideal climate to find matching locations.")


def _f_to_c(v):
    return (v - 32) * 5 / 9


_tu = temp_unit()
_heat_thresh = f"{temp(35):.0f}{_tu}" if use_fahrenheit else "35°C"
_cold_thresh = f"{temp(-20):.0f}{_tu}" if use_fahrenheit else "-20°C"

with st.sidebar.expander("Set preferences", expanded=False):
    if use_fahrenheit:
        pref_avg_temp_raw = st.slider(
            f"Avg annual temp ({_tu})",
            -4.0,
            95.0,
            59.0,
            1.0,
            key="pref_avg",
        )
        pref_summer_temp_raw = st.slider(
            f"Avg summer temp ({_tu})",
            14.0,
            104.0,
            77.0,
            1.0,
            key="pref_summer",
        )
        pref_winter_temp_raw = st.slider(
            f"Avg winter temp ({_tu})",
            -22.0,
            77.0,
            41.0,
            1.0,
            key="pref_winter",
        )
        pref_avg_temp = _f_to_c(pref_avg_temp_raw)
        pref_summer_temp = _f_to_c(pref_summer_temp_raw)
        pref_winter_temp = _f_to_c(pref_winter_temp_raw)
    else:
        pref_avg_temp = st.slider(
            f"Avg annual temp ({_tu})",
            -20.0,
            35.0,
            15.0,
            0.5,
            key="pref_avg",
        )
        pref_summer_temp = st.slider(
            f"Avg summer temp ({_tu})",
            -10.0,
            40.0,
            25.0,
            0.5,
            key="pref_summer",
        )
        pref_winter_temp = st.slider(
            f"Avg winter temp ({_tu})",
            -30.0,
            25.0,
            5.0,
            0.5,
            key="pref_winter",
        )
    pref_variability = st.slider(
        "Temp variability (low = steady)",
        0.0,
        15.0,
        5.0,
        0.5,
        key="pref_var",
    )
    pref_precip = st.slider(
        "Annual precipitation (mm)",
        0.0,
        3000.0,
        800.0,
        50.0,
        key="pref_precip",
    )
    pref_snow = st.slider(
        "Annual snowfall (mm)",
        0.0,
        5000.0,
        100.0,
        50.0,
        key="pref_snow",
    )
    pref_heat_days = st.slider(
        f"Extreme heat days/yr (>{_heat_thresh})",
        0.0,
        120.0,
        5.0,
        1.0,
        key="pref_heat",
    )
    pref_cold_days = st.slider(
        f"Extreme cold days/yr (<{_cold_thresh})",
        0.0,
        120.0,
        0.0,
        1.0,
        key="pref_cold",
    )

# Always store prefs in Celsius for similarity computation
user_prefs = {
    "avg_temp_c": pref_avg_temp,
    "avg_summer_temp_c": pref_summer_temp,
    "avg_winter_temp_c": pref_winter_temp,
    "temp_variability": pref_variability,
    "annual_precip_mm": pref_precip,
    "annual_snowfall_mm": pref_snow,
    "heat_days_per_year": pref_heat_days,
    "cold_days_per_year": pref_cold_days,
}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🌨️ Weather Dashboard")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Temperature Trends", "Precipitation & Snow", "Monthly Summary", "Prediction Features", "Find Your Climate"]
)

# ── Tabs 1–4 require ingested data ────────────────────────────────────────

_NO_DATA_MSG = "Download weather data and select stations to view this tab."

if selected_ids:
    daily_df = load_daily(selected_ids, viz_years)
    monthly_df = load_monthly(selected_ids, viz_years)
    feat_df = load_prediction_features(selected_ids, viz_years)
else:
    daily_df = monthly_df = feat_df = pd.DataFrame()

# ── Tab 1 ──────────────────────────────────────────────────────────────────
with tab1:
    if daily_df.empty:
        st.info(_NO_DATA_MSG)
    else:
        st.subheader("Daily Temperature")
        fig = go.Figure()
        for sid, grp in daily_df.groupby("station_id"):
            name = grp["station_name"].iloc[0]
            fig.add_trace(
                go.Scatter(
                    x=grp["obs_date"],
                    y=temp(grp["tmax_celsius"]),
                    name=f"{name} — max",
                    mode="lines",
                    line=dict(width=1),
                    legendgroup=sid,
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=grp["obs_date"],
                    y=temp(grp["tmin_celsius"]),
                    name=f"{name} — min",
                    mode="lines",
                    line=dict(width=1, dash="dot"),
                    legendgroup=sid,
                    fill="tonexty",
                    fillcolor="rgba(100,200,120,0.08)",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=grp["obs_date"],
                    y=temp(grp["tavg_7d_avg"]),
                    name=f"{name} — 7d avg",
                    mode="lines",
                    line=dict(width=2),
                    legendgroup=sid,
                )
            )
        fig.update_layout(xaxis_title="Date", yaxis_title=temp_unit(), hovermode="x unified", height=450)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Temperature Change (day-over-day)")
        plot_df = daily_df.copy()
        change_col = plot_df["temp_change_1d"]
        plot_df["temp_change_display"] = temp(change_col) if use_fahrenheit else change_col
        fig2 = px.line(
            plot_df,
            x="obs_date",
            y="temp_change_display",
            color="station_name",
            height=300,
            labels={"temp_change_display": f"Δ {temp_unit()}", "obs_date": "Date", "station_name": "Station"},
        )
        fig2.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig2, use_container_width=True)

# ── Tab 2 ──────────────────────────────────────────────────────────────────
with tab2:
    if daily_df.empty:
        st.info(_NO_DATA_MSG)
    else:
        st.subheader("Daily Precipitation")
        fig = px.bar(
            daily_df,
            x="obs_date",
            y="precipitation_mm",
            color="station_name",
            barmode="group",
            height=350,
            labels={"precipitation_mm": "Precipitation (mm)", "obs_date": "Date", "station_name": "Station"},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Daily Rainfall")
        daily_rain = daily_df.copy()
        daily_rain["rainfall_mm"] = (
            daily_rain["precipitation_mm"].fillna(0) - daily_rain["snowfall_mm"].fillna(0)
        ).clip(lower=0)
        fig = px.bar(
            daily_rain,
            x="obs_date",
            y="rainfall_mm",
            color="station_name",
            barmode="group",
            height=350,
            labels={"rainfall_mm": "Rainfall (mm)", "obs_date": "Date", "station_name": "Station"},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Daily Snowfall")
        snow_df = daily_df[daily_df["snowfall_mm"] > 0]
        if snow_df.empty:
            st.info("No snowfall recorded in the selected period.")
        else:
            fig = px.bar(
                snow_df,
                x="obs_date",
                y="snowfall_mm",
                color="station_name",
                barmode="group",
                height=350,
                labels={"snowfall_mm": "Snowfall (mm)", "obs_date": "Date", "station_name": "Station"},
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("30-Day Precipitation Total")
        fig = px.line(
            daily_df,
            x="obs_date",
            y="precip_30d_total",
            color="station_name",
            height=300,
            labels={"precip_30d_total": "Cumulative (mm, 30d window)", "obs_date": "Date", "station_name": "Station"},
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 3 ──────────────────────────────────────────────────────────────────
with tab3:
    if monthly_df.empty:
        st.info(_NO_DATA_MSG)
    else:
        st.subheader("Monthly Average Temperature")
        monthly_plot = monthly_df.copy()
        monthly_plot["avg_temp_display"] = temp(monthly_plot["avg_temp_celsius"])
        fig = px.line(
            monthly_plot,
            x="month_label",
            y="avg_temp_display",
            color="station_name",
            markers=True,
            height=380,
            labels={"avg_temp_display": f"Avg Temp ({temp_unit()})", "month_label": "Month", "station_name": "Station"},
        )
        st.plotly_chart(fig, use_container_width=True)

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Monthly Precipitation")
            fig = px.bar(
                monthly_df,
                x="month_label",
                y="total_precip_mm",
                color="station_name",
                barmode="group",
                height=320,
                labels={"total_precip_mm": "Total Precip (mm)", "month_label": "Month", "station_name": "Station"},
            )
            st.plotly_chart(fig, use_container_width=True)
        with col_b:
            st.subheader("Monthly Rainfall")
            rainfall_df = monthly_df.copy()
            rainfall_df["rainfall_mm"] = (
                rainfall_df["total_precip_mm"].fillna(0) - rainfall_df["total_snowfall_mm"].fillna(0)
            ).clip(lower=0)
            fig = px.bar(
                rainfall_df,
                x="month_label",
                y="rainfall_mm",
                color="station_name",
                barmode="group",
                height=320,
                labels={"rainfall_mm": "Rainfall (mm)", "month_label": "Month", "station_name": "Station"},
            )
            st.plotly_chart(fig, use_container_width=True)

        col_c, col_d = st.columns(2)
        with col_c:
            heat_threshold = f"{temp(35):.0f} {temp_unit()}" if use_fahrenheit else "35 °C"
            st.subheader("Extreme Heat Days per Month")
            fig = px.bar(
                monthly_df,
                x="month_label",
                y="extreme_heat_days",
                color="station_name",
                barmode="group",
                height=320,
                labels={
                    "extreme_heat_days": f"Days > {heat_threshold}",
                    "month_label": "Month",
                    "station_name": "Station",
                },
            )
            st.plotly_chart(fig, use_container_width=True)
        with col_d:
            cold_threshold = f"{temp(-20):.0f} {temp_unit()}" if use_fahrenheit else "-20 °C"
            st.subheader("Extreme Cold Days per Month")
            fig = px.bar(
                monthly_df,
                x="month_label",
                y="extreme_cold_days",
                color="station_name",
                barmode="group",
                height=320,
                labels={
                    "extreme_cold_days": f"Days < {cold_threshold}",
                    "month_label": "Month",
                    "station_name": "Station",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Monthly Snowfall")
        fig = px.bar(
            monthly_df,
            x="month_label",
            y="total_snowfall_mm",
            color="station_name",
            barmode="group",
            height=320,
            labels={"total_snowfall_mm": "Snowfall (mm)", "month_label": "Month", "station_name": "Station"},
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 4 ──────────────────────────────────────────────────────────────────
with tab4:
    if feat_df.empty:
        st.info(_NO_DATA_MSG)
    else:
        st.subheader("Next-day Temperature Target vs. Actual")
        st.caption("Once a prediction model is added, predicted values will appear here alongside actuals.")

        fig = go.Figure()
        for sid, grp in feat_df.groupby("station_id"):
            name_rows = daily_df[daily_df["station_id"] == sid]["station_name"]
            name = name_rows.iloc[0] if not name_rows.empty else sid
            fig.add_trace(
                go.Scatter(
                    x=grp["obs_date"],
                    y=temp(grp["tavg_derived_celsius"]),
                    name=f"{name} — actual",
                    mode="lines",
                    line=dict(width=1.5),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=grp["obs_date"],
                    y=temp(grp["target_tavg_next_day"]),
                    name=f"{name} — next-day target",
                    mode="lines",
                    line=dict(width=1, dash="dot"),
                )
            )
        fig.update_layout(xaxis_title="Date", yaxis_title=temp_unit(), hovermode="x unified", height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Lag Features")
        sample = feat_df[feat_df["station_id"] == feat_df["station_id"].iloc[0]].copy()
        lag_plot = sample[["obs_date"]].copy()
        for col in ["tavg_derived_celsius", "tavg_lag_1d", "tavg_lag_7d", "tavg_7d_avg", "tavg_30d_avg"]:
            lag_plot[col] = temp(sample[col])
        fig = px.line(
            lag_plot,
            x="obs_date",
            y=["tavg_derived_celsius", "tavg_lag_1d", "tavg_lag_7d", "tavg_7d_avg", "tavg_30d_avg"],
            height=380,
            labels={"value": temp_unit(), "obs_date": "Date", "variable": "Feature"},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Seasonal Encoding")
        fig = px.line(
            sample,
            x="obs_date",
            y=["season_sin", "season_cos"],
            height=280,
            labels={"value": "Encoding", "obs_date": "Date", "variable": ""},
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 5 — Find Your Climate ─────────────────────────────────────────────
with tab5:
    st.subheader("Find Your Ideal Climate")
    st.caption(
        "Adjust your weather preferences in the sidebar under **Weather Preferences**, "
        "then check results here. Stations are ranked by cosine similarity to your ideal climate."
    )

    profiles = load_climate_profiles()

    if profiles.empty:
        st.info(
            "No station metadata loaded. Download station metadata first using the **Location** section in the sidebar."
        )
    else:
        n_observed = (profiles["data_source"] == "observed").sum()
        n_estimated = (profiles["data_source"] == "estimated").sum()
        st.caption(
            f"Searching {len(profiles):,} stations worldwide "
            f"({n_observed:,} with observed data, {n_estimated:,} with geographic estimates)"
        )

        results = compute_climate_similarity(profiles, user_prefs)
        top_n = _get_diverse_top_results(results, n=20)

        # Summary of current preferences (display in user's chosen unit)
        pref_summary = (
            f"Looking for: **{temp(user_prefs['avg_temp_c']):.0f}{_tu}** avg · "
            f"**{temp(user_prefs['avg_summer_temp_c']):.0f}{_tu}** summer · "
            f"**{temp(user_prefs['avg_winter_temp_c']):.0f}{_tu}** winter · "
            f"**{user_prefs['annual_precip_mm']:.0f}mm** rain · "
            f"**{user_prefs['annual_snowfall_mm']:.0f}mm** snow"
        )
        st.markdown(pref_summary)

        st.markdown("---")
        st.subheader(f"Top {len(top_n)} matching locations")

        # Add country display name
        top_n = top_n.copy()
        top_n["country"] = top_n["country_code"].map(lambda c: pipeline.COUNTRY_NAMES.get(c, c))

        # Results table
        display_df = top_n[
            [
                "station_name",
                "country",
                "climate_zone",
                "data_source",
                "similarity_pct",
                "avg_temp_c",
                "avg_summer_temp_c",
                "avg_winter_temp_c",
                "annual_precip_mm",
                "annual_snowfall_mm",
                "heat_days_per_year",
                "cold_days_per_year",
            ]
        ].copy()
        display_df["avg_temp_c"] = temp(display_df["avg_temp_c"])
        display_df["avg_summer_temp_c"] = temp(display_df["avg_summer_temp_c"])
        display_df["avg_winter_temp_c"] = temp(display_df["avg_winter_temp_c"])
        display_df.columns = [
            "Station",
            "Country",
            "Climate Zone",
            "Source",
            "Match %",
            f"Avg Temp {_tu}",
            f"Summer {_tu}",
            f"Winter {_tu}",
            "Precip mm/yr",
            "Snow mm/yr",
            "Heat Days/yr",
            "Cold Days/yr",
        ]
        for col in display_df.columns:
            if display_df[col].dtype in ["float64", "float32"]:
                display_df[col] = display_df[col].round(1)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Map of top matches
        st.subheader("Top matches on map")
        fig = px.scatter_mapbox(
            top_n,
            lat="latitude",
            lon="longitude",
            hover_name="station_name",
            color="similarity_pct",
            color_continuous_scale="RdYlGn",
            size="similarity_pct",
            size_max=15,
            hover_data={
                "country": True,
                "climate_zone": True,
                "data_source": True,
                "avg_temp_c": ":.1f",
                "annual_precip_mm": ":.0f",
                "similarity_pct": ":.1f",
                "latitude": False,
                "longitude": False,
            },
            zoom=1,
            height=500,
            mapbox_style="open-street-map",
        )
        fig.update_layout(coloraxis_colorbar_title="Match %")
        st.plotly_chart(fig, use_container_width=True)

        # Radar chart comparing user prefs vs top 3 matches
        st.subheader("Your preferences vs. top matches")
        radar_labels = [
            "Avg Temp",
            "Summer Temp",
            "Winter Temp",
            "Variability",
            "Precipitation",
            "Snowfall",
            "Heat Days",
            "Cold Days",
        ]

        # Normalize all values to 0-1 for radar
        all_vals = profiles[CLIMATE_FEATURES].fillna(0)
        mins = all_vals.min()
        maxs = all_vals.max()
        ranges = maxs - mins
        ranges = ranges.replace(0, 1)

        fig = go.Figure()

        # User preference trace
        user_norm = [(user_prefs[c] - mins[c]) / ranges[c] for c in CLIMATE_FEATURES]
        fig.add_trace(
            go.Scatterpolar(
                r=user_norm + [user_norm[0]],
                theta=radar_labels + [radar_labels[0]],
                name="Your preference",
                line=dict(width=3, color="black", dash="dash"),
                fill="none",
            )
        )

        colors = px.colors.qualitative.Set2
        for i, (_, row) in enumerate(top_n.head(3).iterrows()):
            station_norm = [(row[c] - mins[c]) / ranges[c] for c in CLIMATE_FEATURES]
            fig.add_trace(
                go.Scatterpolar(
                    r=station_norm + [station_norm[0]],
                    theta=radar_labels + [radar_labels[0]],
                    name=f"{row['station_name']} ({row['similarity_pct']}%)",
                    line=dict(width=2, color=colors[i % len(colors)]),
                    fill="toself",
                    opacity=0.3,
                )
            )

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            height=500,
            showlegend=True,
        )
        st.plotly_chart(fig, use_container_width=True)
