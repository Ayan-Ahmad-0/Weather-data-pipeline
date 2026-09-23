from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import pydeck as pdk
import streamlit as st
st.set_page_config(page_title="Weather Pipeline", layout="wide", page_icon="⛅")

st.title("⛅ Weather Pipeline")
st.caption("Open-Meteo → Postgres → dbt → Streamlit · 10 cities · live + historical")

# ---------------------------------------------------------------------------
# Styling — dark cards, accent color, tighter spacing than Streamlit defaults
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #232838 100%);
        border: 1px solid #2d3348;
        border-radius: 12px;
        padding: 18px 20px;
        text-align: center;
    }
    .metric-card .label { color: #8b93a7; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.05em; }
    .metric-card .value { color: #f0f2f6; font-size: 1.9rem; font-weight: 700; margin-top: 4px; }
    .metric-card .sub   { color: #5f6c85; font-size: 0.78rem; margin-top: 2px; }
    .city-card {
        background: #171b26;
        border: 1px solid #2d3348;
        border-radius: 12px;
        padding: 14px 16px;
        margin-bottom: 10px;
    }
    .city-card .name { color: #f0f2f6; font-size: 1.05rem; font-weight: 600; }
    .city-card .temp { color: #f0f2f6; font-size: 1.6rem; font-weight: 700; }
    .city-card .cond { color: #8b93a7; font-size: 0.85rem; }
    h1, h2, h3 { color: #f0f2f6 !important; }
</style>
""", unsafe_allow_html=True)

WEATHER_CODES = {
    0: ("Clear sky", "☀️"), 1: ("Mainly clear", "🌤️"), 2: ("Partly cloudy", "⛅"),
    3: ("Overcast", "☁️"), 45: ("Fog", "🌫️"), 48: ("Rime fog", "🌫️"),
    51: ("Light drizzle", "🌦️"), 53: ("Drizzle", "🌦️"), 55: ("Dense drizzle", "🌧️"),
    61: ("Slight rain", "🌧️"), 63: ("Rain", "🌧️"), 65: ("Heavy rain", "🌧️"),
    71: ("Slight snow", "❄️"), 73: ("Snow", "❄️"), 75: ("Heavy snow", "❄️"),
    80: ("Rain showers", "🌦️"), 81: ("Rain showers", "🌧️"), 82: ("Violent showers", "⛈️"),
    85: ("Snow showers", "🌨️"), 86: ("Heavy snow showers", "🌨️"),
    95: ("Thunderstorm", "⛈️"), 96: ("Thunderstorm, hail", "⛈️"),
    99: ("Thunderstorm, heavy hail", "⛈️"),
}


def describe_weather(code):
    return WEATHER_CODES.get(int(code) if pd.notnull(code) else -1, ("Unknown", "❓"))


def get_connection():
    return psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"],
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        connect_timeout=5,
        options="-c lock_timeout=5000 -c statement_timeout=30000",
    )


@st.cache_data(ttl=300)
def load_latest():
    query = """
        select distinct on (f.city_name)
            f.city_name, f.observed_at, f.temperature_c, f.windspeed_kmh,
            f.weather_code, f.is_day, d.latitude, d.longitude, d.country
        from staging.fct_weather_readings f
        join staging.dim_cities d on d.city_name = f.city_name
        where f.data_source = 'open-meteo'
        order by f.city_name, f.observed_at desc
    """
    connection = get_connection()
    try:
        return pd.read_sql(query, connection)
    finally:
        connection.close()


@st.cache_data(ttl=300)
def load_history_all():
    query = """
        select city_name, observed_at, temperature_c, windspeed_kmh
        from staging.fct_weather_readings
        order by observed_at
    """
    connection = get_connection()
    try:
        return pd.read_sql(query, connection)
    finally:
        connection.close()


@st.cache_data(ttl=600)
def load_pipeline_stats():
    query = """
        select
            count(*) as total_rows,
            count(*) filter (where source = 'open-meteo-archive') as backfilled_rows,
            max(fetched_at) filter (where source = 'open-meteo') as last_run,
            count(distinct city_name) as city_count
        from raw.weather_readings
    """
    connection = get_connection()
    try:
        return pd.read_sql(query, connection).iloc[0]
    finally:
        connection.close()

try:
    latest = load_latest()
    history = load_history_all()
    stats = load_pipeline_stats()
except (psycopg2.Error, pd.errors.DatabaseError) as exc:
    st.error("Unable to load dashboard data from PostgreSQL.")
    st.exception(exc)
    st.stop()

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
hottest = latest.loc[latest["temperature_c"].idxmax()]
coldest = latest.loc[latest["temperature_c"].idxmin()]
windiest = latest.loc[latest["windspeed_kmh"].idxmax()]
avg_temp = latest["temperature_c"].mean()

if pd.isna(stats["last_run"]):
    minutes_ago = None
else:
    last_run = pd.to_datetime(stats["last_run"])
    if last_run.tzinfo is None:
        last_run = last_run.tz_localize("UTC")
    minutes_ago = int((datetime.now(timezone.utc) - last_run).total_seconds() // 60) 

kpis = [
    ("Avg temp (10 cities)", f"{avg_temp:.1f}°C", ""),
    ("Hottest", f"{hottest['temperature_c']:.1f}°C", hottest["city_name"]),
    ("Coldest", f"{coldest['temperature_c']:.1f}°C", coldest["city_name"]),
    ("Windiest", f"{windiest['windspeed_kmh']:.0f} km/h", windiest["city_name"]),
    ("Last pipeline run",
     f"{minutes_ago}m ago" if minutes_ago is not None else "No runs yet",
     f"{int(stats['total_rows']):,} rows total"),
]

cols = st.columns(len(kpis))
for col, (label, value, sub) in zip(cols, kpis):
    col.markdown(f"""
        <div class="metric-card">
            <div class="label">{label}</div>
            <div class="value">{value}</div>
            <div class="sub">{sub}</div>
        </div>
    """, unsafe_allow_html=True)

st.write("")

# ---------------------------------------------------------------------------
# Map + city cards
# ---------------------------------------------------------------------------
map_col, cards_col = st.columns([2, 1])

with map_col:
    st.subheader("Current temperature by city")

    map_df = latest.copy()
    t_min, t_max = map_df["temperature_c"].min(), map_df["temperature_c"].max()
    t_range = max(t_max - t_min, 0.01)

    def temp_color(t):
        ratio = (t - t_min) / t_range
        r = int(70 + ratio * 185)
        g = int(130 - ratio * 80)
        b = int(230 - ratio * 200)
        return [r, g, b, 210]

    map_df["color"] = map_df["temperature_c"].apply(temp_color)
    map_df["radius"] = 60000 + (map_df["temperature_c"] - t_min) / t_range * 90000
    map_df["label"] = map_df["city_name"] + "  " + map_df["temperature_c"].round(1).astype(str) + "°C"

    dot_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position=["longitude", "latitude"],
        get_radius="radius",
        get_fill_color="color",
        get_line_color=[255, 255, 255, 160],
        line_width_min_pixels=1.5,
        stroked=True,
        pickable=True,
    )
    text_layer = pdk.Layer(
        "TextLayer",
        data=map_df,
        get_position=["longitude", "latitude"],
        get_text="label",
        get_size=13,
        get_color=[230, 233, 240, 255],
        get_pixel_offset=[0, -22],
        font_family="Helvetica, Arial, sans-serif",
    )

    view_state = pdk.ViewState(latitude=15, longitude=20, zoom=0.9, pitch=0)
    st.pydeck_chart(pdk.Deck(
        layers=[dot_layer, text_layer],
        initial_view_state=view_state,
        map_provider="carto",
        map_style="dark",
        tooltip={"text": "{city_name}\n{temperature_c}°C"},
    ))

    st.subheader("Temperature trend — compare cities")
    default_cities = list(latest.sort_values("temperature_c", ascending=False)["city_name"].head(3))
    selected = st.multiselect(
        "Cities", options=sorted(latest["city_name"].unique()),
        default=default_cities, key="trend_cities",
    )

    if selected:
        trend_data = history.copy()
        trend_data["observed_at"] = pd.to_datetime(
            trend_data["observed_at"], utc=True, errors="coerce"
        )
        trend_data["temperature_c"] = pd.to_numeric(
            trend_data["temperature_c"], errors="coerce"
        )
        trend_data = trend_data.dropna(
            subset=["city_name", "observed_at", "temperature_c"]
        ).sort_values(["city_name", "observed_at"])

        fig = go.Figure()
        for city in selected:
            city_hist = trend_data[trend_data["city_name"] == city]
            if city_hist.empty:
                continue

            trend_x = []
            trend_y = []
            previous_time = None
            for row in city_hist.itertuples(index=False):
                if (
                    previous_time is not None
                    and row.observed_at - previous_time > pd.Timedelta(hours=3)
                ):
                    trend_x.append(None)
                    trend_y.append(None)
                trend_x.append(row.observed_at)
                trend_y.append(row.temperature_c)
                previous_time = row.observed_at

            fig.add_trace(go.Scatter(
                x=trend_x, y=trend_y,
                mode="lines+markers", name=city, line=dict(width=2),
                marker=dict(size=5), connectgaps=False,
            ))
        if fig.data:
            fig.update_layout(
                template="plotly_dark", height=600,
                paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                legend=dict(orientation="h", y=1.15),
                xaxis_title="Observed at", yaxis_title="Temperature (°C)",
                hovermode="x unified",
                xaxis=dict(rangeslider=dict(visible=True), type="date"),
                margin=dict(l=20, r=20, t=20, b=20),
            )
            st.plotly_chart(
                fig,
                use_container_width=True,
                config={
                    "displayModeBar": True,
                    "displaylogo": False,
                    "scrollZoom": True,
                },
            )
        else:
            st.info("No temperature history is available for the selected cities.")

with cards_col:
    st.subheader("Conditions")
    for _, row in latest.sort_values("temperature_c", ascending=False).iterrows():
        desc, icon = describe_weather(row["weather_code"])
        day_night = "☀️ Day" if row["is_day"] else "🌙 Night"
        st.markdown(f"""
            <div class="city-card">
                <div class="name">{icon} {row['city_name']}, {row['country']}</div>
                <div class="temp">{row['temperature_c']:.1f}°C</div>
                <div class="cond">{desc} · {row['windspeed_kmh']:.0f} km/h · {day_night}</div>
            </div>
        """, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Wind comparison (bar)
# ---------------------------------------------------------------------------
st.subheader("Current wind speed by city")

wind_sorted = latest.sort_values("windspeed_kmh", ascending=False)

def wind_color(speed, s_min, s_max):
    ratio = (speed - s_min) / max(s_max - s_min, 0.01)
    r = int(70 + ratio * 185)   # 70   -> 255
    g = int(130 - ratio * 80)   # 130  -> 50
    b = int(230 - ratio * 200)  # 230  -> 30
    return f"rgb({r},{g},{b})"

s_min, s_max = wind_sorted["windspeed_kmh"].min(), wind_sorted["windspeed_kmh"].max()
bar_colors = [wind_color(v, s_min, s_max) for v in wind_sorted["windspeed_kmh"]]

wind_fig = go.Figure(go.Bar(
    x=wind_sorted["city_name"],
    y=wind_sorted["windspeed_kmh"],
    marker_color=bar_colors,
))
wind_fig.update_layout(
    template="plotly_dark", height=500,
    paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
    yaxis_title="km/h", margin=dict(l=20, r=20, t=20, b=20),
)
st.plotly_chart(wind_fig, use_container_width=True)