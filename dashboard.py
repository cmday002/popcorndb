# dashboard_real_time_synced_plotly.py
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# ------------------------------
# Postgres connection
# ------------------------------
conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="secret",
    host="localhost",  # 'postgres' if in Docker
    port=5432
)

st.set_page_config(page_title="Movie Watchers Dashboard", layout="wide")
st.title("Real-Time Movie Watchers Dashboard")

# ------------------------------
# User selections
# ------------------------------
movies_to_display = st.multiselect(
    "Select movies/events to monitor",
    options=["UFC Fight", "NFL Game", "Taylor Swift Concert"],
    default=["UFC Fight", "NFL Game", "Taylor Swift Concert"]
)

# ------------------------------
# Refresh interval
# ------------------------------
refresh_interval = st.slider(
    "Refresh interval (milliseconds)", 100, 5000, 1000)

# ------------------------------
# Auto-refresh page
# ------------------------------
st_autorefresh(interval=refresh_interval, limit=None, key="auto_refresh")

# ------------------------------
# Color map for consistent colors
# ------------------------------
color_map = {
    "UFC Fight": "#636EFA",           # blue
    "NFL Game": "#EF553B",            # red
    "Taylor Swift Concert": "#00CC96"  # green
}

# ------------------------------
# Side-by-side layout
# ------------------------------
col1, col2 = st.columns([3, 2])  # line chart 60%, pie chart 40%

# ------------------------------
# Line chart with Plotly (explicit colors)
# ------------------------------
if movies_to_display:
    query_line = """
        SELECT window_start, movie_title, watchers
        FROM movie_counts_time_series
        WHERE movie_title = ANY(%s)
        ORDER BY window_start ASC
        LIMIT 500
    """
    df_line = pd.read_sql(query_line, conn, params=(movies_to_display,))
    if not df_line.empty:
        fig_line = px.line(
            df_line,
            x='window_start',
            y='watchers',
            color='movie_title',
            color_discrete_map=color_map,
            title='Watchers Over Time'
        )
        col1.plotly_chart(fig_line)
    else:
        col1.write("No data yet for selected events.")
else:
    st.warning("Select at least one movie/event to display.")

# ------------------------------
# Pie chart with synced colors
# ------------------------------
df_pie = pd.read_sql("SELECT movie_title, watchers FROM movie_counts", conn)
if not df_pie.empty:
    # Filter to selected events to match line chart
    if movies_to_display:
        df_pie = df_pie[df_pie['movie_title'].isin(movies_to_display)]
    fig_pie = px.pie(
        df_pie,
        names='movie_title',
        values='watchers',
        title='Watchers Share per Event',
        color='movie_title',
        color_discrete_map=color_map
    )
    col2.plotly_chart(fig_pie)
else:
    col2.write("No data available for pie chart yet.")
