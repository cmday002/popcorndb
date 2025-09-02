import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# Postgres connection
conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="secret",
    host="localhost",
    port=5432
)

st.set_page_config(page_title="Movie Watchers Dashboard", layout="wide")
st.title("Real-Time Movie Watchers Dashboard")

# Select movies/events
movies_to_display = st.multiselect(
    "Select movies/events to monitor",
    options=["UFC Fight", "NFL Game", "Taylor Swift Concert"],
    default=["UFC Fight", "NFL Game", "Taylor Swift Concert"]
)

# Refresh interval
refresh_interval = st.slider(
    "Refresh interval (milliseconds)", 100, 5000, 1000)

# Auto-refresh the page
st_autorefresh(interval=refresh_interval, limit=None, key="auto_refresh")

# Side-by-side columns
col1, col2, col3 = st.columns([3, 1, 2])

# --- Line chart ---
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
        chart_data = df_line.pivot(
            index="window_start", columns="movie_title", values="watchers")
        col1.line_chart(chart_data)

        latest_window = df_line[df_line['window_start']
                                == df_line['window_start'].max()]
        total_watchers = latest_window['watchers'].sum()
        col2.metric("Total Watchers (selected events)", total_watchers)
    else:
        col1.write("No data yet for selected events.")
        col2.write("Total Watchers: 0")
else:
    st.warning("Select at least one movie/event to display.")

# --- Pie chart ---
query_pie = "SELECT movie_title, watchers FROM movie_counts"
df_pie = pd.read_sql(query_pie, conn)

if not df_pie.empty:
    fig = px.pie(df_pie, names='movie_title', values='watchers',
                 title="Watchers Share per Event")
    col3.plotly_chart(fig)  # No key needed
else:
    col3.write("No data available for pie chart yet.")
