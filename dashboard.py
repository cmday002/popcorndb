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
    host="localhost",  # use 'postgres' if running inside Docker
    port=5432
)

st.set_page_config(page_title="Event Dashboard", layout="wide")
st.title("Real-Time Event Watchers Dashboard")

# ------------------------------
# Select movies/events to monitor
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
# Auto-refresh Streamlit every `refresh_interval` milliseconds
# ------------------------------
st_autorefresh(interval=refresh_interval, limit=None, key="auto_refresh")

# ------------------------------
# Time series line chart
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
        # Pivot for line chart
        chart_data = df_line.pivot(
            index="window_start",
            columns="movie_title",
            values="watchers"
        )
        st.line_chart(chart_data)

        # Show total watchers for selected events
        latest_window = df_line[df_line['window_start']
                                == df_line['window_start'].max()]
        total_watchers = latest_window['watchers'].sum()
        st.metric("Total Watchers (selected events)", total_watchers)
    else:
        st.write("No data yet for selected events.")
else:
    st.warning("Select at least one movie/event to display.")

# ------------------------------
# Pie chart for all events
# ------------------------------
query_pie = "SELECT movie_title, watchers FROM movie_counts"
df_pie = pd.read_sql(query_pie, conn)

if not df_pie.empty:
    fig = px.pie(df_pie, names='movie_title', values='watchers',
                 title='Watchers Share per Event')
    st.plotly_chart(fig)  # No key needed
else:
    st.write("No data available for pie chart yet.")
