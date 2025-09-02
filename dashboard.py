import streamlit as st
import pandas as pd
import psycopg2
import time

# ------------------------------
# Postgres connection
# ------------------------------
conn = psycopg2.connect(
    dbname="mydb",
    user="admin",
    password="secret",
    host="localhost",  # use 'localhost' if running locally, 'postgres' if in Docker
    port=5432
)

st.set_page_config(page_title="Watchers Dashboard", layout="wide")
st.title("Real-Time Watchers")

# ------------------------------
# Select movies to display
# ------------------------------
movies_to_display = st.multiselect(
    "Select movies to monitor",
    options=[
        "NFL Game",
        "UFC Fight",
        "Taylor Swift Concert"
    ],
    default=[
        "NFL Game",
        "UFC Fight",
        "Taylor Swift Concert"
    ]
)

# ------------------------------
# Live chart update
# ------------------------------
st.subheader("Total Viewers")
chart_placeholder = st.empty()

st.subheader("Viewership Gain/Loss")
total_placeholder = st.empty()

st.subheader("10 Second Moving Average")
placeholder_3 = st.empty()

refresh_interval = st.slider("Refresh interval (seconds)", 1, 5, 1)

while True:
    if not movies_to_display:
        st.warning("Select at least one movie to display.")
        time.sleep(refresh_interval)
        continue

    # Query latest data from Postgres
    query = f"""
        SELECT window_start, movie_title, watchers
        FROM movie_counts_time_series
        WHERE movie_title = ANY(%s)
        ORDER BY window_start ASC
    """
    df = pd.read_sql(query, conn, params=(movies_to_display,))

    # Query total data from Postgres
    query_cumulative = f"""
        SELECT window_start
            , movie_title
            , coalesce(watchers
            - lag(watchers) over (
                partition by movie_title
                order by window_start), 0) watchers
        FROM movie_counts_time_series
        WHERE movie_title = ANY(%s)
        ORDER BY window_start ASC
    """
    df_cumulative = pd.read_sql(
        query_cumulative, conn, params=(movies_to_display,))

    # Query total data from Postgres
    query_3 = f"""
        SELECT window_start
            , movie_title
            , cast(avg(watchers) over (
                partition by movie_title
                order by window_start
                rows between 10 preceding and current row) as int) watchers
        FROM movie_counts_time_series
        WHERE movie_title = ANY(%s)
        ORDER BY window_start ASC
    """
    df_3 = pd.read_sql(query_3, conn, params=(movies_to_display,))

    if not df.empty:
        # Pivot for Streamlit line chart
        chart_data = df.pivot(index="window_start",
                              columns="movie_title", values="watchers")
        chart_placeholder.line_chart(chart_data)
        # Pivot for Streamlit line chart
        chart_data_total = df_cumulative.pivot(index="window_start",
                                               columns="movie_title", values="watchers")
        total_placeholder.line_chart(chart_data_total)
        # Pivot for Streamlit line chart
        chart_data_3 = df_3.pivot(index="window_start",
                                  columns="movie_title", values="watchers")
        placeholder_3.line_chart(chart_data_3)

    else:
        chart_placeholder.write("No data yet...")
        total_placeholder.write("No data yet...")
        placeholder_3.write("No data yet...")

    time.sleep(refresh_interval)
