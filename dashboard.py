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

st.set_page_config(page_title="Movie Watchers Dashboard", layout="wide")
st.title("Real-Time Movie Watchers")

# ------------------------------
# Select movies to display
# ------------------------------
movies_to_display = st.multiselect(
    "Select movies to monitor",
    options=[
        "Inception",
        "The Dark Knight",
        "Interstellar",
        "Spirited Away",
        "American Beauty",
        "Spider-Man: No Way Home",
        "The Good, the Bad and the Ugly"
    ],
    default=[
        "Inception",
        "Spirited Away"
    ]
)

# ------------------------------
# Live chart update
# ------------------------------
chart_placeholder = st.empty()
total_placeholder = st.empty()  # For total watchers

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
        LIMIT 500
    """
    df = pd.read_sql(query, conn, params=(movies_to_display,))

    if not df.empty:
        # Pivot for Streamlit line chart
        chart_data = df.pivot(index="window_start",
                              columns="movie_title", values="watchers")
        chart_placeholder.line_chart(chart_data)

        # Show total watchers across all selected movies (latest window)
        latest_window = df[df['window_start'] == df['window_start'].max()]
        total_watchers = latest_window['watchers'].sum()
        total_placeholder.metric(
            "Total Watchers (selected movies)", total_watchers)
    else:
        chart_placeholder.write("No data yet...")
        total_placeholder.write("Total Watchers (selected movies): 0")

    time.sleep(refresh_interval)
