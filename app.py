import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

@st.cache_data
def get_data():
    """Fetch weather data from Redshift"""
    con = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST").split(":")[0],
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        port=int(os.getenv("REDSHIFT_PORT", 5439))
    )
    query = "SELECT city_name, temperature, humidity_percent, date FROM weather_data LIMIT 1000;"
    cur = con.cursor()
    cur.execute(query)
    colnames = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=colnames)
    con.close()
    return df

df = get_data()

st.title("🌦 Weather Data Dashboard")

st.sidebar.header("🔍 Filters")
available_dates = sorted(df['date'].unique())
selected_date = st.sidebar.selectbox("Select Date", available_dates)

filtered_df = df[df['date'] == selected_date]

cities = st.sidebar.multiselect("Select Cities", filtered_df['city_name'].unique())
if cities:
    filtered_df = filtered_df[filtered_df['city_name'].isin(cities)]

st.markdown(f"### 📅 Weather Summary for **{selected_date}**")

col1, col2 = st.columns(2)
col1.metric("Average Temperature (°C)", round(filtered_df['temperature'].mean(), 2))
col2.metric("Average Humidity (%)", round(filtered_df['humidity_percent'].mean(), 2))

st.markdown("---")

if not filtered_df.empty:
    col1, col2 = st.columns(2)

    with col1:
        fig1 = px.bar(filtered_df, x='city_name', y='temperature',
                      title="Temperature by City", color='city_name')
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        fig2 = px.bar(filtered_df, x='city_name', y='humidity_percent',
                      title="Humidity by City", color='city_name')
        st.plotly_chart(fig2, use_container_width=True)
else:
    st.warning("No data available for this selection.")

st.markdown("### 🧾 Data Preview")
 
