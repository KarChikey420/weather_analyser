import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

@st.cache_data
def get_data():
    con = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST").split(":")[0],
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        port=int(os.getenv("REDSHIFT_PORT", 5439))
    )
    
    query = "SELECT city_name, temperature, humidity_percent, date FROM weather_data LIMIT 500;"
    cur = con.cursor()
    cur.execute(query)
    colnames = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=colnames)
    con.close()
    
    return df

# Load data
df = get_data()

# Debug columns (optional)
st.write("Columns in data:", list(df.columns))

st.title("🌦 Weather Data Dashboard")

# City selection
filtered_df = df.copy()
cities = st.multiselect("Select Cities", df['city_name'].unique())
if cities:
    filtered_df = df[df['city_name'].isin(cities)]

col1, col2 = st.columns(2)

with col1:
    st.subheader("🌡️ Temperature over Time")
    fig1 = px.line(filtered_df, x='date', y='temperature', color='city_name')
    st.plotly_chart(fig1, use_container_width=True)
    
with col2:
    st.subheader("💧 Humidity over Time")
    fig2 = px.line(filtered_df, x='date', y='humidity_percent', color='city_name')
    st.plotly_chart(fig2, use_container_width=True)
    
st.dataframe(filtered_df)
