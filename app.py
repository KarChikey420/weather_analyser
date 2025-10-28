import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

@st.cache_data
def get_data():
    con=psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST").split(":")[0],
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        port=int(os.getenv("REDSHIFT_PORT",5439))
    )
    query="SELECT City_Name,Temperature,Humidity_Percent,Date from weather_data LIMIT 500;"
    df=pd.read_sql(query,con)   
    con.close()
    return df

 

  