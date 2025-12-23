from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
import os
from Extract_api import Extract_Api
from transform1 import transform, transform_raw
from load import upload_df_to_s3
from s3_to_redsift import load_data_to_redshift

app=FastAPI("Weather ETL API")

load_dotenv()

class WheatherRequest(BaseModel):
    city_names: list[str]
    table_name:str
    
@app.post("/etl")
def etl_process(request: WheatherRequest):
    try:
        API_KEY = os.getenv("Weather_Api")
        Bucket_Name = os.getenv("AWS_BUCKET")
        REDSHIFT_HOST = os.getenv("REDSHIFT_HOST").split(":")[0]
        REDSHIFT_DB = os.getenv("REDSHIFT_DB")
        REDSHIFT_USER = os.getenv("REDSHIFT_USER")
        REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
        REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", 5439))
        IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN")
        TABLE_NAME = os.getenv("TABLE_NAME")
        REGION = os.getenv("AWS_REGION")
        
        if not all([API_KEY, Bucket_Name, REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD, IAM_ROLE_ARN, TABLE_NAME, REGION]):
            return {"status": "error", "message": "One or more environment variables are missing."}
        
        raw_data= Extract_Api(request.city_names, API_KEY)
        df_raw = transform_raw(raw_data)
        df_clean = transform(df_raw)
    
    except Exception as e:
        return {"status": "error", "message": f"Environment variable error: {e}"}
        
