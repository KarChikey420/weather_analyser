from fastapi import FastAPI, HTTPException, Request,logger
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
        logger.info("Data extraction completed.")
        df_raw = transform_raw(raw_data)
        logger.info("Raw data transformation completed.")
        df_clean = transform(df_raw)
        logger.info("Data cleaning transformation completed.")
        
        s3_key = upload_df_to_s3(
            df=df_clean,
            bucket_name=Bucket_Name
        )
        logger.info("Data uploaded to S3.")
        
        load_data_to_redshift(
            table_name=request.table_name,
            s3_bucket=Bucket_Name,
            s3_key=s3_key,
            iam_role_arn=IAM_ROLE_ARN,
            host=REDSHIFT_HOST,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            port=REDSHIFT_PORT,
            region=REGION
        )
        logger.info("Data loaded to Redshift.")
        
        return {
            "status": "success",
            "message": "Weather ETL completed successfully",
            "rows_loaded": len(df_clean),
            "s3_path": f"s3://{Bucket_Name}/{s3_key}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    
        
