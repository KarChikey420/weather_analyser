import boto3
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def load_data_to_s3(file_path, bucket_name, s3_key=None):
    current_date = datetime.now().strftime("%Y-%m-%d")
    if not s3_key:
       s3_key = f"transformed/{current_date}_weather_data.csv"
    
    s3=boto3.client(
        "s3",
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        region_name=os.getenv("AWS_REGION")   
    )
    try:
        s3.upload_file(file_path,bucket_name,s3_key)
        print(f"File {file_path} uploaded to s3://{bucket_name}/{s3_key}")
        return s3_key
    except Exception as e:
        print(f"error{e}")
        