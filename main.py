from prefect import flow, task
from Extract_api import Extract_Api
from transform1 import transform, transform_raw
from load import load_data_to_s3
from s3_to_redsift import load_data_to_redshift
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()  

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

@task
def extract_task(city_names):
    print("🌤 Starting data extraction")
    return Extract_Api(city_names, API_KEY)

@task
def transform_task(all_data):
    print("🔄 Transforming data")
    df = transform_raw(all_data)
    return transform(df)

@task
def save_and_upload(df):
    print("💾 Saving file locally and uploading to S3")
    current_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = "data"
    os.makedirs(folder_path, exist_ok=True)
    file_name = f"{current_date}_weather_data.csv"
    file_path = os.path.join(folder_path, file_name)
    df.to_csv(file_path, index=False)
    print(f"✅ File saved: {file_path}")
    s3_key = load_data_to_s3(file_path, Bucket_Name)
    return s3_key

@task
def load_to_redshift(s3_key):
    if s3_key:
        print("🛢 Loading data into Redshift...")
        load_data_to_redshift(
            table_name=TABLE_NAME,
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
    print("✅ ETL pipeline completed successfully!")

@flow(name="Weather_ETL_Pipeline")
def weather_etl_flow():
    city_names = ["Delhi", "Mumbai", "Dehradun", "Lucknow", "Chandigarh"]
    all_data = extract_task(city_names)
    df = transform_task(all_data)
    s3_key = save_and_upload(df)
    load_to_redshift(s3_key)

if __name__ == "__main__":
    weather_etl_flow()
