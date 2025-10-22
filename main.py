from Extract_api import Extract_Api
from transform1 import transform,transform_raw
from load import load_data_to_s3
from s3_to_redsift import load_data_to_redshift
from datetime import datetime
import os

API_KEY=os.getenv("Weather_Api")
Bucket_Name=os.getenv("AWS_BUCKET")
REDSHIFT_HOST=os.getenv("REDSHIFT_HOST").split(":")[0]
REDSHIFT_DB=os.getenv("REDSHIFT_DB")
REDSHIFT_USER=os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD=os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_PORT=int(os.getenv("REDSHIFT_PORT",5439))
IAM_ROLE_ARN=os.getenv("IAM_ROLE_ARN")
TABLE_NAME=os.getenv("TABLE_NAME")
REGION=os.getenv("REGION")

def main():
   city_name=["Delhi", "Mumbai", "Dehradun", "Lucknow", "Chandigarh"]
   
   print("Starting data extraction")
   all_data=Extract_Api(city_name,API_KEY)
   
   print("transforming raw data")
   df=transform_raw(all_data)
   
   print("transforming data")
   df1=transform(df)
   
   current_date = datetime.now().strftime("%Y-%m-%d")
   folder_path = "data"
   os.makedirs(folder_path, exist_ok=True)
    
   file_name = f"{current_date}_weather_data.csv"
   file_path = os.path.join(folder_path, file_name)

   df1.to_csv(file_path, index=False)
   print(f"File saved locally at: {file_path}")

   print("Uploading to S3...")
   s3_key=load_data_to_s3(file_path, Bucket_Name)
   print("s3 key:",s3_key)
   
   if s3_key:
       print("Loading data into Redshift...")
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

   print("ETL pipeline completed successfully!")

if __name__=="__main__":
    main()