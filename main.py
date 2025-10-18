from Extract_api import Extract_Api
from transform1 import transform,transform_raw
from load import load_data_to_s3
from datetime import datetime
import os

API_KEY=os.getenv("Weather_Api")
Bucket_Name=os.getenv("AWS_BUCKET")

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
   load_data_to_s3(file_path, Bucket_Name)

   print("ETL pipeline completed successfully!")

if __name__=="__main__":
    main()