from Extract_api import Extract_Api
from transform1 import transform,transform_raw
import os

API_KEY=os.getenv("Weather_Api")

def main():
   city_name=["Delhi", "Mumbai", "Dehradun", "Lucknow", "Chandigarh"]
   
   print("Starting data extraction")
   all_data=Extract_Api(city_name,API_KEY)
   
   print("transforming raw data")
   df=transform_raw(all_data)
   
   print("transforming data")
   df1=transform(df)
   
   folder_path = "data"
   if os.path.exists(folder_path):
        os.system(f'rmdir /S /Q {folder_path}')  
   os.makedirs(folder_path)                      

   df1.to_csv(os.path.join(folder_path, "transformed_weather.csv"), index=False)
   print(f"File saved at {folder_path}/transformed_weather.csv")
   
   
   df1.to_csv("data/transformed_weather.csv",index=False)
   
if __name__=="__main__":
    main()