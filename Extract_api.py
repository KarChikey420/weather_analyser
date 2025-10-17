from time import sleep
import requests
import json
import os
from dotenv import load_dotenv
from urllib3 import response
import pandas as pd

load_dotenv()


API_KEY=os.getenv("Weather_Api")
city_name=["Delhi", "Mumbai", "Dehradun", "Lucknow", "Chandigarh"]

def Extract_Api(city_name,API_KEY):
    all_data={}
    BASE_URL="https://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={API_KEY}"

    for city in city_name:
        url=BASE_URL.format(city_name=city,API_KEY=API_KEY)
        response=requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            cleaned_list = []

            for entry in data.get("list", []):
                cleaned_list.append({
                    "datetime": entry["dt_txt"],
                    "temperature": entry["main"]["temp"],
                    "humidity": entry["main"]["humidity"],
                    "weather": entry["weather"][0]["main"],
                    "description": entry["weather"][0]["description"],
                    "wind_speed": entry["wind"]["speed"]
                })

            all_data[city] = {
                "city_name": data["city"]["name"],
                "country": data["city"]["country"],
                "forecast": cleaned_list
            }

        else:
            print(f"Error for {city}: {response.status_code}")
            all_data[city] = None

        sleep(1)

    return all_data

# weather_data = Extract_Api(city_name, API_KEY)

# with open("weather_data.json", "w") as json_file:
#     json.dump(weather_data, json_file, indent=2)

def transform(all_data):
    data=[]
    for city ,city_data in all_data.items():
        if city is not None:
            for forecast in city_data['forecast']:
                data.append({
                    'city':city_data['city_name'],
                    'country':city_data['country'],
                    'datetime': forecast['datetime'],
                    'temperature': forecast['temperature'],
                    'humidity': forecast['humidity'],
                    'weather': forecast['weather'],
                    'description': forecast['description'],
                    'wind_speed': forecast['wind_speed']
                })
    df=pd.DataFrame(data)
    return df

print("Extrating data from the API")
weather_data=Extract_Api(city_name,API_KEY)

print("Transforming the data")
df=transform(weather_data)

df.to_csv("weather_data.csv",index=False)