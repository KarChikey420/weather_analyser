from time import sleep
import requests
import json
import os
from dotenv import load_dotenv
from urllib3 import response

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

