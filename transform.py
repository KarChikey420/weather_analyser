import pandas as pd

df=pd.read_csv("weather_data.csv")

def transform(df):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.date
    df['time'] = df['datetime'].dt.time
    df['hour'] = df['datetime'].dt.hour
    df['minute'] = df['datetime'].dt.minute
    df['weekday'] = df['datetime'].dt.day_name()
    df['month'] = df['datetime'].dt.month_name()
    
    df['temperature_celsius'] = df['temperature'] - 273.15
    df['temperature_celsius'] = df['temperature_celsius'].round(2)
    
    df['is_hot_day'] = df['temperature_celsius'] > 30
    df['is_rainy'] = df['weather'].str.contains("Rain", case=False)
    df['is_night'] = df['hour'].apply(lambda x: x < 6 or x >= 18)
    
    df1=df[['city','country','humidity','weather','wind_speed','date','time','hour','weekday','month','temperature_celsius','is_hot_day','is_rainy','is_night']]
    df1 = df1.rename(columns={
    'city': 'City_Name',
    'country': 'Country_Name',
    'humidity': 'Humidity_Percent',
    'weather': 'Weather_Type',
    'wind_speed': 'WindSpeed',
    'temperature_celsius': 'Temperature',
    'is_hot_day': 'Hot_Day',
    'is_rainy': 'Rainy_Day',
    'is_night': 'Night_Time',
    'date': 'Date',
    'time': 'Time',
    'hour': 'Hour_of_Day',
    'weekday': 'Day_of_Week',
    })
    
    return df1

if __name__=="__main__":
    df=transform(df)
    df.to_csv("transformed_weather_data.csv",index=False)