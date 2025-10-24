import pandas as pd

def transform_raw(all_data):
    data = []
    for city, city_data in all_data.items():
        if city is not None:
            for forecast in city_data['forecast']:
                data.append({
                    'city': city_data['city_name'],
                    'country': city_data['country'],
                    'datetime': forecast['datetime'],
                    'temperature': forecast['temperature'],
                    'humidity': forecast['humidity'],
                    'weather': forecast['weather'],
                    'description': forecast['description'],
                    'wind_speed': forecast['wind_speed']
                })
    df = pd.DataFrame(data)
    return df


def transform(df):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['Date'] = df['datetime'].dt.strftime('%Y-%m-%d')   
    df['Time'] = df['datetime'].dt.strftime('%H:%M:%S')   
    df['Hour_of_Day'] = df['datetime'].dt.hour
    df['Minute'] = df['datetime'].dt.minute
    df['Day_of_Week'] = df['datetime'].dt.day_name()
    df['Month'] = df['datetime'].dt.month_name()
    
    df['Temperature'] = (df['temperature'] - 273.15).round(2)
    
    df['Hot_Day'] = df['Temperature'] > 30
    df['Rainy_Day'] = df['weather'].str.contains("Rain", case=False)
    df['Night_Time'] = df['Hour_of_Day'].apply(lambda x: x < 6 or x >= 18)
    
    df['Humidity_Percent'] = df['humidity'].fillna(0)
    df['WindSpeed'] = df['wind_speed'].fillna(0)
    
    df_clean = df[['city', 'country', 'Humidity_Percent', 'weather', 'WindSpeed',
                   'Date', 'Time', 'Hour_of_Day', 'Day_of_Week', 'Month',
                   'Temperature', 'Hot_Day', 'Rainy_Day', 'Night_Time']].rename(columns={
        'city': 'City_Name',
        'country': 'Country_Name',
        'weather': 'Weather_Type',
        'Month': 'month'
    })
    
    return df_clean
