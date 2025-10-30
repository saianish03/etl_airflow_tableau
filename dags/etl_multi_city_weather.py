from typing import Any
from attr.setters import convert
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

import requests_cache
from retry_requests import retry
import openmeteo_requests

from datetime import datetime, timedelta
import json
import math
import pandas as pd
import numpy as np

CITIES = [
    {"name": "Boston", "latitude": "42.3601", "longitude": "-71.0589", "country": "USA"},
    {"name": "London", "latitude": "51.5074", "longitude": "-0.1278", "country": "UK"},
    {"name": "Mumbai", "latitude": "19.0760", "longitude": "72.8777", "country": "India"},
    {"name": "Tokyo", "latitude": "35.6762", "longitude": "139.6503", "country": "Japan"},
    {"name": "Sydney", "latitude": "-33.8688", "longitude": "151.2093", "country": "Australia"},
    {"name": "Sao Paulo", "latitude": "-23.5505", "longitude": "-46.6333", "country": "Brazil"}
]

POSTGRES_CONN_ID = 'postgres_default'
URL = 'https://api.open-meteo.com/v1/forecast'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}


end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

# setup the open meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

with DAG(dag_id='Multi-City-Weather-ETL-Pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_weather_data():
        """
        Extract historical weather data from Open-Meteo API for multiple cities
        """
        
        # parameters to fetch
        hourly_params = [
            'temperature_2m',
            'relative_humidity_2m', 
            'precipitation',
            'wind_speed_10m',
            'wind_direction_10m',
            'pressure_msl',
            'cloud_cover',
            'weather_code'
        ]
        
        all_city_data = []
        
        for city in CITIES:
            params = {
                "latitude": city['latitude'],
                "longitude": city['longitude'],
                "hourly": hourly_params,
                "start_date": start_date,
                "end_date": end_date
            }
            
            try:
                response = openmeteo.weather_api(url = URL, params=params)
                json_output = convert_to_json(response[0], city['name'], city['country'])
                all_city_data.append(json_output)

            except Exception as e:
                print(f"Error fetching data for {city['name']}: {str(e)}")
                continue
        
        return all_city_data
    
    @task()
    def transform_weather_data(city_weather_data):
        """
        Transform the extracted weather data with basic and advanced calculations
        """
        transformed_records = []
        
        for city_data in city_weather_data:
            
            hourly_timestamps = pd.date_range(
                start = pd.to_datetime(city_data['timestamp_start'], unit = "s", utc = True),
                end =  pd.to_datetime(city_data['timestamp_end'], unit = "s", utc = True),
                freq = pd.Timedelta(seconds = city_data['timestamp_interval']),
                inclusive = "left"
            )
            
            for i, ts in enumerate(hourly_timestamps):
                # timestamp = hourly_timestamps[i]
                temperature = round(city_data['temperature'][i] if city_data['temperature'][i] is not None else 0, 1)
                humidity = round(city_data['humidity'][i] if city_data['humidity'][i] is not None else 0, 1)
                precipitation = round(city_data['precipitation'][i] if city_data['precipitation'][i] is not None else 0, 1)
                wind_speed = round(city_data['wind_speed'][i] if city_data['wind_speed'][i] is not None else 0, 1)
                wind_direction = round(city_data['wind_direction'][i] if city_data['wind_direction'][i] is not None else 0, 1)
                pressure = round(city_data['pressure'][i] if city_data['pressure'][i] is not None else 0, 1)
                cloud_cover = round(city_data['cloud_cover'][i] if city_data['cloud_cover'][i] is not None else 0, 1)
                weather_code = city_data['weather_code'][i] if city_data['weather_code'][i] is not None else 0
                
                # feels_like_temp (heat index or wind chill)
                feels_like_temp = calculate_feels_like_temp(temperature, humidity, wind_speed)
                
                # precipitation categorization
                precipitation_category = categorize_precipitation(precipitation)
                
                # wind speed categorization
                wind_category = categorize_wind_speed(wind_speed)
                
                # comfort index
                comfort_index = calculate_comfort_index(temperature, humidity)
                
                record = {
                    'city_name': city_data['city_name'],
                    'latitude': city_data['latitude'],
                    'longitude': city_data['longitude'],
                    'country': city_data['country'],
                    'timestamp': int(hourly_timestamps[i].timestamp()),
                    'temperature_2m': temperature,
                    'relative_humidity_2m': humidity,
                    'precipitation': precipitation,
                    'wind_speed_10m': wind_speed,
                    'wind_direction_10m': wind_direction,
                    'pressure_msl': pressure,
                    'cloud_cover': cloud_cover,
                    'weather_code': weather_code,
                    'feels_like_temp': feels_like_temp,
                    'precipitation_category': precipitation_category,
                    'wind_category': wind_category,
                    'comfort_index': comfort_index,
                    'data_loaded_at': datetime.now().isoformat()
                }
                
                transformed_records.append(record)
        
        return transformed_records
    
    @task()
    def load_weather_data(transformed_data):
        """
        Load the transformed data into PostgreSQL
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # create table if doesnt exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data_multi_city (
            city_name VARCHAR(50),
            latitude FLOAT,
            longitude FLOAT,
            country VARCHAR(50),
            timestamp TIMESTAMP,
            temperature_2m FLOAT,
            relative_humidity_2m FLOAT,
            precipitation FLOAT,
            wind_speed_10m FLOAT,
            wind_direction_10m FLOAT,
            pressure_msl FLOAT,
            cloud_cover FLOAT,
            weather_code INT,
            feels_like_temp FLOAT,
            precipitation_category VARCHAR(20),
            wind_category VARCHAR(20),
            comfort_index INT,
            data_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (city_name, timestamp)
        );
        """)
        
        # create index for faster queries
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_city_timestamp 
        ON weather_data_multi_city (city_name, timestamp);
        """)

        insert_data = []
        for record in transformed_data:
            insert_data.append((
                record['city_name'],
                record['latitude'],
                record['longitude'],
                record['country'],
                datetime.fromtimestamp(record['timestamp']),
                record['temperature_2m'],
                record['relative_humidity_2m'],
                record['precipitation'],
                record['wind_speed_10m'],
                record['wind_direction_10m'],
                record['pressure_msl'],
                record['cloud_cover'],
                record['weather_code'],
                record['feels_like_temp'],
                record['precipitation_category'],
                record['wind_category'],
                record['comfort_index']
            ))

        # bulk insert using executemany for efficiency
        cursor.executemany("""
        INSERT INTO weather_data_multi_city 
        (city_name, latitude, longitude, country, timestamp, temperature_2m, 
         relative_humidity_2m, precipitation, wind_speed_10m, wind_direction_10m, 
         pressure_msl, cloud_cover, weather_code, feels_like_temp, 
         precipitation_category, wind_category, comfort_index)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_name, timestamp) DO UPDATE SET
            temperature_2m = EXCLUDED.temperature_2m,
            relative_humidity_2m = EXCLUDED.relative_humidity_2m,
            precipitation = EXCLUDED.precipitation,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            wind_direction_10m = EXCLUDED.wind_direction_10m,
            pressure_msl = EXCLUDED.pressure_msl,
            cloud_cover = EXCLUDED.cloud_cover,
            weather_code = EXCLUDED.weather_code,
            feels_like_temp = EXCLUDED.feels_like_temp,
            precipitation_category = EXCLUDED.precipitation_category,
            wind_category = EXCLUDED.wind_category,
            comfort_index = EXCLUDED.comfort_index,
            data_loaded_at = EXCLUDED.data_loaded_at;
        """, insert_data)
        
        conn.commit()
        cursor.close()
        
        return f"Successfully loaded {len(transformed_data)} weather records for {len(CITIES)} cities"
    

    def convert_to_json(response, city_name, country):
        """
        Convert API response object into serializable JSON data for airflow
        """
        hourly_data = response.Hourly()

        return {
            'city_name': city_name,
            'country': country,
            'latitude': response.Latitude(),
            'longitude': response.Longitude(),
            'timestamp_start': hourly_data.Time(),
            'timestamp_end': hourly_data.TimeEnd(),
            'timestamp_interval': hourly_data.Interval(),
            'temperature': hourly_data.Variables(0).ValuesAsNumpy().tolist(),
            'humidity': hourly_data.Variables(1).ValuesAsNumpy().tolist(),
            'precipitation': hourly_data.Variables(2).ValuesAsNumpy().tolist(),
            'wind_speed': hourly_data.Variables(3).ValuesAsNumpy().tolist(),
            'wind_direction': hourly_data.Variables(4).ValuesAsNumpy().tolist(),
            'pressure': hourly_data.Variables(5).ValuesAsNumpy().tolist(),
            'cloud_cover': hourly_data.Variables(6).ValuesAsNumpy().tolist(),
            'weather_code': hourly_data.Variables(7).ValuesAsNumpy().tolist(),
        }
    
    def calculate_feels_like_temp(temp, humidity, wind_speed):
        """
        Calculate feels-like temperature using heat index or wind chill
        """
        if temp > 27:  # heat index for warm temps
            hi = temp + 0.5 * (humidity - 50) * 0.1
            return round(hi, 1)
        elif temp < 10:  # wind chill for cold temps
            wc = temp - (wind_speed * 0.7)
            return round(wc, 1)
        else:
            return temp
    
    def categorize_precipitation(precip):
        """
        Categorize precipitation amount
        """
        if precip == 0:
            return "none"
        elif precip < 2.5:
            return "light"
        elif precip < 10:
            return "moderate"
        else:
            return "heavy"
    
    def categorize_wind_speed(wind_speed):
        """
        Categorize wind speed
        """
        if wind_speed < 5:
            return "calm"
        elif wind_speed < 20:
            return "breeze"
        elif wind_speed < 40:
            return "strong"
        else:
            return "gale"
    
    def calculate_comfort_index(temp, humidity):
        """
        Calculate comfort index (0-100) based on temperature and humidity
        """

        temp_score = 0
        if 18 <= temp <= 24:
            temp_score = 50
        elif 15 <= temp < 18 or 24 < temp <= 27:
            temp_score = 40
        elif 10 <= temp < 15 or 27 < temp <= 30:
            temp_score = 30
        else:
            temp_score = 20
        

        humidity_score = 0
        if 40 <= humidity <= 60:
            humidity_score = 50
        elif 30 <= humidity < 40 or 60 < humidity <= 70:
            humidity_score = 40
        elif 20 <= humidity < 30 or 70 < humidity <= 80:
            humidity_score = 30
        else:
            humidity_score = 20
        
        return temp_score + humidity_score
    
    # creating the DAG workflow - E --> T --> L
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_result = load_weather_data(transformed_data)
