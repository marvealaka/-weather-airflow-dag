from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
import json
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
import s3fs
import os

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9 / 5) + 32
    return temp_in_fahrenheit

def transform_loaded_data(task_instance):
    # fetch the data from the previous task
    data = task_instance.xcom_pull(task_ids='fetch_weather_data')
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Minimum Temp (F)": min_temp_fahrenheit,
        "Maximum Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    # Initialize boto3 session
    aws_credencials = {"key": os.environ.get("AWS_ACCESS_KEY_ID"), "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"), "token": os.environ.get("AWS_SESSION_TOKEN")}

    now = datetime.now()
    df_str = now.strftime("current_weather_data_%Y-%m-%d_%H-%M-%S")
    
    # Save the transformed data to S3
    df_data.to_csv(f's3://weatherapisecondproject/{df_str}.csv', index=False, storage_options=aws_credencials)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='weather_dag',
    default_args=default_args,
    description='A simple weather data processing DAG',
    schedule=timedelta(days=1),
    catchup=False
) as dag:

    is_weather_data_available = HttpSensor(
        task_id='is_weather_data_available',
        http_conn_id='weather_api',
        endpoint='data/2.5/weather?q=texas&appid=9db4eb62ce2e6a9a0798e2af70015ff3',
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=600
    )

    fetch_weather_data = HttpOperator(
        task_id='fetch_weather_data',
        http_conn_id='weather_api',
        endpoint='data/2.5/weather?q=texas&appid=9db4eb62ce2e6a9a0798e2af70015ff3',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_loaded_data
    )

    is_weather_data_available >> fetch_weather_data >> transform_weather_data
