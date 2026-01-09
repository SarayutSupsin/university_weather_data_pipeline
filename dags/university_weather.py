import json
from datetime import datetime
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

from datetime import datetime, timedelta


# 1. ดึงข้อมูลจาก API

def get_weather_data(**context):
    print("=== Start get_weather_data ===")
    data = {}
    try:
        url = 'https://api.open-meteo.com/v1/forecast?latitude=7.0065&longitude=100.4993&hourly=temperature_2m&timezone=Asia%2FBangkok'
        response = requests.get(url, timeout=30)  # เพิ่ม timeout
        response.raise_for_status()
        data = response.json()
        print(f"Received {len(data.get('hourly', {}).get('temperature_2m', []))} temperature points")
    except Exception as e:
        print(f"Error in get_weather_data: {e}, pushing empty dict to XCom")
        data = {"hourly": {"time": [], "temperature_2m": []}}

    context['ti'].xcom_push(key='weather_data', value=data)
    print("Successfully pushed weather_data to XCom")
    print("=== End get_weather_data ===")



# 2. Transform & Load

def transform_and_load(**context):
    print("=== Start transform_and_load ===")
    data = context['ti'].xcom_pull(task_ids='get_weather_data', key='weather_data')
    
    times = data.get('hourly', {}).get('time', [])
    temps = data.get('hourly', {}).get('temperature_2m', [])

    if not times or not temps:
        print("No data available, skipping transform_and_load")
        raise AirflowSkipException("No weather data, skipping task")

    pg_hook = PostgresHook(postgres_conn_id='weather_postg')

    for t, temp in zip(times, temps):
        time_obj = datetime.fromisoformat(t)

        if temp >= 30:
            category = 'ร้อน'
        elif temp >= 25:
            category = 'ปกติ'
        else:
            category = 'เย็น'

        insert_sql = """
            INSERT INTO university_weather (time, temperature, temperature_category)
            VALUES (%s, %s, %s);
        """
        pg_hook.run(insert_sql, parameters=(time_obj, temp, category))

    print("=== End transform_and_load ===")



# 3. DAG definition

default_args = {
    'owner': 'ait',
    'start_date': datetime(2026, 1, 6),
    'retries': 1,
}

with DAG(
    dag_id='university_weather_pipeline',
    default_args=default_args,
    schedule='@hourly',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='get_weather_data',
        python_callable=get_weather_data,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    t2 = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load
    )

    t1 >> t2
