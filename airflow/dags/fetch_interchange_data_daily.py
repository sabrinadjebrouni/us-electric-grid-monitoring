from pathlib import Path
import json
import requests
import pandas as pd
import pyarrow as pa
import os
import io
import pendulum
from airflow import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import task
import time
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators import TriggerDagRunOperator

GCS_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

API_URL = "https://api.eia.gov/v2/electricity/rto/interchange-data/data"
API_NAME = "interchange_data"


EIA_KEY_FILE = json.load(open('/opt/airflow/eia_api_key.json'))
EIA_KEY = EIA_KEY_FILE['key']


def _fetch_rto_data(start_period, end_period, api_route):
    
    all_records = []

    params = {
        "api_key": EIA_KEY,
        "frequency": "hourly",
        "data[0]" : "value",
        "start": start_period,
        "end": end_period,
        "offset": 0,
        "length": 5000
    }

    total_api_calls=0

    response = requests.get(api_route, params=params)
    if response.status_code == 200:
        total_api_calls += 1
        total_rows = int(response.json()['response']['total'])
        all_records.extend(response.json()['response']['data']) #doing this to not waste the first api call
        params["offset"] += 5000

        while params['offset'] < total_rows:
            response = requests.get(api_route, params=params)
            if response.status_code == 200:
                    all_records.extend(response.json()['response']['data'])
                    params["offset"] += 5000
                    total_api_calls += 1
            else:
                print(f"Error: {response.status_code}")
                print(response.text)
                break
    
    print(f"Total api calls is {total_api_calls} for route {api_route}")

    return all_records

def _upload_to_gcs(route_name, all_records, data_interval_start):
    year = data_interval_start.format('YYYY')
    month = data_interval_start.format('MM')
    day = data_interval_start.format('DD')

    # transform data into parquet in RAM to gain more speed than write/read from disk
    buffer = io.BytesIO() #allocate RAM space
    df = pd.DataFrame(all_records) #transform dict to dataframe
    df.to_parquet(buffer, index=False, engine='pyarrow') #transform the df into parquet format and put it in buffer
    parquet_bytes = buffer.getvalue() #get parquet data in bytes format

    object_namespace = f"{route_name}/{year}/{month}/{day}/data.parquet"

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    gcs_hook.upload(
        bucket_name = GCS_BUCKET_NAME,
        object_name = object_namespace,
        data = parquet_bytes,
        mime_type ='application/vnd.apache.parquet',
        num_max_attempts =3
    )
    buffer.close()


with DAG(
    dag_id="fetch_fuel_type_data_daily",
    schedule=CronDataIntervalTimetable("5 13 * * *", timezone="UTC"), #daily at 3:05pm UTC
    start_date=pendulum.datetime(year=2019, month=1, day=1),
    end_date=pendulum.datetime(year=2030, month=12, day=31),
    catchup=False,
    max_active_runs=20 
):
    
    @task(pool="fetch_rto_data_pool")
    def fetch_and_upload_to_gcs(data_interval_start=None):
       
        start_period = data_interval_start.subtract(days=2).format('YYYY-MM-DD') #fetch data from the day before yesterday
        end_period = data_interval_start.format('YYYY-MM-DD')

        fetch_start = time.time()
        records =_fetch_rto_data(start_period, end_period, API_URL)

        fetch_duration = time.time()-fetch_start
        print(f"DATA_INTERVAL_START : {start_period}")
        print(f"DATA_INTERVAL_END : {end_period}")
        print(f"Fetch duration : {fetch_duration} ")

        if records: #check if data is fetched, else do not upoload to gcs
            upload_start = time.time()

            _upload_to_gcs(API_NAME, records, data_interval_start)

            upload_duration = time.time()-upload_start
            print(f"Upload_duration : {upload_duration}")

        else:
            print(f"No data found for {API_NAME} at {start_period}")

            
    trigger_spark_job = TriggerDagRunOperator(
        task_id="trigger_spark_job",
        trigger_dag_id="spark_job_transform_interchange",
        conf={
            "api_name": API_NAME,
            "date_path": "{{ data_interval_start.format('YYYY/MM/DD') }}"
        },
    )

    fetch_and_upload_to_gcs() >> trigger_spark_job