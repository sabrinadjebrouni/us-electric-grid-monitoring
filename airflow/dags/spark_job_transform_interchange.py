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
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

GCS_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
PYSPARK_JOB_PATH = os.getenv("SPARK_INTERCHANGE_JOB_PATH")
GCS_DATASET_NAME = os.getenv("GCP_DATASET")
REGION="us-central1"
GCP_CONN_ID = "google_cloud_default"
CLUSTER_NAME = os.getenv("CLUSTER_NAME")


with DAG(
    dag_id="spark_job_transform_interchange",
    schedule=None, # runs when triggered by dags that fetch data from the api
    start_date=pendulum.datetime(year=2019, month=1, day=1),
    catchup=False
):
    date = "{{ dag_run.conf['date_path'] }}"

    pyspark_job = {
        "reference": {"project_id": GCS_PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_JOB_PATH,
            "args" : [GCS_PROJECT_ID, GCS_BUCKET_NAME, GCS_DATASET_NAME]
        } 
    }

    submit_job = DataprocSubmitJobOperator(
        task_id = "submit_job",
        project_id = GCS_PROJECT_ID,
        region = REGION,
        job= pyspark_job,
        gcp_conn_id = GCP_CONN_ID
    )


    submit_job
