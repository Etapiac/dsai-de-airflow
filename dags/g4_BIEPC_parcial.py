import os
from datetime import datetime, timedelta
from pendulum import timezone
from airflow.decorators import task, dag
from airflow.timetables.trigger import MultipleCronTriggerTimetable

default_args = {
    'owner': 'g4_BIEPC_parcial',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g4_BIEPC_parcial",
    description="Avance Proyecto Parcial",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Lima")),
    schedule=MultipleCronTriggerTimetable("@daily", timezone="UTC"),
    catchup=False,
    tags=["g4", "BIEPC", "laboratory_02", "azure", "blob", "upload"],
)
def g04_parcial_project():

    @task
    def download_json_data():
        print("Downloading JSON data...")

    @task
    def download_txt_data():
        print("Downloading TXT data...")

    @task
    def clean_standarization(data_json, data_txt):
        print("Cleaning and standardizing data...")

    @task
    def quality_validation(data):
        print("Validating data quality...")

    @task
    def integration_upload(data):
        print("Uploading integrated data...")

    data_json = download_json_data()
    data_txt = download_txt_data()
    data = clean_standarization(data_json, data_txt)
    data = quality_validation(data)
    integration_upload(data)
dag = g04_parcial_project()