from datetime import datetime, timedelta
from pendulum import timezone
from airflow.decorators import task, dag
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from scripts.g4_scripts import download_json_data, download_txt_data

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
    def task_download_json_data():
        return download_json_data()

    @task
    def task_download_txt_data():
        return download_txt_data()

    @task
    def clean_standarization(data_json, data_txt):
        print("Cleaning and standardizing data...")

    @task
    def quality_validation(data):
        print("Validating data quality...")

    @task
    def integration_upload(data):
        print("Uploading integrated data...")

    data_json = task_download_json_data()
    data_txt = task_download_txt_data()
    data = clean_standarization(data_json, data_txt)
    data = quality_validation(data)
    integration_upload(data)
dag = g04_parcial_project()
