from datetime import datetime, timedelta
from dags.services.azure_service import upload_to_azure
from pendulum import timezone
from airflow.decorators import task, dag
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from scripts.g4_scripts import download_json_data, download_txt_data

AZURE_PATH = "raw/G4"
CONTAINER_NAME = "airflow"
WASB_CONN_ID = "azure_blob_storage_2"

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
    def upload_azure_datalake(path_file_json, path_file_txt):
        print("Uploading integrated data...")

        for file_path in [path_file_json, path_file_txt]:
            upload_to_azure(
                local_file_path=file_path,
                container_name=CONTAINER_NAME,
                destination_path=AZURE_PATH,
                wasb_conn_id=WASB_CONN_ID
            )

    data_json = task_download_json_data()
    data_txt = task_download_txt_data()
    upload_azure_datalake(data_json, data_txt)
dag = g04_parcial_project()
