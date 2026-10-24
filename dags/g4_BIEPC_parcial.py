import os
from datetime import datetime, timedelta
from dags.services.azure_service import upload_to_azure
from pendulum import timezone
from airflow.decorators import task, dag
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from scripts.g4_scripts import download_json_data, download_txt_data, merge_sources_to_csv

AZURE_PATH = "raw/airflow/G4"
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "azure_blob_storage_2"

destination_paths = {
    "json": f"{AZURE_PATH}/pozos",
    "txt": f"{AZURE_PATH}/arena",
    "csv": f"{AZURE_PATH}/pozos_arena",
}

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
    schedule=MultipleCronTriggerTimetable("*/15 * * * *", timezone="UTC"),
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
    def task_merge_sources_to_csv(path_file_json, path_file_txt):
        return merge_sources_to_csv(path_file_json, path_file_txt)

    @task
    def upload_azure_datalake(path_file_json, path_file_txt, data_unificada_csv):
        for file_path in [path_file_json, path_file_txt, data_unificada_csv]:
            extension = os.path.splitext(file_path)[1].lstrip(".").lower()
            upload_to_azure(
                local_file_path=file_path,
                container_name=CONTAINER_NAME,
                destination_path=destination_paths[extension],
                wasb_conn_id=WASB_CONN_ID
            )

    data_json = task_download_json_data()
    data_txt = task_download_txt_data()
    data_unificada_csv = task_merge_sources_to_csv(data_json, data_txt)
    upload_azure_datalake(data_json, data_txt, data_unificada_csv)
dag = g04_parcial_project()
