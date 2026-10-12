import os
from datetime import datetime, timedelta
from pendulum import timezone
from airflow.decorators import task, dag
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from services.azure_service import upload_to_azure

AZURE_PATH = "raw/G4"
CONTAINER_NAME = "airflow"
LOCAL_PATH = "/opt/airflow/data"
WASB_CONN_ID = "azure_blob_storage"

default_args = {
    'owner': 'g4_BIEPC',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g4_BIEPC",
    description="Laboratory 02: Upload a local file to Azure Blob Storage",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Lima")),
    schedule=MultipleCronTriggerTimetable("@daily", timezone="UTC"),
    catchup=False,
    tags=["g4", "BIEPC", "laboratory_02", "azure", "blob", "upload"],
) 
def g04_lab_02_upload_file_to_azure_dag():

    @task
    def upload_file_to_azure():
        file_path = os.path.join(LOCAL_PATH, 'sample.txt')

        upload_to_azure(
            local_file_path=file_path,
            container_name=CONTAINER_NAME,
            destination_path=AZURE_PATH,
            wasb_conn_id=WASB_CONN_ID
        )

    upload_file_to_azure()

dag = g04_lab_02_upload_file_to_azure_dag()