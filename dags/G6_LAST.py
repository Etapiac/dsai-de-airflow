from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from pendulum import timezone

from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix

LOCAL_FILE_PATH = "/opt/airflow/data/sample.txt"
CONTAINER_NAME = "airflow"
WASB_CONN_ID = "azure_blob_storage"
BLOB_NAME = "raw/G6/archivo_subido.txt"

default_args = {
    "owner": "LAST",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="g6_last",
    description="Carga el archivo del grupo 6 a Azure con un sufijo de fecha.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1",
    catchup=False,
    tags=["G6", "azure", "blob", "upload"],
)
def upload_dag():
    @task
    def call_upload():
        if not Path(LOCAL_FILE_PATH).is_file():
            raise FileNotFoundError(LOCAL_FILE_PATH)

        new_blob_name = add_date_suffix(BLOB_NAME)
        upload_to_adls(
            local_file_path=LOCAL_FILE_PATH,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name,
            wasb_conn_id=WASB_CONN_ID,
        )

    call_upload()


dag = upload_dag()
