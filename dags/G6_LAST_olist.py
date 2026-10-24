from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from pendulum import timezone

from scripts.azure_upload import upload_to_adls

LOCAL_FOLDER = Path("/opt/airflow/data/olist")
FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_order_reviews_dataset.csv",
]
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "azure_blob_storage_olist"
BLOB_PREFIX = "raw/G6/olist"

default_args = {
    "owner": "LAST",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="g6_last_olist",
    description="Carga cinco archivos de Olist desde la carpeta local a Raw en Azure.",
    default_args=default_args,
    start_date=datetime(2026, 10, 24, tzinfo=timezone("America/Lima")),
    schedule="0 6 * * *",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["G6", "olist", "raw"],
)
def upload_dag():
    @task
    def upload_files():
        for filename in FILES:
            local_file_path = LOCAL_FOLDER / filename
            if not local_file_path.is_file():
                raise FileNotFoundError(local_file_path)

            upload_to_adls(
                local_file_path=str(local_file_path),
                container_name=CONTAINER_NAME,
                blob_name=f"{BLOB_PREFIX}/{filename}",
                wasb_conn_id=WASB_CONN_ID,
            )

    upload_files()


dag = upload_dag()
