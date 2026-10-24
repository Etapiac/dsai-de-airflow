from airflow.decorators import dag, task
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix
from datetime import datetime, timedelta
from pathlib import Path

LOCAL_FILE_PATH = "/opt/airflow/data/sample.txt"
LOCAL_FOLDER= "/opt/airflow/data/incoming"
CONTAINER_NAME = "datalake" # airflow
WASB_CONN_ID = "utec_blob_storage"
BLOB_PREFIX= "raw/airflow/G2"
ESPERADOS = ["agencies", "customers", "installments", "loans", "payments"]  # los 5 archivos del día
 
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g2_utec",
    description="Uploads all CSV files from the local folder to Azure Blob Storage.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 6 * * *",          # todos los dIas a las 06:00, antes de que empiece cobranzas
    catchup=False,
    tags=["utec", "blob", "upload"],
)

def upload_dag():

    @task
    def call_upload():
        archivos = sorted(Path(LOCAL_FOLDER).glob("*.csv"))
        faltan = [e for e in ESPERADOS if not any(a.name.startswith(e + "_") for a in archivos)]
        if faltan:
            raise FileNotFoundError(f"Faltan archivos de {faltan} en {LOCAL_FOLDER}. No se cargó nada.")

        for archivo in archivos:
            blob_name = f"{BLOB_PREFIX}/{archivo.name}"
            upload_to_adls(
                local_file_path=str(archivo),
                container_name=CONTAINER_NAME,
                blob_name=blob_name,
                wasb_conn_id=WASB_CONN_ID
                )
            print(f"Subido: {archivo.name} -> {CONTAINER_NAME}/{blob_name}")

    call_upload()


dag = upload_dag()
