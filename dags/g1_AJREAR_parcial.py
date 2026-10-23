"""
### Grupo 1 (AJREAR) - Avance del Proyecto Integrador

Sube a la zona **Raw** de Azure Data Lake, sin transformarlos, los CSV de lecturas de
medidores que el servidor (simulado con la carpeta `data/G1/mediciones`) deja cada semana:

`datalake/raw/airflow/G1/mediciones/fecha_carga=YYYY-MM-DD/<archivo original>.csv`

Flujo: `listar_archivos` -> `validar_archivo[n]` -> `cargar_archivo[n]` (una tarea por archivo, en paralelo).
Requiere la conexión `utec_blob_storage` (tipo `wasb`) en Admin -> Connections.
"""
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from scripts.g1_mediciones import listar_csv, subir_csv, validar_csv

TZ = pendulum.timezone("America/Lima")
DATA_DIR = "/opt/airflow/data/G1/mediciones"
CONTAINER_NAME = "datalake"
WASB_CONN_ID = "utec_blob_storage"
RAW_PREFIX = "raw/airflow/G1/mediciones"
COLUMNAS_MINIMAS = ["periodo", "numero_cliente", "fecha", "hora", "kwh_del_int"]

default_args = {"owner": "grupo_1", "retries": 1, "retry_delay": timedelta(minutes=1)}


@dag(
    dag_id="g1_ajrear_parcial",
    description="Sube los CSV de lecturas de medidores a la zona Raw de Azure Data Lake.",
    doc_md=__doc__,
    default_args=default_args,
    start_date=datetime(2026, 10, 1, tzinfo=TZ),
    schedule="0 6 * * 1",  # Semanal: lunes 06:00 hora Lima
    catchup=False,
    max_active_runs=1,
    tags=["g1", "AJREAR", "parcial", "azure", "raw"],
)
def ingesta_mediciones():

    @task
    def listar_archivos() -> list[str]:
        archivos = listar_csv(DATA_DIR)
        if not archivos:
            raise AirflowSkipException(f"No hay CSV en {DATA_DIR}: no hay nada que cargar")
        return archivos

    @task(retries=0)  # un archivo inválido no se arregla reintentando
    def validar_archivo(archivo: str) -> str:
        validar_csv(archivo, COLUMNAS_MINIMAS)
        return archivo

    @task(retries=3, retry_delay=timedelta(minutes=2), retry_exponential_backoff=True,
          execution_timeout=timedelta(hours=2))
    def cargar_archivo(archivo: str, **context) -> str:
        fecha_carga = context.get("ds") or pendulum.now(TZ).to_date_string()
        return subir_csv(archivo, CONTAINER_NAME, f"{RAW_PREFIX}/fecha_carga={fecha_carga}", WASB_CONN_ID)

    cargar_archivo.expand(archivo=validar_archivo.expand(archivo=listar_archivos()))


dag = ingesta_mediciones()
