"""Carga batch del servidor central de Concretera del Pacífico.

Recorre ``/opt/airflow/data/servidor_central`` y sus subcarpetas (un CSV por
planta), deja un único archivo en ``data/consolidado`` y lo sube a Azure Blob
Storage. Sustituye la consolidación manual. La frecuencia es diaria: las
plantas pueden dejar el archivo durante el día y el lote se cierra a las
07:00, hora de Lima.
"""

from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from pendulum import timezone

from scripts.azure_upload import upload_to_adls
from scripts.helpers import (
    abortar_corrida,
    add_date_suffix,
    consolidar_plantas,
    listar_csv_plantas,
    ruta_trabajo_parcial,
    validar_consolidado,
)

SERVIDOR_CENTRAL = "/opt/airflow/data/servidor_central"
LOCAL_FILE_PATH = "/opt/airflow/data/consolidado/ensayos_resistencia_consolidado.csv"
CONTAINER_NAME = "datalake"  # airflow
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow/G5/ensayos_resistencia_consolidado.csv"

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="g5_consolidar_plantas",
    description="Consolida los CSV de las 12 plantas y sube el archivo a Azure Blob Storage.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Lima")),
    schedule="0 7 * * *",
    catchup=False,
    tags=["concretera", "batch", "consolidacion", "azure"],
)
def consolidar_ensayos_plantas():

    @task
    def listar_plantas() -> list[str]:
        try:
            rutas = listar_csv_plantas(SERVIDOR_CENTRAL)
        except Exception as error:
            abortar_corrida(
                "listar_plantas",
                error,
                str(ruta_trabajo_parcial(LOCAL_FILE_PATH)),
            )
        logger.info("Plantas a consolidar: %s", rutas)
        return rutas

    @task
    def consolidar(rutas: list[str]) -> dict:
        try:
            resumen = consolidar_plantas(rutas, LOCAL_FILE_PATH)
        except Exception as error:
            # El primer fallo deja que Airflow reintente a los 5 minutos.
            # abortar_corrida borra el consolidado solo si ese reintento también falla.
            abortar_corrida(
                "consolidar",
                error,
                str(ruta_trabajo_parcial(LOCAL_FILE_PATH)),
            )
        logger.info("Archivo único generado: %s", resumen)
        return resumen

    @task
    def validar(resumen: dict) -> dict:
        try:
            resultado = validar_consolidado(resumen)
        except Exception as error:
            archivo = LOCAL_FILE_PATH
            if isinstance(resumen, dict) and resumen.get("archivo"):
                archivo = resumen["archivo"]
            abortar_corrida("validar", error, archivo)
        logger.info(
            "Validación ok: %s filas de %s plantas en %s",
            resultado["filas"],
            len(resultado["plantas"]),
            resultado["archivo"],
        )
        return resultado

    @task
    def call_upload(resumen: dict) -> str:
        blob_name = add_date_suffix(BLOB_NAME)
        local_file_path = LOCAL_FILE_PATH
        if isinstance(resumen, dict) and resumen.get("archivo"):
            local_file_path = resumen["archivo"]
        try:
            upload_to_adls(
                local_file_path=local_file_path,
                container_name=CONTAINER_NAME,
                blob_name=blob_name,
                wasb_conn_id=WASB_CONN_ID,
            )
        except Exception as error:
            abortar_corrida("call_upload", error)
        logger.info("==> Consolidado subido a %s/%s", CONTAINER_NAME, blob_name)
        return blob_name

    call_upload(validar(consolidar(listar_plantas())))


dag = consolidar_ensayos_plantas()
