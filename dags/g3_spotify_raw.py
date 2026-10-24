from airflow.decorators import dag, task, task_group
from pendulum import timezone
from scripts.azure_upload import upload_to_adls
from scripts import g3_spotify_utils as utils
from datetime import datetime, timedelta
import logging

INCOMING_DIR = "/opt/airflow/data/g3/incoming"
PROCESSED_DIR = "/opt/airflow/data/g3/processed"
ERROR_DIR = "/opt/airflow/data/g3/error"

CONTAINER_NAME = "datalake"
WASB_CONN_ID = "azure_blob_storage"
BASE_BLOB = "raw/airflow/G3"
REGISTRO_BLOB = f"{BASE_BLOB}/_control/registro_cargas.csv"

TIPOS = ["charts", "tracks", "artists"]

default_args = {
    'owner': 'g3',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="g3_spotify_raw",
    description="Carga diaria de CSV de Spotify desde la carpeta local a la zona Raw de Azure.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Lima")),
    schedule="0 6 * * *", # Runs every day at 06:00 local time (GMT-5)
    catchup=False,
    max_active_runs=1, # Only one run at a time writes the control file
    tags=["g3", "spotify", "azure", "raw"],
)
def g3_spotify_raw():

    # Checks every 5 minutes if the daily ranking arrived (fails after 1 hour)
    @task.sensor(poke_interval=300, timeout=3600, mode="reschedule")
    def detectar_archivos():
        archivos = utils.listar_archivos(INCOMING_DIR, "charts")
        logging.info(f"==> Rankings encontrados: {archivos}")
        return len(archivos) > 0

    @task
    def validar(tipo):
        resultados = []
        for archivo in utils.listar_archivos(INCOMING_DIR, tipo):
            r = utils.validar_archivo(INCOMING_DIR, archivo, tipo)
            logging.info(f"==> {archivo}: valido={r['valido']} filas={r['filas']} {r['motivo']}")
            resultados.append(r)
        return resultados

    @task.branch
    def elegir_camino(tipo, resultados):
        caminos = []
        if any(r["valido"] for r in resultados):
            caminos.append(f"{tipo}.revisar_duplicados")
        if any(not r["valido"] for r in resultados):
            caminos.append(f"{tipo}.enviar_cuarentena")
        if not caminos:
            logging.info(f"==> No llegaron archivos de {tipo}")
        return caminos

    @task
    def revisar_duplicados(resultados):
        huellas = utils.huellas_cargadas(WASB_CONN_ID, CONTAINER_NAME, REGISTRO_BLOB)
        validos = []
        for r in resultados:
            if r["valido"]:
                r["duplicado"] = r["huella"] in huellas
                huellas.append(r["huella"])
                validos.append(r)
        return validos

    @task
    def subir_raw(validos):
        registros = []
        for r in validos:
            if r["duplicado"]:
                logging.info(f"==> {r['archivo']} ya fue cargado antes (misma huella), se omite")
                registros.append(utils.registro(r, "omitido_duplicado"))
                continue
            blob_name = f"{BASE_BLOB}/spotify/{r['tipo']}/fecha={r['fecha']}/{r['archivo']}"
            upload_to_adls(
                local_file_path=r["ruta"],
                container_name=CONTAINER_NAME,
                blob_name=blob_name,
                wasb_conn_id=WASB_CONN_ID
                )
            registros.append(utils.registro(r, "cargado", blob_name))
        return registros

    @task
    def enviar_cuarentena(resultados):
        registros = []
        hoy = datetime.now(timezone("America/Lima")).strftime("%Y-%m-%d")
        for r in resultados:
            if not r["valido"]:
                blob_name = f"{BASE_BLOB}/_rechazados/{r['tipo']}/fecha={hoy}/{r['archivo']}"
                upload_to_adls(
                    local_file_path=r["ruta"],
                    container_name=CONTAINER_NAME,
                    blob_name=blob_name,
                    wasb_conn_id=WASB_CONN_ID
                    )
                utils.guardar_texto(WASB_CONN_ID, CONTAINER_NAME, blob_name + ".motivo.txt", r["motivo"])
                logging.info(f"==> {r['archivo']} enviado a cuarentena: {r['motivo']}")
                registros.append(utils.registro(r, "rechazado", blob_name))
        return registros

    # Runs unless an upload failed (skipped paths do not block it)
    @task(trigger_rule="none_failed_min_one_success")
    def mover_local(resultados):
        for r in resultados:
            if r["valido"]:
                utils.mover(r["ruta"], PROCESSED_DIR)
            else:
                utils.mover(r["ruta"], ERROR_DIR)

    # Runs even if a group failed, to keep track of what was loaded
    @task(trigger_rule="all_done")
    def registrar_control(ti=None):
        registros = []
        for tipo in TIPOS:
            registros += ti.xcom_pull(task_ids=f"{tipo}.subir_raw") or []
            registros += ti.xcom_pull(task_ids=f"{tipo}.enviar_cuarentena") or []
        if registros:
            utils.agregar_al_registro(WASB_CONN_ID, CONTAINER_NAME, REGISTRO_BLOB, registros)
        logging.info(f"==> {len(registros)} archivos agregados al registro de control")

    # One group per file type, all running in parallel
    grupos = []
    for tipo in TIPOS:
        @task_group(group_id=tipo)
        def procesar_tipo():
            resultados = validar(tipo)
            camino = elegir_camino(tipo, resultados)
            validos = revisar_duplicados(resultados)
            cuarentena = enviar_cuarentena(resultados)
            camino >> [validos, cuarentena]
            [subir_raw(validos), cuarentena] >> mover_local(resultados)

        grupos.append(procesar_tipo())

    detectar_archivos() >> grupos >> registrar_control()

dag = g3_spotify_raw()
