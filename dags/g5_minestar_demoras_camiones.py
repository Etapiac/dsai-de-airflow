from airflow.decorators import dag, task
from airflow.sdk import Param
from pendulum import timezone
from datetime import date, datetime, timedelta
import pendulum

from scripts.g5_minestar import pipeline

default_args = {
    'owner': 'G5',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),  # el reporte de MineStar puede llegar con retraso
}

@dag(
    dag_id="g5_minestar_demoras_camiones",
    description="Ingesta diaria de demoras de camiones (MineStar) a la zona Raw del Data Lake UTEC.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=timezone("America/Lima")),
    schedule="30 7 * * *",  # 07:30 hora de Lima: el día operativo cierra con el cambio de turno de las 07:00
    catchup=False,
    max_active_runs=1,
    params={
        "fecha_operativa": Param(
            "", type="string",
            description="Día operativo a procesar (YYYY-MM-DD). Vacío = último día cerrado.",
        ),
    },
    tags=["G5", "minestar", "camiones", "raw", "utec"],
)
def g5_minestar_demoras_camiones():

    @task
    def resolver_fecha(**context):
        fecha_ejecucion = context.get("logical_date") or context["dag_run"].run_after
        fecha = pipeline.resolver_fecha_operativa(context["params"]["fecha_operativa"], fecha_ejecucion)
        return fecha.isoformat()

    @task
    def detectar_archivo(fecha):
        return pipeline.detectar_archivo(date.fromisoformat(fecha))

    @task
    def validar(ruta, fecha):
        return pipeline.validar_reporte(ruta, date.fromisoformat(fecha))

    @task
    def normalizar(ruta, fecha):
        fecha_carga = pendulum.now("America/Lima")
        return pipeline.normalizar_reporte(ruta, date.fromisoformat(fecha), fecha_carga)

    @task(retries=3, retry_delay=timedelta(minutes=5))
    def cargar_a_raw(ruta, fecha):
        return pipeline.cargar_a_raw(ruta, date.fromisoformat(fecha))

    @task
    def calcular_kpis(ruta, fecha):
        return pipeline.calcular_kpis(ruta, date.fromisoformat(fecha))

    fecha = resolver_fecha()
    ruta = detectar_archivo(fecha)
    resumen = validar(ruta, fecha)
    normalizado = normalizar(ruta, fecha)
    resumen >> normalizado
    cargar_a_raw(normalizado, fecha)
    calcular_kpis(normalizado, fecha)

dag = g5_minestar_demoras_camiones()
