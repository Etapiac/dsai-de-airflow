"""Parámetros del pipeline G5 MineStar (rutas, esquema, modelo de tiempos y flota)."""
import os

# --- Rutas (dentro del contenedor, montadas desde ./data) ---
DATA_DIR = os.getenv("G5_MINESTAR_DATA_DIR", "/opt/airflow/data/minestar")
EXPORTS_DIR = os.path.join(DATA_DIR, "exportaciones")   # exportaciones originales de MineStar
SERVER_DIR = os.path.join(DATA_DIR, "servidor")         # servidor simulado: 1 archivo por día operativo
RAW_READY_DIR = os.path.join(DATA_DIR, "raw_ready")     # archivo normalizado listo para subir
KPI_DIR = os.path.join(DATA_DIR, "kpi")                 # vista previa de KPI diarios (futura capa Gold)

SOURCE_FILE_TEMPLATE = "demoras_camiones_{yyyymmdd}.csv"

# --- Azure Data Lake (zona Raw del Data Lake UTEC) ---
WASB_CONN_ID = "utec_blob_storage"
CONTAINER_NAME = "datalake"
RAW_PREFIX = "raw/airflow/G5/minestar/demoras_camiones"

# --- Día operativo: la mina trabaja en turnos 07:00-19:00 y 19:00-07:00 ---
TIMEZONE = "America/Lima"
DAY_START_HOUR = 7
HOURS_PER_DAY = 24.0

# --- Esquema esperado del reporte de demoras de MineStar ---
SOURCE_COLUMNS = [
    "OID Minestar", "Flota", "Equipo", "Categoría Demora", "Subcategoría Demora",
    "Descripción Demora", "Inicio Demora", "Fin Demora", "Duración", "ID Datamart",
]
DATETIME_FORMAT = "%d-%b-%y %I:%M:%S %p"   # ej. 28-Dec-25 05:40:18 AM
ACTIVE_DELAY_TEXT = "Active Delay"          # MineStar marca así una demora aún abierta

# --- Modelo de tiempos (estándar ASARCO) aplicado a las categorías de MineStar ---
# Tiempo calendario = Disponible + Mantenimiento
# Disponible        = Operativo + Reserva
# Operativo         = Efectivo + Demoras
TIME_MODEL = {
    "Mant-Programado": "MANTENIMIENTO",
    "Mant-No-Programado": "MANTENIMIENTO",
    "Condicion-Operativa": "RESERVA",
    "Factor-Externo": "RESERVA",
    "Fuera de Plan": "RESERVA",
    "Demora-Programada": "DEMORA",
    "Demora-No-Programada": "DEMORA",
    "Paradas-Operacionales": "DEMORA",
}
UNPLANNED_MAINTENANCE = "Mant-No-Programado"  # cada evento cuenta como una falla (MTBF / MTTR)

# --- Flota de camiones de acarreo (maestro de equipos) ---
FLEET = {
    "Serie CM100": [f"CM1{n:02d}" for n in range(1, 31) if n not in (17, 19)],
    "Serie CM400": [f"CM4{n:02d}" for n in range(1, 11)],
    "Serie CM500": [f"CM5{n:02d}" for n in range(1, 9)],
}
SERIES_MODEL = {
    "Serie CM100": "CAT 797F",
    "Serie CM400": "Komatsu 980E",
    "Serie CM500": "CAT 798",
}
TRUCK_TO_SERIES = {truck: series for series, trucks in FLEET.items() for truck in trucks}
