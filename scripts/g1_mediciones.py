"""Funciones del Grupo 1 (AJREAR): listar, validar y subir a Azure los CSV de mediciones."""
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

log = logging.getLogger("airflow.task")


def listar_csv(carpeta):
    """Rutas de todos los CSV de la carpeta, incluidas subcarpetas."""
    return sorted(str(p) for p in Path(carpeta).rglob("*") if p.suffix.lower() == ".csv")


def validar_csv(archivo, columnas_minimas):
    """Revisa solo la cabecera y la primera fila (los archivos pesan cientos de MB)."""
    with open(archivo, encoding="utf-8-sig", errors="replace") as f:
        cabecera, primera_fila = f.readline().strip(), f.readline().strip()
    if not primera_fila:
        raise ValueError(f"{archivo}: vacío o sin registros")
    separador = ";" if cabecera.count(";") > cabecera.count(",") else ","
    faltantes = set(columnas_minimas) - {c.strip().lower() for c in cabecera.split(separador)}
    if faltantes:
        raise ValueError(f"{archivo}: faltan las columnas {sorted(faltantes)}")
    log.info(f"==> {archivo} válido (separador '{separador}')")


def subir_csv(archivo, contenedor, prefijo, wasb_conn_id):
    """
    Sube el archivo tal cual y guarda la hora de carga en el metadato `timestamp_carga` del blob.
    overwrite=True: reprocesar la misma fecha no duplica datos.
    """
    blob = f"{prefijo}/{Path(archivo).name}"
    timestamp_carga = datetime.now(ZoneInfo("America/Lima")).isoformat(timespec="seconds")
    # max_concurrency: sube varios bloques en paralelo (archivos de cientos de MB)
    WasbHook(wasb_conn_id=wasb_conn_id).load_file(
        archivo, contenedor, blob, overwrite=True, max_concurrency=4,
        metadata={"timestamp_carga": timestamp_carga},
    )
    log.info(f"==> Cargado {archivo} -> {contenedor}/{blob} (timestamp_carga={timestamp_carga})")
    return blob
