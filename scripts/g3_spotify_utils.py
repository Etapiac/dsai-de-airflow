"""Funciones de apoyo del DAG g3_spotify_raw (Grupo 3)."""
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from pendulum import timezone
from datetime import datetime
import pandas as pd
import hashlib
import io
import os
import shutil

# Columnas minimas que debe traer cada tipo de archivo
COLUMNAS_REQUERIDAS = {
    "charts": ["spotify_id", "name", "artists", "daily_rank", "country", "snapshot_date"],
    "tracks": ["id", "name", "popularity", "artists", "id_artists", "release_date"],
    "artists": ["id", "name", "followers", "genres", "popularity"],
}


def listar_archivos(carpeta, tipo):
    """Devuelve los CSV de la carpeta que empiezan con el tipo (ej. charts_...)."""
    if not os.path.exists(carpeta):
        return []
    archivos = [a for a in os.listdir(carpeta) if a.startswith(tipo + "_") and a.endswith(".csv")]
    return sorted(archivos)


def validar_archivo(carpeta, archivo, tipo):
    """Revisa nombre, columnas y que tenga filas. No modifica el archivo."""
    ruta = os.path.join(carpeta, archivo)
    with open(ruta, "rb") as f:
        huella = hashlib.md5(f.read()).hexdigest()  # mismo contenido = misma huella

    resultado = {"archivo": archivo, "ruta": ruta, "tipo": tipo, "fecha": "",
                 "filas": 0, "huella": huella, "valido": False, "motivo": ""}

    # 1. El nombre debe ser tipo_YYYY-MM-DD.csv
    fecha = archivo.replace(tipo + "_", "").replace(".csv", "")
    try:
        datetime.strptime(fecha, "%Y-%m-%d")
    except ValueError:
        resultado["motivo"] = f"El nombre no sigue el formato {tipo}_YYYY-MM-DD.csv"
        return resultado
    resultado["fecha"] = fecha

    # 2. Debe poder leerse, tener las columnas minimas y al menos una fila
    try:
        df = pd.read_csv(ruta)
    except Exception as e:
        resultado["motivo"] = f"No se pudo leer el CSV: {e}"
        return resultado

    resultado["filas"] = len(df)
    faltantes = [c for c in COLUMNAS_REQUERIDAS[tipo] if c not in df.columns]
    if faltantes:
        resultado["motivo"] = "Faltan columnas: " + ", ".join(faltantes)
    elif len(df) == 0:
        resultado["motivo"] = "El archivo no tiene filas"
    else:
        resultado["valido"] = True
    return resultado


def registro(resultado, estado, destino=""):
    """Fila del registro de control para un archivo."""
    return {
        "fecha_hora": datetime.now(timezone("America/Lima")).strftime("%Y-%m-%d %H:%M:%S"),
        "archivo": resultado["archivo"],
        "tipo": resultado["tipo"],
        "filas": resultado["filas"],
        "huella": resultado["huella"],
        "estado": estado,
        "motivo": resultado["motivo"],
        "destino": destino,
    }


def leer_registro(wasb_conn_id, container_name, blob_name):
    hook = WasbHook(wasb_conn_id=wasb_conn_id)
    if not hook.check_for_blob(container_name, blob_name):
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(hook.read_file(container_name, blob_name)))


def huellas_cargadas(wasb_conn_id, container_name, blob_name):
    """Huellas de los archivos que ya se subieron en ejecuciones anteriores."""
    df = leer_registro(wasb_conn_id, container_name, blob_name)
    if df.empty:
        return []
    return df[df["estado"] == "cargado"]["huella"].tolist()


def agregar_al_registro(wasb_conn_id, container_name, blob_name, registros):
    df = pd.concat([leer_registro(wasb_conn_id, container_name, blob_name), pd.DataFrame(registros)])
    guardar_texto(wasb_conn_id, container_name, blob_name, df.to_csv(index=False))


def guardar_texto(wasb_conn_id, container_name, blob_name, texto):
    WasbHook(wasb_conn_id=wasb_conn_id).load_string(
        texto, container_name=container_name, blob_name=blob_name, overwrite=True
    )


def mover(ruta, carpeta_destino):
    os.makedirs(carpeta_destino, exist_ok=True)
    shutil.move(ruta, os.path.join(carpeta_destino, os.path.basename(ruta)))
