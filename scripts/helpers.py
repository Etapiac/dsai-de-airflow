from datetime import datetime
import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

FILAS_ESPERADAS = 1030
PLANTAS_ESPERADAS = 12

COLUMNAS_ESPERADAS = [
    "id_ensayo",
    "planta_id",
    "planta_nombre",
    "ciudad",
    "departamento",
    "cemento_kg_m3",
    "escoria_alto_horno_kg_m3",
    "ceniza_volante_kg_m3",
    "agua_kg_m3",
    "superplastificante_kg_m3",
    "agregado_grueso_kg_m3",
    "agregado_fino_kg_m3",
    "edad_dias",
    "resistencia_compresion_mpa",
]

def add_date_suffix(filename, date=None):
    """
    Appends _YYYYMMDD to the filename before the extension.
    
    Args:
        filename (str): Original filename (e.g. 'data.csv')
        date (datetime, optional): Date to use; defaults to today.
    
    Returns:
        str: Modified filename (e.g. 'data_20250501.csv')
    """
    if date is None:
        date = datetime.today()
        
    name, ext = os.path.splitext(filename)
    date_str = date.strftime("%Y%m%d")
    return f"{name}_{date_str}{ext}"

def leer_datos(path):
    return pd.read_csv(path)

def transformar_datos(df):
    df['total'] = df['cantidad'] * df['precio']
    return df

def resumir_datos(df):
    resumen = df.groupby('producto')['total'].sum().reset_index()
    return resumen

def guardar_datos(df, path):
    df.to_csv(path, index=False)


def ruta_trabajo_parcial(archivo_salida: str | Path) -> Path:
    """Ruta temporal de la corrida, junto al consolidado definitivo."""
    salida = Path(archivo_salida)
    return salida.with_suffix(salida.suffix + ".parcial")


def eliminar_trabajo_procesado(*rutas: str | Path) -> list[str]:
    """Borra archivos generados por una corrida que no puede continuar.

    Solo elimina las rutas recibidas. No borra los CSV que cada planta dejó
    en el servidor central.
    """
    eliminados = []
    for ruta in rutas:
        archivo = Path(ruta)
        if not archivo.exists():
            logger.warning("==> No hay trabajo procesado para eliminar: %s", archivo)
            continue
        if not archivo.is_file():
            logger.error("==> Error crítico: %s no es un archivo y no se elimina.", archivo)
            continue
        try:
            archivo.unlink()
        except OSError as error:
            logger.error("==> Error crítico: no se pudo eliminar %s. %s", archivo, error)
            raise
        eliminados.append(str(archivo))
        logger.error("==> Trabajo procesado eliminado: %s", archivo)
    return eliminados


def _airflow_reintentara() -> bool:
    """True si esta ejecución de la tarea todavía tiene un reintento pendiente.

    Con retries=1, el primer intento tiene try_number <= max_tries y Airflow
    lo vuelve a programar. El segundo intento ya no es elegible.
    """
    from airflow.sdk import get_current_context

    try:
        tarea = get_current_context()["ti"]
    except Exception:
        return False
    operador = getattr(tarea, "task", None)
    reintentos = getattr(operador, "retries", 0) or 0
    return bool(reintentos and tarea.try_number <= tarea.max_tries)


def abortar_corrida(etapa: str, error: Exception, *archivos_a_eliminar: str) -> None:
    """Registra el fallo y lo relanza para que Airflow aplique su reintento.

    El primer fallo no borra el consolidado ya publicado: el reintento, a los
    5 minutos, tiene que poder volver a leerlo. El archivo parcial sí se
    descarta en cada intento. Si el reintento también falla, se borra el
    trabajo de la corrida y la tarea queda en failed.
    """
    logger.error("==> Error crítico en %s: %s", etapa, error)
    if _airflow_reintentara():
        logger.error(
            "==> Airflow reintentará la tarea 1 vez en 5 minutos. "
            "Se conserva el consolidado publicado."
        )
        parciales = [ruta for ruta in archivos_a_eliminar if str(ruta).endswith(".parcial")]
        if parciales:
            eliminar_trabajo_procesado(*parciales)
    else:
        logger.error(
            "==> Reintentos agotados. El DAG termina. "
            "Se elimina el trabajo procesado de esta corrida."
        )
        eliminar_trabajo_procesado(*archivos_a_eliminar)
    raise error


def listar_csv_plantas(directorio: str | Path) -> list[str]:
    """Recorre el servidor central y sus subcarpetas y devuelve los CSV.

    El orden es alfabético para que dos ejecuciones con los mismos archivos
    produzcan el mismo consolidado.
    """
    raiz = Path(directorio)
    if not raiz.is_dir():
        mensaje = (
            f"No existe el servidor central: {raiz}. "
            "Cada planta debe dejar su CSV en una subcarpeta."
        )
        logger.error("==> Error crítico: %s", mensaje)
        raise FileNotFoundError(mensaje)

    archivos = sorted(
        ruta
        for ruta in raiz.rglob("*.csv")
        if ruta.is_file() and not ruta.name.startswith("_") and not ruta.name.endswith(".parcial")
    )
    if not archivos:
        mensaje = f"No hay CSV dentro de las subcarpetas de {raiz}."
        logger.error("==> Error crítico: %s", mensaje)
        raise FileNotFoundError(mensaje)

    logger.info("Se encontraron %s CSV en %s", len(archivos), raiz)
    return [str(ruta) for ruta in archivos]


def consolidar_plantas(rutas: list[str], archivo_salida: str | Path) -> dict:
    """Lee los CSV de las plantas y escribe un único archivo consolidado.

    Agrega ``archivo_origen`` para saber de qué subcarpeta salió cada fila.
    No imputa nulos ni borra duplicados del laboratorio: si el mismo
    ``id_ensayo`` aparece dos veces al unir, la tarea falla. El conteo de
    plantas y de filas lo rechaza ``validar_consolidado``.
    """
    if not rutas:
        mensaje = "La lista de archivos de planta está vacía."
        logger.error("==> Error crítico: %s", mensaje)
        raise ValueError(mensaje)

    salida = Path(archivo_salida)
    parcial = ruta_trabajo_parcial(salida)
    publicado = False
    try:
        marcos = []
        columnas_referencia = None

        for ruta in rutas:
            try:
                marco = pd.read_csv(ruta)
            except Exception as error:
                logger.error("==> Error crítico al leer %s: %s", ruta, error)
                raise
            if marco.empty:
                mensaje = f"El archivo de planta está vacío: {ruta}"
                logger.error("==> Error crítico: %s", mensaje)
                raise ValueError(mensaje)

            columnas = list(marco.columns)
            faltantes = [columna for columna in COLUMNAS_ESPERADAS if columna not in columnas]
            if faltantes:
                mensaje = f"{ruta} no trae las columnas {faltantes}."
                logger.error("==> Error crítico: %s", mensaje)
                raise ValueError(mensaje)
            if columnas_referencia is None:
                columnas_referencia = columnas
            elif columnas != columnas_referencia:
                mensaje = (
                    f"Esquema distinto en {ruta}. "
                    f"Esperado {columnas_referencia}, llegó {columnas}."
                )
                logger.error("==> Error crítico: %s", mensaje)
                raise ValueError(mensaje)

            origen = Path(ruta)
            marco = marco.copy()
            marco["archivo_origen"] = f"{origen.parent.name}/{origen.name}"
            marcos.append(marco)

        consolidado = pd.concat(marcos, ignore_index=True)
        repetidos = consolidado.loc[consolidado["id_ensayo"].duplicated(), "id_ensayo"]
        if not repetidos.empty:
            muestra = repetidos.astype(int).tolist()[:10]
            mensaje = f"id_ensayo repetido al consolidar: {muestra}"
            logger.error("==> Error crítico: %s", mensaje)
            raise ValueError(mensaje)

        plantas = sorted(consolidado["planta_id"].dropna().astype(str).unique().tolist())
        consolidado = consolidado.sort_values(["planta_id", "id_ensayo"]).reset_index(drop=True)
        salida.parent.mkdir(parents=True, exist_ok=True)
        # Escritura directa: os.replace del .parcial falla con ENOENT en el
        # volumen de Docker (data/ -> /opt/airflow/data).
        publicado = True
        consolidado.to_csv(salida, index=False)
        if parcial.exists():
            parcial.unlink()

        resumen = {
            "archivo": str(salida),
            "filas": int(len(consolidado)),
            "plantas": plantas,
            "archivos_leidos": len(rutas),
        }
        logger.info(
            "Consolidado escrito en %s (%s filas, %s plantas)",
            salida,
            resumen["filas"],
            len(plantas),
        )
        return resumen
    except Exception as error:
        logger.error(
            "==> Error crítico al consolidar. Se descarta el trabajo de esta corrida. %s",
            error,
        )
        if parcial.exists():
            eliminar_trabajo_procesado(parcial)
        if publicado:
            eliminar_trabajo_procesado(salida)
        raise


def validar_consolidado(resumen: dict) -> dict:
    """Vuelve a leer el archivo escrito y confirma 1 030 filas y 12 plantas.

    Si una planta no dejó su CSV, ``consolidar_plantas`` igual publica el
    archivo y esta función es la que falla.
    """
    archivo = Path(resumen["archivo"])
    if not archivo.is_file():
        mensaje = f"No se escribió el consolidado: {archivo}"
        logger.error("==> Error crítico: %s", mensaje)
        raise FileNotFoundError(mensaje)

    try:
        verificacion = pd.read_csv(archivo)
    except Exception as error:
        logger.error("==> Error crítico al releer el consolidado %s: %s", archivo, error)
        raise
    if len(verificacion) != int(resumen["filas"]):
        mensaje = (
            "El archivo consolidado no coincide con el conteo de la tarea anterior. "
            f"Disco={len(verificacion)} resumen={resumen['filas']}"
        )
        logger.error("==> Error crítico: %s", mensaje)
        raise ValueError(mensaje)
    plantas = sorted(verificacion["planta_id"].dropna().astype(str).unique().tolist())
    if len(plantas) != PLANTAS_ESPERADAS:
        mensaje = (
            f"Faltan plantas: se esperaban {PLANTAS_ESPERADAS} "
            f"y el consolidado tiene {len(plantas)}: {plantas}"
        )
        logger.error("==> Error crítico: %s", mensaje)
        raise ValueError(mensaje)
    if len(verificacion) != FILAS_ESPERADAS:
        mensaje = (
            f"El archivo consolidado tiene {len(verificacion)} filas, no {FILAS_ESPERADAS}."
        )
        logger.error("==> Error crítico: %s", mensaje)
        raise ValueError(mensaje)

    logger.info("Consolidado válido: %s", archivo)
    return resumen
