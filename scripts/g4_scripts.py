import json
import logging
import os

import boto3
import pandas as pd
import requests

log = logging.getLogger('airflow.task')

API_BASE_URL = os.environ.get("API_BASE_URL", "http://host.docker.internal:8000")
API_KEY = os.environ.get("API_KEY", "clave-dev-cambiar")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL") or None
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID") or None
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY") or None
DEFAULT_DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
BUCKET_NAME = "pozos-arena"
TXT_OBJECT_KEY = "datos_arena.txt"


def get_s3_client():
    """
    Creates an S3 client.

    Uses S3_ENDPOINT_URL when defined (e.g. local Floci emulator).
    If S3_ENDPOINT_URL is empty, boto3 talks to real AWS.
    """
    log.info(f"Creating S3 client (endpoint={S3_ENDPOINT_URL or 'AWS'})")
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def download_json_data(output_dir: str = DEFAULT_DATA_DIR) -> str:
    """
    Downloads datos_pozos.json from the FastAPI endpoint (X-API-Key auth).

    Args:
        output_dir (str): Directory where the file will be saved.

    Returns:
        str: Path of the downloaded file.
    """
    url = f"{API_BASE_URL.rstrip('/')}/datos-pozos"
    log.info(f"Downloading JSON data from {url}")

    response = requests.get(
        url,
        headers={"X-API-Key": API_KEY},
        timeout=10,
    )

    if response.status_code == 401:
        log.warning("API key invalida o ausente (401 Unauthorized)")
        raise ValueError("API key invalida o ausente (401 Unauthorized)")

    response.raise_for_status()

    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, "datos_pozos.json")
    with open(file_path, "wb") as f:
        f.write(response.content)

    log.info(f"Downloaded {len(response.content)} bytes -> {file_path}")
    return file_path


def download_txt_data(
        bucket_name: str = BUCKET_NAME,
        object_key: str = TXT_OBJECT_KEY,
        output_dir: str = DEFAULT_DATA_DIR
    ) -> str:
    """
    Downloads a TXT file from an S3 bucket (Floci local or real AWS).

    Args:
        bucket_name (str): Name of the bucket.
        object_key (str): Key of the object to download.
        output_dir (str): Directory where the file will be saved.

    Returns:
        str: Path of the downloaded file.
    """
    s3 = get_s3_client()

    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, os.path.basename(object_key))

    log.info(f"Downloading s3://{bucket_name}/{object_key} -> {file_path}")
    s3.download_file(bucket_name, object_key, file_path)

    size = os.path.getsize(file_path)
    log.info(f"Downloaded {size} bytes -> {file_path}")
    return file_path

def merge_sources_to_csv(path_file_json: str, path_file_txt: str) -> str:
    """
    Merges datos_pozos.json and datos_arena.txt into a single CSV by
    (id_pozo, fecha_hora), adding cantidad_arena_ppm to each row.

    Dirty numeric values (e.g. "", "  ", "error", "N/D") become NaN.

    Args:
        path_file_json (str): Path to the datos_pozos.json file.
        path_file_txt (str): Path to the datos_arena.txt file.

    Returns:
        str: Path of the generated datos_unificados.csv file.
    """
    log.info(f"Merging {path_file_json} + {path_file_txt}")

    with open(path_file_json, "r", encoding="utf-8") as f:
        df_pozos = pd.DataFrame(json.load(f))

    df_arena = pd.read_csv(path_file_txt, sep="|", dtype=str)

    pozos_numeric_cols = [
        "voltaje_v",
        "amperaje_a",
        "frecuencia_bomba_hz",
        "temperatura_fahrenheit",
        "vibracion_mm_s_rms",
    ]
    for col in pozos_numeric_cols:
        df_pozos[col] = pd.to_numeric(df_pozos[col], errors="coerce")

    df_arena["cantidad_arena_ppm"] = pd.to_numeric(
        df_arena["cantidad_arena_ppm"], errors="coerce"
    )
    df_arena = df_arena.rename(columns={"pozo": "id_pozo"})
    df_arena = df_arena[["id_pozo", "fecha_hora", "cantidad_arena_ppm"]]

    df_unificado = df_pozos.merge(
        df_arena, on=["id_pozo", "fecha_hora"], how="inner"
    )
    df_unificado = df_unificado.sort_values(
        by=["id_pozo", "fecha_hora"]
    ).reset_index(drop=True)

    os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)
    output_path = os.path.join(DEFAULT_DATA_DIR, "datos_unificados.csv")
    df_unificado.to_csv(output_path, index=False)

    log.info(
        f"Merged {len(df_unificado)} rows -> {output_path}"
    )
    return output_path
