import logging
import os

import boto3
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
    log.info(f"==> Creating S3 client (endpoint={S3_ENDPOINT_URL or 'AWS'})")
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
    log.info(f"==> Downloading JSON data from {url}")

    response = requests.get(
        url,
        headers={"X-API-Key": API_KEY},
        timeout=10,
    )

    if response.status_code == 401:
        log.warning("==> API key invalida o ausente (401 Unauthorized)")
        raise ValueError("API key invalida o ausente (401 Unauthorized)")

    response.raise_for_status()

    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, "datos_pozos.json")
    with open(file_path, "wb") as f:
        f.write(response.content)

    log.info(f"==> Downloaded {len(response.content)} bytes -> {file_path}")
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

    log.info(f"==> Downloading s3://{bucket_name}/{object_key} -> {file_path}")
    s3.download_file(bucket_name, object_key, file_path)

    size = os.path.getsize(file_path)
    log.info(f"==> Downloaded {size} bytes -> {file_path}")
    return file_path
