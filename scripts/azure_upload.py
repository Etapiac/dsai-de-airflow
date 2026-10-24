from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging
import os
from time import perf_counter

log = logging.getLogger('airflow.task')

def upload_to_adls(
        local_file_path = "/opt/airflow/data/sample.txt",
        container_name = "airflow",
        blob_name = "raw/uploaded_sample.txt",
        wasb_conn_id = "azure_blob_storage"
        ):
    started_at = perf_counter()
    log.info(
        "==> Starting upload: source=%s container=%s blob=%s",
        local_file_path, container_name, blob_name,
    )
    try:
        if not os.path.exists(local_file_path):
            log.warning(f"==> Local file not found: {local_file_path}")
            return
        
        # Connect using the connection created in UI
        hook = WasbHook(wasb_conn_id=wasb_conn_id)    
        hook.load_file(
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )

        log.info(
            "==> Uploaded: source=%s container=%s blob=%s duration_seconds=%.2f",
            local_file_path, container_name, blob_name, perf_counter() - started_at,
        )
    except Exception:
        log.error(
            "==> Upload failed: source=%s container=%s blob=%s duration_seconds=%.2f",
            local_file_path, container_name, blob_name, perf_counter() - started_at,
            exc_info=True,
        )
        raise
