
from helpers.file_helper import exists_file, is_allowed_file_extension, get_date_suffix_in_filename
import logging

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

log = logging.getLogger('upload_to_azure.task')

def upload_to_azure(
        local_file_path: str,
        container_name: str = "airflow",
        destination_path: str = "raw",
        wasb_conn_id: str = "azure_blob_storage"
    ) -> None:
    """
    Uploads a local file to Azure Blob Storage.
    
    Args:
        local_file_path (str): The path to the local file to upload.
        container_name (str): The name of the Azure Blob Storage container.
        destination_path (str): The path where the file will be stored in Azure Blob Storage.
    """

    # validate the local file path
    if not exists_file(local_file_path):
        log.warning(f"File does not exist: {local_file_path}")
        raise

    # validate the file extension
    if not is_allowed_file_extension(local_file_path, allowed_extensions=['sql', 'txt', 'csv']):
        log.warning(f"File extension is not allowed: {local_file_path}")
        raise

    # get the new file name with date suffix
    new_file_name = get_date_suffix_in_filename(local_file_path)

    # construct the blob name with the destination path and new file name (BLOB_NAME)
    blob_name = f"{destination_path}/uploaded_{new_file_name}"

    log.info(f"Uploading {local_file_path} to Azure Blob Storage as {blob_name} in container {container_name}...")
    try:
        # Connect using the connection created in UI  to the WasbHook
        hook = WasbHook(wasb_conn_id=wasb_conn_id)
        hook.load_file(
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )

        log.info(f"Successfully uploaded {local_file_path} to Azure Blob Storage as {blob_name} in container {container_name}.")
    except Exception as e:
        log.error(f"Failed to upload {local_file_path} to Azure Blob Storage as {blob_name} in container {container_name}. Error: {str(e)}", exc_info=True)
        raise