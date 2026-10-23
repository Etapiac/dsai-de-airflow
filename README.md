# APACHE AIRFLOW WITH DOCKER
- Airflow official documentation
Steps: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- Donwload yaml file indicated in Airflow official documentation
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.0/docker-compose.yaml'
- Create folders for the volumes:
mkdir ./dags ./logs ./config ./plugings
- Setting airflow user
    - For Linux - MAC
    echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
    - For Windows, ignore the warning, or manually we can create an .env file with value:
    AIRFLOW_UID=50000
- Initialize Database (It will create the first user account)
docker compose up airflow-init
(At the end we should see Messages like: User "airflow" created with role "Admin" \n 3.0.0 \n airflow-init_1 exited with code 0)
- Run Airflow
docker compose up
- In a second terminal we can check all the conditions of the containers, by running
docker ps
- To interact with one of the container, just use the docker exec [CONTAINER ID] [Comand Line Interface]
docker exec 0c04f0e03aa8 airflow version

- Adding requirements file to docker airflow
    - Comment the image and add: "build: ." bellow it
    #image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.0.0}
    build: .
    - Create "Dockerfile" in the same folder your docker-compose.yaml file is with content similar to:
    FROM apache/airflow:3.0.0
    ADD requirements.txt .
    RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
    - Create a requirements.txt file in the same folder of the docker-compose.yaml
    - Now we can restart and build:
    docker compose down && docker compose up --build -d

- Adding data volume to the yaml
    - Go to the docker-compose.yaml
    - Locate the volumns part and add the volume
    "- ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data"
    - Create the data folder (at the same level of dags):
    mkdir ./data
    - restart the volume (No need to use build here)
    docker compose down && docker compose up -d

- Note, each task executes acctions (Behind the scenes, each DAG is an operator):
    - Python Function: pythonoperator
    - Bash Command: bashoperator
    - Inser data into a DB: postgresoperator
(Depending in the action we want to trigger from our dag, we will use the corresponding operator)
Operator: It is an object encapsulating the job we want to run in our dag
(Each time we are adding an operator in our dag, we are adding a dag)

Examples:
1. Create my_dag.py inside ./dags
2. You will automatically see the DAG in Airflow UI: http://localhost:8080

# Connecting to Azure Blob Storage
Tutorial: https://www.astronomer.io/docs/learn/connections/azure-blob-storage?tab=shared-access-key#azure-blob-storage
Get Account Keys: https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys

0. Add connection to Azure in Airflow
1. In Airflow, go to admin
2. Go to connection
3. Click in Add conneciton
4. Define a connection_id: azure_blob_storage
5. Connection Type: wasb
6. In Extra files JSON, copy the Blob Storage Connection String (IT is located below Key1)
{"connection_string": <copy_the_connection_string>}
(Make sure there are no other connections from Azure or it would generate warnings)

# Isolating python scripts in Airflow Dags
1. Add Python Path to Airflow in the yaml (To be able to import scripts)
    environment:
        PYTHONPATH: '/opt/airflow'

# Avoid loading samples in Airflow
1. Change in yaml:
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'

# DAG g4_BIEPC_parcial (G4 - Proyecto Parcial)

Este DAG corresponde al avance del proyecto parcial del grupo 4. Se ejecuta cada 15 minutos en UTC con la expresión cron `*/15 * * * *` y tiene `catchup=False`.

## Flujo de trabajo
1. `task_download_json_data`: descarga `datos_pozos.json` desde la API FastAPI (`GET /datos-pozos`) usando autenticación `X-API-Key`.
2. `task_download_txt_data`: descarga `datos_arena.txt` desde el bucket S3 `pozos-arena`.
3. `task_merge_sources_to_csv`: une ambas fuentes por `id_pozo` y `fecha_hora`, convierte los valores numéricos y genera `datos_unificados.csv` en `/opt/airflow/data`.
4. `upload_azure_datalake`: sube los tres archivos a Azure Blob Storage, contenedor `datalake`, en las rutas:
   - `raw/G4/pozos`
   - `raw/G4/arena`
   - `raw/G4/pozos_arena`

Los blobs se nombran como `uploaded_<nombre_archivo>_<YYYYMMDD>.<extensión>` y se cargan con `overwrite=True`. Como el sufijo solo incluye la fecha, las ejecuciones del mismo día sobrescriben el mismo blob.

## Configuración requerida
- Crear el archivo `.env` a partir de `.env.sample` y reemplazar los valores de ejemplo:
  ```bash
  cp .env.sample .env
  ```
- Configurar la API FastAPI mediante `API_BASE_URL` y `API_KEY`.
- Configurar S3 mediante `S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` y `AWS_DEFAULT_REGION`. Se puede usar el emulador local Floci o AWS real.
- Crear en Airflow la conexión WASB `azure_blob_storage_2` y verificar que exista el contenedor `datalake`.
- Las dependencias `requests`, `boto3` y `pandas` están declaradas en `requirements.txt`.

## Archivos agregados
- `scripts/g4_scripts.py`: descarga de las fuentes JSON y TXT, y unión en CSV.
- `dags/services/azure_service.py`: subida de archivos a Azure Blob Storage.
- `dags/helpers/file_helper.py`: validaciones de archivos y generación del sufijo de fecha.
- `.env.sample`: variables de entorno necesarias para API, S3 y Airflow.
- `requirements.txt`: dependencias de Python del DAG.
- `docker-compose.yaml`: variables de entorno, `extra_hosts` para `host.docker.internal`, volumen de `scripts` y `PYTHONPATH`.

## Ejecución y verificación
Reconstruir y reiniciar los contenedores después de agregar dependencias o variables de entorno:
```bash
docker compose down && docker compose up --build -d
```

# Docker compose codes
- docker compose up airflow-init: Initial load for airflow
- docker compose up: To turn on airflow
- docker compose up -d: To turn on airflow and run it in background (Recommended)
- docker compose down && docker compose up -d: Normal Restart
- docker compose down && docker compose up --build -d: Restart forcing to build docker file (Only when dockerfile is updated)