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

# Docker compose codes
- docker compose up airflow-init: Initial load for airflow
- docker compose up: To turn on airflow
- docker compose up -d: To turn on airflow and run it in background (Recommended)
- docker compose down && docker compose up -d: Normal Restart
- docker compose down && docker compose up --build -d: Restart forcing to build docker file (Only when dockerfile is updated)

## Grupo 6 — Ingesta de Olist

### Objetivo y alcance

Preparar datos para medir entregas tardías, identificar regiones y vendedores asociados a retrasos y relacionar la puntualidad con la evaluación del cliente. Este avance ingiere archivos a Raw; la limpieza y los indicadores se desarrollarán posteriormente.

Fuente: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), histórico 2016–2018, bajo licencia [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). Se conservan los CSV sin transformar.

```text
Kaggle → descarga inicial → data/olist/ → Airflow → datalake/raw/G6/olist/
```

### Preparación y ejecución

1. Descargar el dataset y extraer en `data/olist/` estos archivos:
   - `olist_orders_dataset.csv`
   - `olist_order_items_dataset.csv`
   - `olist_customers_dataset.csv`
   - `olist_sellers_dataset.csv`
   - `olist_order_reviews_dataset.csv`
2. Iniciar Airflow siguiendo las instrucciones generales de este README. La carpeta local se monta en `/opt/airflow/data/olist/` y simula el servidor de origen.
3. Crear la conexión `azure_blob_storage_olist`, tipo `wasb`, para la cuenta `stmcdiade`. En Extra, usar `{"connection_string": "<cadena autorizada>"}`. El contenedor `datalake` debe existir. No guardar credenciales en Git.
4. En [Airflow local](http://localhost:8080), abrir `g6_last_olist`, despausarlo y usar **Trigger** para una ejecución manual. Revisar la tarea `upload_files` y sus logs.

### Operación y errores

El DAG [G6_LAST_olist.py](dags/G6_LAST_olist.py) tiene una tarea que verifica y carga los cinco archivos secuencialmente mediante [azure_upload.py](scripts/azure_upload.py).

- **Frecuencia:** diaria a las 06:00, `America/Lima` (`0 6 * * *`), para apoyar la revisión operativa matutina. Requiere Docker y Airflow activos; `catchup=False` evita recuperar todas las ejecuciones históricas.
- **Repetición:** reemplaza archivos en sus rutas fijas (`overwrite=True`), sin agregar filas. El histórico es estático: una nueva ejecución no implica datos nuevos.
- **Fallos:** un reintento tras un minuto, desde el primer archivo. Si una carga falla, las anteriores permanecen en Azure; no hay reversión del lote.
- **Logs:** inicio, origen, contenedor, destino y duración por transferencia; los fallos conservan la excepción técnica. El DAG lanza `FileNotFoundError` con la ruta si falta un archivo.

### Validación y evolución

Se probaron ejecuciones reales de éxito, reemplazo sin duplicados, archivo faltante persistente, recuperación en el reintento y contenedor inexistente. Los cinco CSV en Azure coincidieron con los originales mediante SHA-256. Autenticación, permisos, red y timeout se validaron adicionalmente con excepciones simuladas en la función, fuera de las ejecuciones del DAG.

Como evolución se propone simular entregas periódicas con registros nuevos y actualizados, y separar las cargas en tareas por archivo para aislar reintentos y configurar concurrencia según los recursos disponibles.
