# Grupo 2 · Ingesta diaria de cartera y mora (Banco Andino)

Proyecto integrador del curso **Ingeniería de Datos**, Maestría en Ciencia de Datos e Inteligencia Artificial (UTEC).
*Banco Andino es un banco ficticio y todos los datos son sintéticos.*

## 1. Problema de negocio

El área de cobranzas del banco recibe la mora de cada agencia con días de retraso y armada
a mano. Así, los administradores de agencia actúan tarde y la cartera vencida crece.

Este proyecto automatiza la carga diaria de la información de créditos, cuotas y pagos
desde el **servidor de contingencia** hasta la zona **Raw** del Data Lake en Azure. El
servidor de contingencia es una réplica de producción, así que extraer de él no afecta la
operación del banco. En el proyecto final esta información alimentará un reporte diario
de mora por agencia para cada administrador.

La idea parte de un caso real: una extracción diaria desde un servidor de contingencia para
enviar a cada administrador de agencia un Excel con su mora y priorizar la cobranza.

## 2. Arquitectura

```
 Servidor de contingencia          Apache Airflow (Docker)          Azure Data Lake
 (simulado: data/incoming)   --->  DAG g2_utec, diario 06:00  --->  contenedor datalake
 5 CSV por día                     tarea call_upload                raw/airflow/G2/
```

| Componente | Rol | Por qué |
|---|---|---|
| Carpeta `data/incoming` | Fuente (servidor de contingencia simulado) | Una réplica no carga la base de producción en horario de atención |
| Apache Airflow 3 en Docker | Orquestador | Programa, reintenta y registra cada ejecución |
| Azure Data Lake, contenedor `datalake` | Almacenamiento, zona Raw | Barato y escalable; guarda el dato original para auditoría y reprocesos |

## 3. Datos

Cada día llegan 5 archivos CSV con la **fecha de corte** en el nombre (`AAAAMMDD`):

| Archivo | Contenido | Tipo |
|---|---|---|
| `agencies_AAAAMMDD.csv` | Agencias, región y administrador | Maestro (copia completa diaria) |
| `customers_AAAAMMDD.csv` | Clientes y agencia asignada | Maestro (copia completa diaria) |
| `loans_AAAAMMDD.csv` | Créditos desembolsados | Transaccional |
| `installments_AAAAMMDD.csv` | Cronograma de cuotas y vencimientos | Transaccional |
| `payments_AAAAMMDD.csv` | Pagos recibidos ese día | Transaccional |

Los datos se generan con `scripts/generar_datos_mora.py`, que solo usa la biblioteca estándar
de Python. Son deterministas (la misma fecha produce siempre los mismos archivos) y simulan
12 agencias con una mora de entre 1% y 6%.

Los CSV **no se versionan en Git**: la carpeta `data/` está en `.gitignore`. El lugar de los
datos es el Data Lake, y en un banco real serían información confidencial.

## 4. Pipeline: DAG `g2_utec`

Archivo: `dags/g2_utec.py`

```
data/incoming  --->  call_upload  --->  upload_to_adls  --->  datalake/raw/airflow/G2/
(5 CSV)              (tarea)            (función del curso)
```

1. La tarea `call_upload` busca los CSV en `/opt/airflow/data/incoming`.
2. Verifica que estén los 5 tipos de archivo esperados (`agencies`, `customers`, `installments`,
   `loans` y `payments`). Si falta alguno, falla indicando cuál y no carga nada.
3. Sube cada archivo, sin transformarlo, con la función del curso `scripts/azure_upload.py`
   (`upload_to_adls`), a `datalake/raw/airflow/G2/<nombre_del_archivo>`.

| Parámetro | Valor |
|---|---|
| Frecuencia | Diaria a las 06:00 (UTC-5), antes de que empiece la gestión de cobranzas |
| Zona horaria | `America/Bogota`, definida en `start_date` (misma hora que Lima) |
| Reintentos | 1, después de 1 minuto |
| `catchup` | `False` (no ejecuta días pasados) |
| Conexión | `utec_blob_storage` (tipo `wasb`) |

### Errores contemplados

| Error posible | Cómo se maneja |
|---|---|
| Falta alguno de los 5 archivos | La tarea falla indicando cuál falta y no carga nada |
| Falla de conexión con Azure | Reintento automático después de 1 minuto |
| El mismo día se carga dos veces | Se sobrescribe el archivo (`overwrite=True`), sin duplicados |
| Exposición de credenciales | La connection string vive en la conexión de Airflow, nunca en el código |

## 5. Cómo ejecutarlo

Requisitos: el entorno de Airflow del curso levantado con Docker Compose
(ver `README.md`) y acceso a la cuenta de almacenamiento del curso.

**1. Levantar Airflow**
```
docker compose up -d
```
La interfaz queda en http://localhost:8080.

**2. Crear la conexión** (solo la primera vez), en **Admin → Connections → +**:
- Connection Id: `utec_blob_storage`
- Connection Type: `wasb`
- Extra: `{"connection_string": "<connection string de la cuenta de almacenamiento>"}`

**3. Generar los datos del día anterior**
```
docker exec dsai-de-airflow-airflow-worker-1 python /opt/airflow/scripts/generar_datos_mora.py --salida /opt/airflow/data/incoming
```
Opciones: `--fecha 2026-10-21` (una fecha específica), `--dias 7` (varios días seguidos),
`--corromper columnas` o `--corromper vacio` (genera un archivo dañado, para pruebas).

**4. Ejecutar el DAG:** en Airflow, activa `g2_utec` y haz clic en **Trigger**.

**5. Verificar la zona Raw**
```
docker exec dsai-de-airflow-airflow-scheduler-1 python -c "from airflow.providers.microsoft.azure.hooks.wasb import WasbHook; [print(b) for b in WasbHook(wasb_conn_id='utec_blob_storage').get_blobs_list('datalake', prefix='raw/airflow/G2/')]"
```

## 6. Resultados

Ejecución final del 23/10/2026: tarea `call_upload` en estado **success**, 5 de 5 archivos
cargados en 16 segundos, al primer intento.

```
datalake/raw/airflow/G2/agencies_20261021.csv
datalake/raw/airflow/G2/customers_20261021.csv
datalake/raw/airflow/G2/installments_20261021.csv
datalake/raw/airflow/G2/loans_20261021.csv
datalake/raw/airflow/G2/payments_20261021.csv
```

## 7. Control de versiones (Gitflow)

| Rama | Uso |
|---|---|
| `main` | Versión estable |
| `develop` | Integración del trabajo de los grupos |
| `feature/dag-g2-mora-MFRO` | Rama de trabajo del grupo 2, creada desde `develop` |

El trabajo se integra a `develop` mediante el **Pull Request #64**, revisado antes del merge.

Archivos del grupo 2 en este repositorio:

```
dags/g2_utec.py                  DAG de ingesta
scripts/generar_datos_mora.py    generador de datos sintéticos
README_G2.md                     este documento
```

## 8. Desafíos y aprendizajes

| Desafío | Cómo se resolvió |
|---|---|
| La conexión `utec_blob_storage` no existía | Se creó en Airflow, tipo `wasb`, con la connection string |
| Azure respondió `AccountIsDisabled` | El log mostró que el error era de la cuenta, no del código; se obtuvo una cadena nueva |
| Sin cuenta de Azure activa para probar | Se probó antes con Azurite (emulador local); la carga final fue en Azure real |

## 9. Próximos pasos (proyecto final)

- **Validación:** revisar la fecha, que los archivos no vengan vacíos y que tengan las columnas esperadas.
- **Silver:** clientes, créditos, cuotas y pagos limpios y tipados.
- **Gold:** `mora_por_agencia` (diaria), `cartera_por_tramo` (1–30, 31–60, 61–90 y más de 90 días) y `clientes_en_riesgo`.
- **Data product:** reporte diario de mora enviado a cada administrador de agencia.
