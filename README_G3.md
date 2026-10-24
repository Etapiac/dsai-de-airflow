# G3 – Ingesta diaria de datos de Spotify a Azure (zona Raw)

**Grupo 3:** Johan Barrera, Aracely Chavez, Jonatan Lavado, Hans Mallma, Roger Reategui, Erick Rosas

## Problema de negocio
*SoundWave Records*, un sello musical independiente, decide en qué artistas y países invertir publicidad revisando los rankings de Spotify a mano. Esto genera decisiones tardías y, si un día no se descarga el ranking, ese dato se pierde.

Este DAG automatiza la carga diaria de esos datos a la nube, sin cambiar su contenido, para construir un histórico confiable que luego se usará en las capas Silver y Gold.

## Arquitectura
```
data/g3/incoming/  ──►  Airflow (DAG g3_spotify_raw)  ──►  Azure · contenedor datalake · raw/airflow/G3/
(servidor simulado)      valida · carga en paralelo         ├── spotify/      datos válidos
                         · registra                         ├── _rechazados/  cuarentena
                                                            └── _control/     registro de cargas
```

## Datos
| Archivo | Frecuencia | Origen (Kaggle) |
|---|---|---|
| `charts_YYYY-MM-DD.csv` | Diaria (obligatorio) | [Top Spotify Songs in 73 Countries](https://www.kaggle.com/datasets/asaniczka/top-spotify-songs-in-73-countries-daily-updated) |
| `tracks_YYYY-MM-DD.csv` | Eventual | [Spotify Dataset 1921-2020](https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks) |
| `artists_YYYY-MM-DD.csv` | Eventual | [Spotify Dataset 1921-2020](https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks) |

Los CSV no se suben al repositorio (`data/` está en `.gitignore`).

### Formato esperado
- **Ubicación:** `data/g3/incoming/`.
- **Tipo:** CSV separado por comas, codificado en UTF-8 y con encabezado en la primera fila.
- **Nombre:** `<tipo>_YYYY-MM-DD.csv`, en minúsculas y con una fecha válida (ej. `charts_2025-03-15.csv`). Esa fecha define la partición `fecha=YYYY-MM-DD` en Azure.
- **Contenido:** al menos una fila de datos y estas columnas mínimas (puede traer más; el orden no importa):

| Tipo | Columnas mínimas |
|---|---|
| `charts` | `spotify_id`, `name`, `artists`, `daily_rank`, `country`, `snapshot_date` |
| `tracks` | `id`, `name`, `popularity`, `artists`, `id_artists`, `release_date` |
| `artists` | `id`, `name`, `followers`, `genres`, `popularity` |

Los archivos que no empiezan con `charts_`, `tracks_` o `artists_` o que no terminan en `.csv` se ignoran y quedan en `incoming/`. Si empiezan con un tipo válido pero tienen la fecha mal escrita, les faltan columnas o están vacíos, se mandan a la cuarentena.

## Qué hace el DAG
Corre todos los días a las 06:00 (hora de Lima).

1. **`detectar_archivos`**: espera hasta 1 hora a que llegue el ranking del día.
2. **Un grupo por tipo de archivo** (`charts`, `tracks`, `artists`), en paralelo:
   - `validar`: nombre con fecha, columnas mínimas y que tenga filas.
   - `elegir_camino`: los válidos siguen a la carga y los inválidos van a la cuarentena.
   - `revisar_duplicados`: compara la huella MD5 con el registro y no vuelve a subir lo que ya se cargó.
   - `subir_raw` o `enviar_cuarentena` (esta última adjunta un `.motivo.txt`).
   - `mover_local`: mueve el archivo a `processed/` o `error/`.
3. **`registrar_control`**: agrega una fila por archivo en `raw/airflow/G3/_control/registro_cargas.csv` (archivo, filas, huella, estado y motivo).

Si un grupo falla, los otros siguen. Las fallas de conexión se reintentan 3 veces, cada 5 minutos.

## Cómo ejecutarlo
1. Levantar Airflow como indica el [README del curso](README.md) (`docker compose up -d`).
2. Crear en Airflow la *Connection* `azure_blob_storage` (tipo `wasb`) con el *connection string*. **Nunca subirlo al repo.**
3. Colocar los CSV en `data/g3/incoming/`.
4. Activar el DAG `g3_spotify_raw` en http://localhost:8080 y ejecutarlo.

**Prueba de errores:** copiar a `incoming/` un archivo con columnas faltantes (debe terminar en `_rechazados/`) y una copia de un archivo ya cargado (debe quedar como `omitido_duplicado` en el registro).

## Archivos del grupo
- `dags/g3_spotify_raw.py`: definición del DAG.
- `scripts/g3_spotify_utils.py`: validación, huella, registro de control y movimiento de archivos.
- Se reutiliza `scripts/azure_upload.py` del curso para subir a Azure.
