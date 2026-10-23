# Grupo 1 (AJREAR): ingesta de mediciones de medidores industriales

**Integrantes:** Rodrigo Zambrano, Enrique Huasupoma, Alvaro Colquicocha, Jair Hurtado, Alex Valdez, Ruben Coquil

## Contexto

La empresa tiene medidores en ~20 000 a 25 000 clientes industriales. Cada medidor registra la energía cada 15 minutos (96 lecturas por día) y el servidor de lecturas entrega extracciones en CSV de cientos de MB. Este DAG automatiza la primera etapa: copiar esos CSV, sin transformarlos, a la zona **Raw** de Azure Data Lake. Las capas Silver y Gold quedan para el proyecto final.

## Archivos de origen (muestra marzo 2025)

| Archivo | Separador | Filas | Tamaño |
|---|---|---|---|
| `202503 Clientes Libres LDS.csv` | `;` | 3 228 960 | 209 MB |
| `202503 maximetros LDS_lote_0.csv` | `,` | 10 723 882 | 674 MB |

Columnas mínimas: `periodo, numero_cliente, fecha, hora, kwh_del_int`.

## Estructura

```text
dags/g1_AJREAR_parcial.py   # DAG g1_ajrear_parcial
scripts/g1_mediciones.py    # listar, validar y subir los CSV
data/G1/mediciones/         # "servidor" simulado (fuera de git por .gitignore)
```

## Flujo del DAG

```text
listar_archivos -> validar_archivo[n] -> cargar_archivo[n]
```

| Tarea | Qué hace |
|---|---|
| `listar_archivos` | Busca todos los `*.csv` en `data/G1/mediciones` (incluye subcarpetas). |
| `validar_archivo` | Una tarea por archivo: revisa que tenga registros y las columnas mínimas (solo lee la cabecera y la primera fila). |
| `cargar_archivo` | Una tarea por archivo, en paralelo: sube el CSV original a `datalake/raw/airflow/G1/mediciones/fecha_carga=YYYY-MM-DD/` y guarda la hora de carga en el metadato del blob `timestamp_carga` (ej. `2026-10-26T06:00:05-05:00`). |

**Frecuencia:** semanal, lunes 06:00 hora de Lima (`0 6 * * 1`).

## Manejo de errores

| Situación | Comportamiento |
|---|---|
| No hay CSV en la carpeta | La corrida queda como *skipped*. |
| CSV vacío, sin registros o sin columnas mínimas | Ese archivo no se sube y la corrida termina en *failed*; los demás archivos sí se suben. |
| Falla de red o de Azure | `cargar_archivo` se reintenta 3 veces con espera exponencial (máx. 2 h por intento). |
| Se reprocesa la misma fecha | Se sobrescribe el mismo blob (`overwrite=True`): no se duplican datos. |

## Cómo ejecutarlo

1. Copiar los CSV a `data/G1/mediciones/`.
2. Levantar Airflow: `docker compose up --build -d` y entrar a http://localhost:8080.
3. En Admin → Connections → Add: `utec_blob_storage`, tipo `wasb`, y en *Extra* el JSON `{"connection_string": "..."}` que entregó el profesor. La cadena de conexión **nunca** se sube al repositorio.
4. Activar y ejecutar el DAG `g1_ajrear_parcial`.
5. Revisar en Azure: contenedor `datalake`, ruta `raw/airflow/G1/mediciones/`.

## Gitflow

`main` ← `develop` ← `feature/proyecto-parcial-g1-AJREAR`, integrado con un Pull Request hacia `develop`.
