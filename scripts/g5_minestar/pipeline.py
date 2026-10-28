"""Funciones del pipeline G5: detectar, validar, normalizar, cargar a Raw y calcular KPI diarios."""
import logging
import os
from datetime import date, datetime, timedelta

import pandas as pd
import pendulum

from scripts.g5_minestar import config as cfg

log = logging.getLogger("airflow.task")


class DatosInvalidosError(ValueError):
    """El archivo del día no cumple con lo esperado; el DAG debe fallar con un mensaje claro."""


def ventana_dia_operativo(fecha_operativa: date) -> tuple[datetime, datetime]:
    """Devuelve [inicio, fin) del día operativo: de 07:00 del día a 07:00 del día siguiente."""
    inicio = datetime.combine(fecha_operativa, datetime.min.time()) + timedelta(hours=cfg.DAY_START_HOUR)
    return inicio, inicio + timedelta(days=1)


def resolver_fecha_operativa(fecha_param: str | None, fecha_ejecucion: datetime) -> date:
    """
    Fecha del día operativo a procesar.

    Si se pasa `fecha_param` (YYYY-MM-DD) se usa esa (reprocesos manuales). Si no, se toma el
    último día operativo cerrado respecto de la fecha de ejecución, en hora de Lima.
    """
    if fecha_param:
        try:
            return date.fromisoformat(fecha_param.strip())
        except ValueError as e:
            raise DatosInvalidosError(f"Parámetro fecha_operativa inválido: '{fecha_param}' (use YYYY-MM-DD)") from e
    ahora = pendulum.instance(fecha_ejecucion).in_timezone(cfg.TIMEZONE)
    return (ahora - timedelta(hours=cfg.DAY_START_HOUR)).date() - timedelta(days=1)


def ruta_archivo_fuente(fecha_operativa: date) -> str:
    """Ruta del archivo del día en el servidor simulado: servidor/AAAA/MM/demoras_camiones_AAAAMMDD.csv."""
    nombre = cfg.SOURCE_FILE_TEMPLATE.format(yyyymmdd=fecha_operativa.strftime("%Y%m%d"))
    return os.path.join(cfg.SERVER_DIR, fecha_operativa.strftime("%Y"), fecha_operativa.strftime("%m"), nombre)


def detectar_archivo(fecha_operativa: date) -> str:
    """Verifica que MineStar haya dejado el archivo del día y que no esté vacío."""
    ruta = ruta_archivo_fuente(fecha_operativa)
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No llegó el reporte de demoras del {fecha_operativa}: {ruta}")
    if os.path.getsize(ruta) == 0:
        raise DatosInvalidosError(f"El reporte de demoras del {fecha_operativa} está vacío: {ruta}")
    log.info(f"==> Archivo encontrado: {ruta} ({os.path.getsize(ruta) / 1024:.1f} KB)")
    return ruta


def leer_reporte(ruta: str) -> pd.DataFrame:
    """Lee el CSV de MineStar conservando todo como texto (el tipado se hace al validar)."""
    return pd.read_csv(ruta, encoding="utf-8-sig", dtype=str, keep_default_na=False)


def validar_reporte(ruta: str, fecha_operativa: date) -> dict:
    """
    Controles de calidad de ingreso. Falla si cambia el esquema, si no hay filas o si las
    fechas no se pueden leer; solo advierte por duplicados, categorías nuevas o demoras abiertas.
    """
    df = leer_reporte(ruta)
    faltantes = [c for c in cfg.SOURCE_COLUMNS if c not in df.columns]
    if faltantes:
        raise DatosInvalidosError(f"Cambió el esquema de MineStar, faltan columnas: {faltantes}")
    if df.empty:
        raise DatosInvalidosError(f"El reporte del {fecha_operativa} no tiene filas")

    inicio = pd.to_datetime(df["Inicio Demora"], format=cfg.DATETIME_FORMAT, errors="coerce")
    abiertas = df["Fin Demora"].eq(cfg.ACTIVE_DELAY_TEXT)
    fin = pd.to_datetime(df["Fin Demora"].where(~abiertas), format=cfg.DATETIME_FORMAT, errors="coerce")
    fechas_malas = inicio.isna() | (fin.isna() & ~abiertas)
    if fechas_malas.any():
        raise DatosInvalidosError(
            f"{fechas_malas.sum()} filas con fecha ilegible, p. ej. OID {df.loc[fechas_malas, 'OID Minestar'].iloc[0]}"
        )
    if (fin < inicio).any():
        raise DatosInvalidosError(f"{(fin < inicio).sum()} demoras terminan antes de empezar")

    resumen = {
        "filas": int(len(df)),
        "equipos": int(df["Equipo"].nunique()),
        "duplicados": int(df["OID Minestar"].duplicated().sum()),
        "demoras_abiertas": int(abiertas.sum()),
        "categorias_nuevas": sorted(set(df["Categoría Demora"]) - set(cfg.TIME_MODEL)),
        "equipos_fuera_de_maestro": sorted(set(df["Equipo"]) - set(cfg.TRUCK_TO_SERIES)),
    }
    for clave in ("duplicados", "demoras_abiertas", "categorias_nuevas", "equipos_fuera_de_maestro"):
        if resumen[clave]:
            log.warning(f"==> {clave}: {resumen[clave]}")
    log.info(f"==> Validación OK: {resumen}")
    return resumen


def normalizar_reporte(ruta: str, fecha_operativa: date, fecha_carga: datetime) -> str:
    """
    Transformación mínima antes de subir a Raw:
    elimina duplicados, recorta cada demora a la ventana del día operativo (07:00-07:00),
    cierra las demoras abiertas al final de la ventana, clasifica según el modelo de tiempos
    y agrega columnas de linaje. Conserva las columnas originales de MineStar.
    """
    df = leer_reporte(ruta).drop_duplicates(subset="OID Minestar", keep="last")
    v_ini, v_fin = ventana_dia_operativo(fecha_operativa)

    df["demora_activa"] = df["Fin Demora"].eq(cfg.ACTIVE_DELAY_TEXT)
    inicio = pd.to_datetime(df["Inicio Demora"], format=cfg.DATETIME_FORMAT)
    fin = pd.to_datetime(df["Fin Demora"].where(~df["demora_activa"]), format=cfg.DATETIME_FORMAT).fillna(v_fin)

    df["inicio_original"] = inicio
    df["fin_original"] = fin.where(~df["demora_activa"])
    df["inicio_dia"] = inicio.clip(lower=v_ini)
    df["fin_dia"] = fin.clip(upper=v_fin)
    df = df[df["fin_dia"] > df["inicio_dia"]].copy()   # descarta lo que no toca el día
    df["duracion_h_dia"] = ((df["fin_dia"] - df["inicio_dia"]).dt.total_seconds() / 3600).round(4)
    df["grupo_tiempo"] = df["Categoría Demora"].map(cfg.TIME_MODEL).fillna("SIN_CLASIFICAR")
    df["serie"] = df["Equipo"].map(cfg.TRUCK_TO_SERIES).fillna("SIN_SERIE")
    df["fecha_operativa"] = fecha_operativa.isoformat()
    df["archivo_origen"] = os.path.basename(ruta)
    df["fec_carga"] = fecha_carga.strftime("%Y-%m-%d %H:%M:%S")

    df = df.rename(columns={
        "OID Minestar": "oid_minestar", "Flota": "flota", "Equipo": "equipo",
        "Categoría Demora": "categoria", "Subcategoría Demora": "subcategoria",
        "Descripción Demora": "descripcion", "Duración": "duracion_minestar", "ID Datamart": "id_datamart",
    }).drop(columns=["Inicio Demora", "Fin Demora"])

    columnas = [
        "fecha_operativa", "oid_minestar", "id_datamart", "flota", "serie", "equipo", "categoria",
        "subcategoria", "descripcion", "grupo_tiempo", "inicio_original", "fin_original", "duracion_minestar",
        "inicio_dia", "fin_dia", "duracion_h_dia", "demora_activa", "archivo_origen", "fec_carga",
    ]
    os.makedirs(cfg.RAW_READY_DIR, exist_ok=True)
    salida = os.path.join(cfg.RAW_READY_DIR, os.path.basename(ruta))
    df.sort_values(["equipo", "inicio_dia"])[columnas].to_csv(salida, index=False, encoding="utf-8")
    log.info(f"==> Normalizado: {len(df)} demoras, {df['duracion_h_dia'].sum():.1f} h -> {salida}")
    return salida


def blob_destino(fecha_operativa: date) -> str:
    """Ruta en el Data Lake, particionada por año y mes."""
    nombre = cfg.SOURCE_FILE_TEMPLATE.format(yyyymmdd=fecha_operativa.strftime("%Y%m%d"))
    return f"{cfg.RAW_PREFIX}/anio={fecha_operativa:%Y}/mes={fecha_operativa:%m}/{nombre}"


def cargar_a_raw(ruta_local: str, fecha_operativa: date) -> str:
    """Sube el archivo normalizado a datalake/raw/airflow/G5/... (sobrescribe: re-ejecutar no duplica)."""
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    if not os.path.exists(ruta_local):
        raise FileNotFoundError(f"No existe el archivo a subir: {ruta_local}")
    blob = blob_destino(fecha_operativa)
    WasbHook(wasb_conn_id=cfg.WASB_CONN_ID).load_file(
        file_path=ruta_local, container_name=cfg.CONTAINER_NAME, blob_name=blob, overwrite=True
    )
    log.info(f"==> Subido a {cfg.CONTAINER_NAME}/{blob}")
    return f"{cfg.CONTAINER_NAME}/{blob}"


def _kpis(horas: pd.DataFrame) -> pd.DataFrame:
    """Calcula los indicadores a partir de horas por grupo de tiempo (una fila por camión o agregado)."""
    k = horas.copy()
    k["h_disponible"] = k["h_calendario"] - k["h_mantenimiento"]
    k["h_operativa"] = k["h_disponible"] - k["h_reserva"]
    k["h_efectiva"] = k["h_operativa"] - k["h_demora"]
    k["disponibilidad_fisica_pct"] = 100 * k["h_disponible"] / k["h_calendario"]
    k["utilizacion_pct"] = 100 * k["h_operativa"] / k["h_disponible"].where(k["h_disponible"] > 0)
    k["eficiencia_operativa_pct"] = 100 * k["h_efectiva"] / k["h_operativa"].where(k["h_operativa"] > 0)
    k["uso_efectivo_pct"] = 100 * k["h_efectiva"] / k["h_calendario"]
    fallas = k["n_fallas"].where(k["n_fallas"] > 0)
    k["mttr_h"] = k["h_mant_no_programado"] / fallas
    k["mtbf_h"] = k["h_operativa"] / fallas
    return k.round(2)


def calcular_kpis(ruta_normalizada: str, fecha_operativa: date) -> str:
    """
    Vista previa de la futura capa Gold: KPI de desempeño por camión, por serie y de toda la flota
    para el día operativo. Los camiones sin demoras cuentan con 24 h efectivas.
    """
    df = pd.read_csv(ruta_normalizada)
    v_ini, _ = ventana_dia_operativo(fecha_operativa)

    horas = df.pivot_table(index="equipo", columns="grupo_tiempo", values="duracion_h_dia",
                           aggfunc="sum", fill_value=0.0)
    horas = horas.reindex(columns=["MANTENIMIENTO", "RESERVA", "DEMORA"], fill_value=0.0)
    horas.columns = ["h_mantenimiento", "h_reserva", "h_demora"]

    mnp = df[df["categoria"] == cfg.UNPLANNED_MAINTENANCE]
    horas["h_mant_no_programado"] = mnp.groupby("equipo")["duracion_h_dia"].sum()
    inicio_en_dia = pd.to_datetime(mnp["inicio_original"], format="ISO8601") >= v_ini
    horas["n_fallas"] = mnp[inicio_en_dia].groupby("equipo").size()

    flota = pd.Index(sorted(set(cfg.TRUCK_TO_SERIES) | set(horas.index)), name="equipo")
    horas = horas.reindex(flota).fillna(0.0)
    horas["h_calendario"] = cfg.HOURS_PER_DAY
    horas["serie"] = [cfg.TRUCK_TO_SERIES.get(e, "SIN_SERIE") for e in horas.index]

    sumables = ["h_calendario", "h_mantenimiento", "h_reserva", "h_demora", "h_mant_no_programado", "n_fallas"]
    por_camion = _kpis(horas).reset_index().assign(nivel="CAMION")
    por_serie = _kpis(horas.groupby("serie")[sumables].sum()).reset_index().assign(nivel="SERIE", equipo="")
    total = _kpis(horas[sumables].sum().to_frame().T).assign(nivel="FLOTA", equipo="", serie="TODAS")

    kpi = pd.concat([total, por_serie, por_camion], ignore_index=True)
    kpi.insert(0, "fecha_operativa", fecha_operativa.isoformat())
    columnas = ["fecha_operativa", "nivel", "serie", "equipo", "h_calendario", "h_mantenimiento", "h_reserva",
                "h_demora", "h_disponible", "h_operativa", "h_efectiva", "disponibilidad_fisica_pct",
                "utilizacion_pct", "eficiencia_operativa_pct", "uso_efectivo_pct", "n_fallas", "mttr_h", "mtbf_h"]
    os.makedirs(cfg.KPI_DIR, exist_ok=True)
    salida = os.path.join(cfg.KPI_DIR, f"kpi_camiones_{fecha_operativa:%Y%m%d}.csv")
    kpi[columnas].to_csv(salida, index=False)

    f = kpi.iloc[0]
    log.info(
        f"==> KPI flota {fecha_operativa}: DF {f.disponibilidad_fisica_pct}% | UT {f.utilizacion_pct}% | "
        f"Ef. operativa {f.eficiencia_operativa_pct}% | fallas {int(f.n_fallas)} | MTTR {f.mttr_h} h -> {salida}"
    )
    return salida
