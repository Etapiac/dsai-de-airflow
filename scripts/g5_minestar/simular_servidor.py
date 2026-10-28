"""
Simula el servidor de MineStar: toma las exportaciones bimestrales y deja un archivo por día operativo
(07:00 a 07:00) en data/minestar/servidor/AAAA/MM/demoras_camiones_AAAAMMDD.csv, con el mismo formato
que exporta MineStar. Una demora que cruza varios días aparece en el archivo de cada día que toca.

Uso (desde el contenedor):
    python -m scripts.g5_minestar.simular_servidor [--desde 2026-01-01] [--hasta 2026-10-25]
"""
import argparse
import glob
import os
from datetime import date, timedelta

import pandas as pd

from scripts.g5_minestar import config as cfg
from scripts.g5_minestar.pipeline import ruta_archivo_fuente, ventana_dia_operativo


def main(desde: date | None, hasta: date | None) -> None:
    archivos = sorted(glob.glob(os.path.join(cfg.EXPORTS_DIR, "*.csv")))
    if not archivos:
        raise SystemExit(f"No hay exportaciones en {cfg.EXPORTS_DIR}")

    df = pd.concat([pd.read_csv(a, encoding="utf-8-sig", dtype=str, keep_default_na=False) for a in archivos])
    df = df.drop_duplicates(subset="OID Minestar", keep="last")
    inicio = pd.to_datetime(df["Inicio Demora"], format=cfg.DATETIME_FORMAT)
    fin = pd.to_datetime(df["Fin Demora"], format=cfg.DATETIME_FORMAT, errors="coerce")
    corte = max(inicio.max(), fin.max())
    fin = fin.fillna(corte)   # demoras abiertas: siguen activas hasta el corte de la exportación

    desde = desde or ventana_dia_operativo(inicio.min().date())[0].date() + timedelta(days=1)
    hasta = hasta or (corte - timedelta(hours=cfg.DAY_START_HOUR)).date() - timedelta(days=1)

    dia, n = desde, 0
    while dia <= hasta:
        v_ini, v_fin = ventana_dia_operativo(dia)
        del_dia = df[(inicio < v_fin) & (fin > v_ini)]
        ruta = ruta_archivo_fuente(dia)
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        del_dia[cfg.SOURCE_COLUMNS].to_csv(ruta, index=False, encoding="utf-8-sig")
        n += 1
        dia += timedelta(days=1)
    print(f"{n} archivos diarios creados en {cfg.SERVER_DIR} ({desde} a {hasta}); corte de datos: {corte}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--desde", type=date.fromisoformat)
    parser.add_argument("--hasta", type=date.fromisoformat)
    args = parser.parse_args()
    main(args.desde, args.hasta)
