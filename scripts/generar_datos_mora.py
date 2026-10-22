"""
Generador de datos sintéticos para el proyecto de morosidad de créditos (Banco Andino, ficticio).

Simula la exportación diaria que el "servidor de contingencia" (réplica de producción)
deja en una carpeta. Por cada fecha de corte genera 5 archivos CSV:

    agencies_AAAAMMDD.csv      maestro de agencias y administradores
    customers_AAAAMMDD.csv     clientes dados de alta hasta la fecha de corte
    loans_AAAAMMDD.csv         créditos desembolsados hasta la fecha de corte
    installments_AAAAMMDD.csv  cronograma de cuotas de esos créditos
    payments_AAAAMMDD.csv      pagos recibidos ese día

Los datos son deterministas (misma semilla = mismos datos), así que generar el mismo día
dos veces produce los mismos archivos, y días consecutivos son coherentes entre sí.

Solo usa la biblioteca estándar de Python, así que corre igual en Windows o dentro
del contenedor de Airflow.

Ejemplos:
    python scripts/generar_datos_mora.py                         # fecha de corte = ayer
    python scripts/generar_datos_mora.py --fecha 2026-10-21
    python scripts/generar_datos_mora.py --fecha 2026-10-21 --dias 7
    python scripts/generar_datos_mora.py --corromper columnas    # para probar la carpeta failed/
"""

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEMILLA = 2026
INICIO_CARTERA = date(2025, 1, 1)   # primer desembolso posible
FIN_CARTERA = date(2026, 12, 31)    # último desembolso posible

# agencia_id, nombre, región, factor de riesgo (1 = promedio)
AGENCIAS = [
    ("AG01", "Lima Centro", "Lima", 1.0),
    ("AG02", "Miraflores", "Lima", 0.6),
    ("AG03", "San Juan de Lurigancho", "Lima", 1.5),
    ("AG04", "Los Olivos", "Lima", 1.2),
    ("AG05", "Arequipa", "Sur", 0.8),
    ("AG06", "Cusco", "Sur", 1.1),
    ("AG07", "Tacna", "Sur", 0.7),
    ("AG08", "Trujillo", "Norte", 1.0),
    ("AG09", "Chiclayo", "Norte", 1.3),
    ("AG10", "Piura", "Norte", 1.6),
    ("AG11", "Huancayo", "Centro", 1.2),
    ("AG12", "Iquitos", "Oriente", 1.8),
]

NOMBRES = ["Ana", "Luis", "María", "Jorge", "Rosa", "Carlos", "Lucía", "José", "Carmen", "Miguel",
           "Elena", "Juan", "Patricia", "Pedro", "Sofía", "Diego", "Valeria", "Raúl", "Gabriela", "César",
           "Milagros", "Víctor", "Daniela", "Fernando", "Karina", "Óscar", "Liliana", "Hugo", "Andrea", "Iván"]
APELLIDOS = ["Quispe", "Flores", "Sánchez", "Rodríguez", "García", "Mamani", "Huamán", "Ramírez",
             "Torres", "Chávez", "Vargas", "Castillo", "Mendoza", "Rojas", "Gutiérrez", "Díaz",
             "Ramos", "Cruz", "Paredes", "Salazar", "Espinoza", "Vásquez", "Córdova", "Ríos"]

# producto, moneda, monto mínimo, monto máximo, plazos posibles (meses), tasa anual
PRODUCTOS = [
    ("Consumo", "PEN", 1_500, 25_000, [6, 12, 18, 24], 0.32),
    ("Mypes", "PEN", 5_000, 80_000, [12, 18, 24, 36], 0.28),
    ("Vehicular", "PEN", 25_000, 120_000, [24, 36], 0.16),
    ("Hipotecario", "USD", 40_000, 180_000, [36], 0.09),
]

# perfil de pago: probabilidad base y rango de días de atraso
PERFILES = {
    "puntual": (0.72, (-5, 0)),
    "ocasional": (0.15, (1, 20)),
    "atrasado": (0.09, (15, 75)),
    "incumple": (0.04, (0, 25)),   # paga con atraso y deja de pagar desde una cuota
}

COLUMNAS = {
    "agencies": ["agencia_id", "nombre_agencia", "region", "administrador", "correo_administrador"],
    "customers": ["cliente_id", "tipo_documento", "numero_documento", "nombres", "apellidos",
                  "fecha_alta", "agencia_id", "segmento"],
    "loans": ["credito_id", "cliente_id", "agencia_id", "producto", "moneda", "monto_desembolsado",
              "tasa_anual", "plazo_meses", "fecha_desembolso"],
    "installments": ["credito_id", "numero_cuota", "fecha_vencimiento", "monto_capital",
                     "monto_interes", "monto_cuota"],
    "payments": ["pago_id", "credito_id", "numero_cuota", "fecha_pago", "monto_pagado", "canal"],
}


def sumar_meses(d: date, meses: int) -> date:
    m = d.month - 1 + meses
    y, m = d.year + m // 12, m % 12 + 1
    dia = min(d.day, [31, 29 if y % 4 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return date(y, m, dia)


def construir_universo(n_clientes: int):
    """Arma toda la cartera posible de forma determinista; luego se filtra por fecha de corte."""
    rnd = random.Random(SEMILLA)

    agencias = []
    for ag_id, nombre, region, _ in AGENCIAS:
        adm = f"{rnd.choice(NOMBRES)} {rnd.choice(APELLIDOS)}"
        usuario = nombre.lower().replace(" ", "")
        agencias.append([ag_id, nombre, region, adm, f"adm.{usuario}@bancoandino.example"])

    riesgo = {a[0]: a[3] for a in AGENCIAS}
    pesos_agencia = [3 if a[2] == "Lima" else 2 for a in AGENCIAS]

    clientes, creditos, cuotas, pagos = [], [], [], []
    dias_cartera = (FIN_CARTERA - INICIO_CARTERA).days

    for i in range(1, n_clientes + 1):
        ag = rnd.choices(AGENCIAS, weights=pesos_agencia)[0][0]
        alta = INICIO_CARTERA + timedelta(days=rnd.randint(0, dias_cartera - 30))
        clientes.append([f"CL{i:06d}", "DNI", f"{rnd.randint(10_000_000, 79_999_999)}",
                         rnd.choice(NOMBRES), f"{rnd.choice(APELLIDOS)} {rnd.choice(APELLIDOS)}",
                         alta.isoformat(), ag, rnd.choice(["Personas", "Personas", "Mype"])])

        # el perfil de pago depende del riesgo de la agencia
        f = riesgo[ag]
        base = {k: v[0] for k, v in PERFILES.items()}
        base["ocasional"] *= f
        base["atrasado"] *= f
        base["incumple"] *= f
        perfil = rnd.choices(list(base), weights=list(base.values()))[0]

        for _ in range(rnd.choices([1, 2], weights=[0.75, 0.25])[0]):
            prod, mon, mmin, mmax, plazos, tasa = rnd.choices(PRODUCTOS, weights=[50, 30, 12, 8])[0]
            desembolso = alta + timedelta(days=rnd.randint(0, 200))
            if desembolso > FIN_CARTERA:
                continue
            cred_id = f"CR{len(creditos) + 1:07d}"
            monto = round(rnd.uniform(mmin, mmax), -2)
            plazo = rnd.choice(plazos)
            tasa_c = round(tasa + rnd.uniform(-0.03, 0.03), 4)
            creditos.append([cred_id, f"CL{i:06d}", ag, prod, mon, f"{monto:.2f}", f"{tasa_c:.4f}",
                             plazo, desembolso.isoformat()])

            r = tasa_c / 12
            cuota = monto * r / (1 - (1 + r) ** -plazo)
            saldo = monto
            corte_incumple = rnd.randint(2, max(2, plazo // 2)) if perfil == "incumple" else None

            for n in range(1, plazo + 1):
                interes = saldo * r
                capital = cuota - interes if n < plazo else saldo
                saldo -= capital
                venc = sumar_meses(desembolso, n)
                monto_cuota = round(capital + interes, 2)
                cuotas.append([cred_id, n, venc.isoformat(), f"{capital:.2f}", f"{interes:.2f}",
                               f"{monto_cuota:.2f}"])

                # ¿cuándo (y si) se paga esta cuota?
                if perfil == "incumple" and n >= corte_incumple:
                    continue
                lo, hi = PERFILES[perfil][1]
                atraso = rnd.randint(lo, hi) if rnd.random() > 0.1 else rnd.randint(-3, 3)
                fecha_pago = venc + timedelta(days=atraso)
                pagos.append([cred_id, n, fecha_pago, monto_cuota,
                              rnd.choices(["Agencia", "App", "Banca por internet", "Agente"],
                                          weights=[25, 40, 20, 15])[0]])

    return agencias, clientes, creditos, cuotas, pagos


def generar_dia(universo, corte: date, salida: Path, corromper: str | None = None) -> list[Path]:
    agencias, clientes, creditos, cuotas, pagos = universo
    iso = corte.isoformat()
    creditos_vigentes = [c for c in creditos if c[8] <= iso]
    ids = {c[0] for c in creditos_vigentes}

    pagos_dia = sorted((p for p in pagos if p[2] == corte), key=lambda p: (p[0], p[1]))
    filas = {
        "agencies": agencias,
        "customers": [c for c in clientes if c[5] <= iso],
        "loans": creditos_vigentes,
        "installments": [q for q in cuotas if q[0] in ids],
        "payments": [[f"PG{corte:%Y%m%d}{k:05d}", p[0], p[1], iso, f"{p[3]:.2f}", p[4]]
                     for k, p in enumerate(pagos_dia, 1)],
    }

    salida.mkdir(parents=True, exist_ok=True)
    escritos = []
    for entidad, rows in filas.items():
        ruta = salida / f"{entidad}_{corte:%Y%m%d}.csv"
        cols = COLUMNAS[entidad]
        if corromper and entidad == "payments":
            if corromper == "vacio":
                ruta.write_text("", encoding="utf-8")
                escritos.append(ruta)
                continue
            if corromper == "columnas":
                cols = cols[:-1] + ["canal_pago_error"]
        with ruta.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            w.writerows(rows)
        escritos.append(ruta)
    return escritos


def main():
    ap = argparse.ArgumentParser(description="Genera los CSV diarios del servidor de contingencia (simulado).")
    ap.add_argument("--fecha", help="Fecha de corte AAAA-MM-DD (por defecto: ayer)")
    ap.add_argument("--dias", type=int, default=1, help="Cantidad de días consecutivos que terminan en --fecha")
    ap.add_argument("--salida", default="data/incoming", help="Carpeta destino (por defecto: data/incoming)")
    ap.add_argument("--clientes", type=int, default=1500, help="Número de clientes del banco")
    ap.add_argument("--corromper", choices=["vacio", "columnas"],
                    help="Daña payments del último día para probar el manejo de errores")
    a = ap.parse_args()

    fin = datetime.strptime(a.fecha, "%Y-%m-%d").date() if a.fecha else date.today() - timedelta(days=1)
    universo = construir_universo(a.clientes)
    salida = Path(a.salida)
    for k in range(a.dias - 1, -1, -1):
        corte = fin - timedelta(days=k)
        archivos = generar_dia(universo, corte, salida, a.corromper if k == 0 else None)
        resumen = ", ".join(f"{p.name.split('_')[0]}={max(0, sum(1 for _ in p.open(encoding="utf-8")) - 1)}"
                            for p in archivos)
        print(f"{corte}: {resumen}")
    print(f"Archivos en: {salida.resolve()}")


if __name__ == "__main__":
    main()
