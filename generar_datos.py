"""
Genera datos sintéticos de sensores IoT para la práctica de Big Data.

Uso:
    python generar_datos.py
    python generar_datos.py --filas 1000 --salida datos/sensores.csv
"""

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def generar_datos(cantidad: int) -> pd.DataFrame:
    random.seed(42)
    ahora = datetime.now(timezone.utc)
    registros = []

    for i in range(cantidad):
        sensor_id = f"sensor_{random.randint(1, 10):02d}"
        timestamp = ahora - timedelta(seconds=(cantidad - i) * 10)
        registros.append({
            "sensor_id": sensor_id,
            "temperature": round(random.uniform(18.0, 38.0), 2),
            "humidity": round(random.uniform(30.0, 90.0), 2),
            "timestamp": timestamp.isoformat()
        })

    return pd.DataFrame(registros)


def main():
    parser = argparse.ArgumentParser(description="Generar mediciones sintéticas de sensores IoT.")
    parser.add_argument("--filas", type=int, default=500, help="Cantidad de mediciones a generar.")
    parser.add_argument("--salida", default="datos/sensores.csv", help="Ruta del CSV de salida.")
    args = parser.parse_args()

    if args.filas <= 0:
        parser.error("--filas debe ser un número mayor que cero.")

    salida = Path(args.salida)
    salida.parent.mkdir(parents=True, exist_ok=True)
    df = generar_datos(args.filas)
    df.to_csv(salida, index=False)

    print(f"Archivo generado: {salida.resolve()}")
    print(f"Registros: {len(df)}")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()
