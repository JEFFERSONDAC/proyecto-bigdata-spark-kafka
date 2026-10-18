"""
Procesamiento batch de lecturas IoT con Apache Spark.

Uso:
    ~/spark-4.2.0-bin-hadoop3/bin/spark-submit spark_batch.py

Opcional:
    spark-submit spark_batch.py datos/sensores.csv resultados
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max as spark_max, min as spark_min, count


def main():
    archivo = sys.argv[1] if len(sys.argv) > 1 else "datos/sensores.csv"
    carpeta_salida = sys.argv[2] if len(sys.argv) > 2 else "resultados"

    if not Path(archivo).exists():
        raise FileNotFoundError(
            f"No se encontró {archivo}. Ejecuta primero: python generar_datos.py"
        )

    spark = (
        SparkSession.builder
        .appName("AnalisisBatchSensoresIoT")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(archivo)
    )

    print("\n=== Esquema de los datos ===")
    df.printSchema()

    print("\n=== Primeras cinco mediciones ===")
    df.show(5, truncate=False)

    print("\n=== Estadísticas generales ===")
    df.select(
        count("*").alias("total_mediciones"),
        avg("temperature").alias("temperatura_promedio"),
        spark_min("temperature").alias("temperatura_minima"),
        spark_max("temperature").alias("temperatura_maxima"),
        avg("humidity").alias("humedad_promedio")
    ).show(truncate=False)

    print("\n=== Resumen por sensor ===")
    resumen = (
        df.groupBy("sensor_id")
        .agg(
            count("*").alias("cantidad_mediciones"),
            avg("temperature").alias("temperatura_promedio"),
            avg("humidity").alias("humedad_promedio")
        )
        .orderBy("sensor_id")
    )
    resumen.show(truncate=False)

    Path(carpeta_salida).mkdir(parents=True, exist_ok=True)
    destino = str(Path(carpeta_salida) / "resumen_por_sensor")
    resumen.coalesce(1).write.mode("overwrite").option("header", True).csv(destino)
    print(f"Resumen guardado en: {destino}")

    spark.stop()


if __name__ == "__main__":
    main()
