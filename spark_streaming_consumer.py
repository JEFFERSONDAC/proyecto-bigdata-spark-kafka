"""
Consume el tópico Kafka sensor_data con Spark Structured Streaming.

La salida se muestra en consola; el procesamiento es en tiempo real.

Ejemplo:
    ~/spark-4.2.0-bin-hadoop3/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
      spark_streaming_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor_data"

ESQUEMA = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


def main():
    spark = (
        SparkSession.builder
        .appName("StreamingSensoresKafka")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    mensajes = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    lecturas = (
        mensajes
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), ESQUEMA).alias("dato"))
        .select("dato.*")
    )

    consulta = (
        lecturas.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", "resultados/checkpoint_streaming")
        .start()
    )

    print(f"Escuchando el tópico Kafka '{TOPIC}'. Presiona Ctrl+C para detener.")
    try:
        consulta.awaitTermination()
    except KeyboardInterrupt:
        print("\nConsumidor detenido por el usuario.")
        consulta.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
