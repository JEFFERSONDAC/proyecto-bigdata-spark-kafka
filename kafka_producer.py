"""
Envía lecturas sintéticas de sensores IoT al tópico Kafka sensor_data.

Requiere que Kafka esté iniciado y que exista el tópico.

Instalación:
    python -m pip install kafka-python

Ejecución:
    python kafka_producer.py
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "sensor_data"


def crear_medicion():
    return {
        "sensor_id": f"sensor_{random.randint(1, 10):02d}",
        "temperature": round(random.uniform(18.0, 38.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def main():
    productor = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )

    print(f"Enviando mediciones al tópico '{TOPIC}'. Presiona Ctrl+C para detener.")
    try:
        while True:
            medicion = crear_medicion()
            future = productor.send(TOPIC, value=medicion)
            try:
                metadata = future.get(timeout=10)
                print(
                    f"Enviado: {medicion} | "
                    f"partición={metadata.partition}, offset={metadata.offset}"
                )
            except KafkaError as error:
                print(f"Error enviando el mensaje: {error}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nProductor detenido por el usuario.")
    finally:
        productor.flush()
        productor.close()


if __name__ == "__main__":
    main()
