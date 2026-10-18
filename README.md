# Análisis de datos IoT con Apache Spark y Apache Kafka

Proyecto académico de Big Data para generar mediciones sintéticas de sensores IoT, analizarlas por lotes con Apache Spark y transmitir datos en tiempo real mediante Apache Kafka y Spark Structured Streaming.

## Objetivos

- Generar un conjunto de datos sintéticos de sensores.
- Calcular estadísticas por sensor con Spark DataFrames.
- Publicar mediciones en el tópico Kafka `sensor_data`.
- Consumir y mostrar los mensajes en tiempo real con Spark Structured Streaming.

## Tecnologías

- Python 3
- Apache Spark 4.2.0
- Apache Kafka 4.3.1 en modo KRaft
- pandas
- kafka-python

> Ajusta las versiones de Spark/Kafka y el identificador del paquete de Spark Kafka si tu instalación utiliza otras versiones.

## Estructura

```text
.
├── datos/
├── resultados/
├── generar_datos.py
├── spark_batch.py
├── kafka_producer.py
├── spark_streaming_consumer.py
├── requirements.txt
├── .gitignore
└── README.md
```

## 1. Preparar Python

Crea un entorno virtual e instala las dependencias:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

En Windows, activa el entorno con:

```powershell
.venv\Scripts\Activate.ps1
```

## 2. Generar los datos

Desde la raíz del proyecto:

```bash
python generar_datos.py
```

Por defecto crea `datos/sensores.csv` con 500 mediciones. Puedes cambiar la cantidad:

```bash
python generar_datos.py --filas 1000
```

## 3. Ejecutar el análisis batch con Spark

Asegúrate de tener Apache Spark instalado y Java configurado. Si Spark está instalado en `~/spark-4.2.0-bin-hadoop3`:

```bash
~/spark-4.2.0-bin-hadoop3/bin/spark-submit spark_batch.py
```

El programa muestra el esquema, una muestra de los datos, estadísticas generales y un resumen por sensor. También guarda el resumen en `resultados/resumen_por_sensor/`.

Si `spark-submit` está en el PATH:

```bash
spark-submit spark_batch.py
```

## 4. Iniciar Kafka y crear el tópico

Inicia el servidor Kafka de acuerdo con la configuración de tu instalación. En Kafka 4.x se puede usar KRaft, sin ZooKeeper.

Desde la carpeta de Kafka, inicia el servidor con la configuración que hayas preparado, por ejemplo:

```bash
bin/kafka-server-start.sh config/server.properties
```

En otra terminal, crea el tópico si aún no existe:

```bash
bin/kafka-topics.sh --create \
  --topic sensor_data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Si el tópico ya existe, Kafka puede indicar que ya está creado; en ese caso continúa.

## 5. Ejecutar el productor Kafka

Activa el entorno virtual del proyecto e inicia el productor:

```bash
python kafka_producer.py
```

El productor envía una medición sintética cada dos segundos. Detén el programa con `Ctrl+C`.

## 6. Ejecutar Spark Structured Streaming

En otra terminal, desde la raíz del proyecto, ejecuta:

```bash
~/spark-4.2.0-bin-hadoop3/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  spark_streaming_consumer.py
```

Mantén Kafka y el productor en ejecución. Spark leerá los mensajes del tópico `sensor_data` y mostrará las mediciones en la consola.

El artefacto `spark-sql-kafka-0-10_2.13:4.2.0` debe corresponder a la versión de Spark y Scala instalada. Si tu versión es distinta, ajusta el paquete.

## 7. Subir el proyecto a GitHub

Crea un repositorio vacío en GitHub y copia su URL. Luego, desde esta carpeta, ejecuta:

```bash
git init
git add .
git commit -m "Añadir proyecto de Big Data con Spark y Kafka"
git branch -M main
git remote add origin https://github.com/TU-USUARIO/TU-REPOSITORIO.git
git push -u origin main
```

Reemplaza la URL de ejemplo por la URL real de tu repositorio. Si Git ya está inicializado o ya existe `origin`, no repitas esos comandos sin revisar la configuración:

```bash
git status
git remote -v
```

## Evidencias recomendadas para la entrega

- Captura de la generación del CSV.
- Captura del resultado del análisis batch en Spark.
- Captura del tópico `sensor_data` y del productor Kafka enviando mensajes.
- Captura del consumidor Spark Structured Streaming mostrando mediciones.
- Enlace al repositorio y enlaces reales a los videos de demostración.

## Nota sobre los datos

Los datos generados son sintéticos y se utilizan con fines académicos. No representan mediciones de sensores físicos. Ejecuta los programas en tu equipo y registra resultados reales; no presentes capturas ni estadísticas que no hayas obtenido.
