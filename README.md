# KafkaMed - Plataforma de monitoreo cardiaco en streaming

KafkaMed es una plataforma de monitoreo cardiaco en streaming construida con
Apache Kafka y Apache Spark Structured Streaming. El proyecto simula la
publicacion continua de registros clinicos y su procesamiento en tiempo casi
real para detectar eventos de interes a partir de mensajes estructurados en
Kafka.

Este repositorio nacio reutilizando la arquitectura ya validada en
SentimentStream, pero KafkaMed es un proyecto independiente y no contiene el
dominio de sentimientos.

## 1. Descripcion general

KafkaMed simula el flujo de registros clinicos de pacientes con riesgo de falla
cardiaca. Un productor lee un CSV local, publica cada fila como JSON en un
topic Kafka y un consumidor Spark Structured Streaming lee esos mensajes para
mostrar los registros parseados en consola.

La meta final del proyecto es evolucionar ese flujo hacia una arquitectura
completa de extremo a extremo con persistencia, API y visualizacion.

## 2. Contexto academico

KafkaMed corresponde a una actividad de Big Data orientada a construir una
plataforma de streaming con componentes reales de industria.

El proyecto reutiliza la arquitectura probada en SentimentStream como base
tecnica, pero el repositorio actual es independiente y esta enfocado en el
dominio clinico.

## 3. Objetivo

El objetivo es implementar un pipeline de streaming que:

1. lea registros clinicos desde un CSV,
2. publique eventos en Kafka,
3. consuma esos eventos con Spark Structured Streaming,
4. y deje lista la base para continuar con persistencia, API y dashboard.

## 4. Arquitectura actual

Flujo validado hasta esta etapa:

```text
CSV clínico -> Kafka Producer -> Topic heart-records -> Spark Structured Streaming -> Consola/logs
```

### Evidencia de Fases validadas

- Fase 1: Kafka levanta en Docker y `producer.py` publica mensajes JSON reales.
- Fase 2: Spark Structured Streaming lee mensajes reales desde Kafka y los
  muestra en consola.

## 5. Arquitectura objetivo

La arquitectura final esperada es:

```text
CSV clínico -> Kafka Producer -> Kafka -> Spark Structured Streaming -> MongoDB -> Flask API -> Power BI
```

Las fases de MongoDB, Flask API y Power BI aun no estan implementadas en este
repositorio.

## 6. Tecnologias

- Python
- Apache Kafka
- Apache Spark / PySpark Structured Streaming
- Docker Compose
- MongoDB, como fase pendiente
- Flask API, como fase pendiente
- Power BI, como fase pendiente

## 7. Estructura del repositorio

- `kafka/`: productor Kafka para el dataset cardiaco.
- `spark_processing/`: helpers de Spark, esquema, configuracion y jobs de
  streaming.
- `docker/`: Dockerfiles usados por el runtime de streaming.
- `data/`: dataset local de prueba y artefactos de ejecucion.
- `docs/`: documentacion tecnica y reportes de validacion.
- `tests/`: pruebas de estructura y validacion basica del repositorio.

## 8. Dataset

Este repositorio usa el dataset **Heart Failure Prediction** para validacion
local del pipeline de ML.

El archivo disponible en el proyecto es:

- `data/raw/heart_failure_prediction.csv`

Estado actual del archivo:

- Filas: `918`
- Columnas: `12`
- Distribucion de `HeartDisease`: `1 -> 508`, `0 -> 410`

Nota: el proyecto conservo inicialmente un extracto de 5 filas para validar el
flujo tecnico. El entrenamiento actual ya se ejecuto con el dataset completo
incorporado en el repositorio para obtener metricas mas representativas.

## 9. Variables de entorno

### Productor Kafka

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `HEART_DATASET_PATH`
- `PRODUCER_INTERVAL_SECONDS`
- `PRODUCER_LIMIT`

### Spark Structured Streaming

- `SPARK_CHECKPOINT_LOCATION`
- `SPARK_STARTING_OFFSETS`
- `SPARK_JARS_PACKAGES`
- `STREAM_TRIGGER_MODE`
- `STREAM_TIMEOUT_SECONDS`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`

## 10. Requisitos previos

- Python 3.12 o superior compatible con el entorno local.
- Docker Desktop.
- Docker Compose.
- Instalacion de dependencias con:

```powershell
pip install -r requirements.txt
```

## 11. Instalacion

1. Clonar o ubicar el repositorio local.
2. Instalar dependencias:

```powershell
pip install -r requirements.txt
```

3. Verificar la configuracion del proyecto:

```powershell
docker compose config
```

## 12. Ejecucion

### Levantar Kafka

```powershell
docker compose up -d kafka
```

### Publicar mensajes de prueba

```powershell
python kafka\producer.py --limit 5
```

### Ejecutar el consumidor Spark Structured Streaming

```powershell
docker compose --profile processing run --rm spark-heart-stream
```

## 13. Validaciones tecnicas

### Sintaxis y pruebas basicas

```powershell
python -m py_compile kafka\producer.py
python -m py_compile spark_processing\src\heart_schema.py
python -m py_compile spark_processing\src\kafka_config.py
python -m py_compile spark_processing\jobs\process_heart_records_stream.py
python -m py_compile spark_processing\jobs\smoke_spark_session.py
python -m pytest tests\test_project_structure.py
docker compose config
```

## 14. Estado actual de fases

| Fase | Estado |
|---|---|
| Fase 1: Kafka + Producer | Validada |
| Fase 2: Spark leyendo Kafka | Validada |
| Fase 3: Spark -> MongoDB | Pendiente |
| Fase 4: Modelo ML | Validada con dataset completo |
| Fase 5: Flask API | Pendiente |
| Fase 6: Power BI | Pendiente |

## 15. Documentacion tecnica

- [Arquitectura KafkaMed](docs/arquitectura/arquitectura_kafkamed.md)
- [Reporte Fase 1 - Kafka y productor](docs/pruebas/reporte_fase_1_kafka_producer.md)
- [Reporte Fase 2 - Spark Structured Streaming y Kafka](docs/pruebas/reporte_fase_2_spark_kafka.md)
- [Reporte de separacion de repositorio](docs/pruebas/reporte_separacion_repositorio.md)
- [Reporte de publicacion en GitHub](docs/pruebas/reporte_publicacion_github.md)
- [Reporte Fase 4 - Modelo ML de riesgo cardiaco](docs/pruebas/reporte_fase_4_modelo_ml.md)
- [Reporte Fase 4.5 - Dataset completo y reentrenamiento](docs/pruebas/reporte_fase_4_5_dataset_completo_ml.md)

## 16. Proximo paso

La siguiente fase es integrar el modelo de riesgo cardiaco al flujo de streaming
sin tocar aun la API clinica ni Power BI.

## 17. Nota sobre SentimentStream

SentimentStream fue usado como referencia arquitectonica y de buenas practicas
de orquestacion, pero KafkaMed ya es un proyecto independiente, con nombre,
repositorio y alcance propios.
