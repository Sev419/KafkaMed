# Arquitectura KafkaMed - Fase 1

## 1. Objetivo

KafkaMed reutiliza la base operativa de SentimentStream para construir una
plataforma de monitoreo cardiaco en streaming con Apache Kafka.

La arquitectura objetivo completa será:

```text
producer.py -> Kafka topic heart-records -> Spark Structured Streaming -> MongoDB -> Flask API -> Power BI
```

## 2. Alcance De Esta Fase

En esta Fase 1 solo se implementa la base de ingesta:

- carpeta `kafka/`
- productor `kafka/producer.py`
- servicio Kafka en `docker-compose.yml`
- dependencias mínimas para publicar mensajes JSON
- dataset clínico inicial en `data/raw/heart_failure_prediction.csv`

## 3. Lo Que No Se Implementa Todavia

Esta fase no incluye:

- consumidor Spark Structured Streaming
- modelo de riesgo cardiaco
- API Flask clinica
- ajustes en Power BI
- consolidacion de Jenkinsfile
- alertas SMS

## 4. Reuso De SentimentStream

Se conserva la estructura tecnica ya validada en el proyecto anterior:

- Docker Compose como mecanismo principal de ejecucion
- gestion de variables de entorno
- logging claro y controlado
- MongoDB y Flask como capas futuras de consumo
- disciplina documental por fases

## 5. Estructura Esperada

```text
KafkaMed
  -> productor Kafka
  -> topic heart-records
  -> Spark Structured Streaming
  -> MongoDB
  -> Flask API
  -> Power BI
```

## 6. Dependencia Kafka Elegida

Para el productor se usa `confluent-kafka`.

### Justificacion breve

- cliente maduro y estable para Kafka
- compatible con envio JSON de mensajes
- evita la colision de nombres entre el paquete local `kafka/` y el modulo
  Python de otros clientes llamados `kafka`
- mantiene el alcance minimo de esta fase sin introducir dependencias extra

## 7. Variables De Entorno

### Productor

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `HEART_DATASET_PATH`
- `PRODUCER_INTERVAL_SECONDS`
- `PRODUCER_LIMIT`

### Docker

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `HEART_DATASET_PATH`

## 8. Validaciones Esperadas

### Validacion 1 - Estructura

```powershell
python -m pytest tests\test_project_structure.py
```

### Validacion 2 - Sintaxis del productor

```powershell
python -m py_compile kafka\producer.py
```

### Validacion 3 - Ayuda CLI

```powershell
python kafka\producer.py --help
```

### Validacion 4 - Compose

```powershell
docker compose config
```

### Validacion 5 - Produccion limitada

```powershell
python kafka\producer.py --limit 2
```

Solo debe ejecutarse cuando Kafka este disponible en `localhost:9092`.

## 9. Riesgos Pendientes

- El dataset suministrado en esta fase es un extracto inicial para alistamiento.
- Kafka todavia no tiene consumidor Spark conectado.
- El servicio Kafka debe validarse antes de la siguiente fase.
- Si en el futuro se ejecuta el productor dentro de Docker, puede ser necesario
  ajustar `KAFKA_BOOTSTRAP_SERVERS` para la red interna del compose.

## 10. Siguiente Paso Recomendado

Cerrar la validacion del broker Kafka y luego implementar el consumidor Spark
Structured Streaming sobre el topic `heart-records`.

