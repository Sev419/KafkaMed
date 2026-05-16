# Reporte de Fase 1 - Kafka y productor

## 1. Objetivo

Validar que Kafka pueda levantarse en Docker y que `kafka/producer.py` pueda
publicar mensajes reales en el topic `heart-records`.

## 2. Archivos revisados

- [kafka/__init__.py](../../kafka/__init__.py)
- [kafka/producer.py](../../kafka/producer.py)
- [docker-compose.yml](../../docker-compose.yml)
- [requirements.txt](../../requirements.txt)
- [docs/arquitectura/arquitectura_kafkamed.md](../arquitectura/arquitectura_kafkamed.md)

## 3. Confirmacion de estructura

- `kafka/__init__.py` existe.
- `kafka/init.py` no existe.
- La carpeta `kafka/` esta correctamente definida como paquete Python.

## 4. Imagen Kafka y configuracion

### Imagen anterior fallida

- `bitnami/kafka:3.7`

### Imagen nueva usada

- `apache/kafka:3.7.2`

### Ajustes relevantes en `docker-compose.yml`

- listeners internos y externos:
  - `INSIDE://:9092`
  - `OUTSIDE://:9094`
  - `CONTROLLER://:9093`
- acceso desde el host:
  - `localhost:9092`
- acceso desde otros servicios Docker:
  - `kafka:9092`
- modo KRaft configurado con variables `KAFKA_*` compatibles con la imagen
  oficial de Apache Kafka.

## 5. Comandos ejecutados y resultado

### Validacion de Compose

```powershell
docker compose config
```

Resultado:
- OK
- la configuracion del Compose es valida
- el servicio `kafka` quedo definido correctamente con listeners interno y
  externo

### Levantamiento de Kafka

```powershell
docker compose up -d kafka
docker compose ps
docker compose logs kafka
```

Resultado:
- Kafka levanto correctamente en Docker
- el contenedor `sentimentstream-kafka` quedo arriba
- los logs muestran arranque exitoso en modo KRaft
- la version reportada por el broker fue `3.7.2`

### Sintaxis del productor

```powershell
python -m py_compile kafka\producer.py
```

Resultado:
- OK

### Ejecucion del productor

```powershell
python kafka\producer.py --limit 2
```

Resultado:
- el productor leyo el dataset local
- conecto correctamente a `localhost:9092`
- publico 2 mensajes en `heart-records`
- total enviado: `2`

### Validacion del topic

```powershell
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic heart-records --from-beginning --max-messages 2
```

Resultado:
- el topic `heart-records` existe
- el consumer devolvio los 2 mensajes enviados por el productor

## 6. Evidencia de Kafka corriendo

El contenedor `sentimentstream-kafka` quedo en ejecucion y aceptando
conexiones. Los logs muestran arranque correcto del broker en KRaft:

- `Running in KRaft mode`
- `Kafka version: 3.7.2`
- `Kafka Server started`

## 7. Evidencia de mensajes enviados

Mensajes confirmados en `heart-records`: `2`

Registros consumidos:

```json
{"Age": 40, "Sex": "M", "ChestPainType": "ATA", "RestingBP": 140, "Cholesterol": 289, "FastingBS": 0, "RestingECG": "Normal", "MaxHR": 172, "ExerciseAngina": "N", "Oldpeak": 0.0, "ST_Slope": "Up", "HeartDisease": 0, "source_file": "heart_failure_prediction.csv", "row_number": 1}
{"Age": 49, "Sex": "F", "ChestPainType": "NAP", "RestingBP": 160, "Cholesterol": 180, "FastingBS": 0, "RestingECG": "Normal", "MaxHR": 156, "ExerciseAngina": "N", "Oldpeak": 1.0, "ST_Slope": "Flat", "HeartDisease": 1, "source_file": "heart_failure_prediction.csv", "row_number": 2}
```

## 8. Problemas encontrados

1. La imagen `bitnami/kafka:3.7` no estaba disponible.
2. El productor necesitaba una correccion de compatibilidad con
   `confluent-kafka` para reportar entrega sin usar `opaque=`.
3. Hubo una fase previa de bloqueo del daemon de Docker Desktop, pero ya fue
   resuelta antes de esta validacion final.

## 9. Pendientes

- Pasar a la Fase 2: consumidor Spark Structured Streaming.
- Mantener separados el dominio de KafkaMed y el dominio original de
  SentimentStream.

## 10. Recomendacion

**La Fase 1 queda aprobada para pasar a Fase 2.**

Kafka levanto correctamente en Docker y el productor publico dos mensajes
reales en el topic `heart-records`.

## 11. Estado final de esta sesion

- Kafka quedo levantado.
- El productor envio 2 mensajes reales.
- El topic `heart-records` fue validado con consumer.
- La Fase 1 esta operativamente cerrada.
