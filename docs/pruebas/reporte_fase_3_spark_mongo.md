# Reporte de validación Fase 3: Spark Structured Streaming -> MongoDB

## 1. Objetivo de la Fase 3
Persistir en MongoDB los registros clínicos leídos desde Kafka por Spark Structured Streaming, sin implementar todavía modelo ML, Flask clínico ni Power BI.

## 2. Archivos creados
En esta fase ya existían y se reutilizaron los siguientes componentes:
- [spark_processing/src/mongo_config.py](C:/Users/User/Desktop/Big%20DAta/KafkaMed/spark_processing/src/mongo_config.py)
- [spark_processing/src/mongo_writer.py](C:/Users/User/Desktop/Big%20DAta/KafkaMed/spark_processing/src/mongo_writer.py)
- [spark_processing/jobs/process_heart_records_to_mongo.py](C:/Users/User/Desktop/Big%20DAta/KafkaMed/spark_processing/jobs/process_heart_records_to_mongo.py)
- [docs/pruebas/reporte_fase_3_spark_mongo.md](C:/Users/User/Desktop/Big%20DAta/KafkaMed/docs/pruebas/reporte_fase_3_spark_mongo.md)

## 3. Archivos modificados
Se mantuvieron y usaron como base los siguientes archivos del proyecto:
- [docker-compose.yml](C:/Users/User/Desktop/Big%20DAta/KafkaMed/docker-compose.yml)
- [\.env.example](C:/Users/User/Desktop/Big%20DAta/KafkaMed/.env.example)
- [tests/test_project_structure.py](C:/Users/User/Desktop/Big%20DAta/KafkaMed/tests/test_project_structure.py)

## 4. Comandos ejecutados y resultado

### 4.1 Levantar Kafka y MongoDB
```powershell
docker compose up -d kafka mongo
```
Resultado:
- OK
- Se creó la red `kafkamed_default`
- Se creó el volumen `kafkamed_mongo_data`
- Se iniciaron `kafkamed-kafka` y `kafkamed-mongo`

### 4.2 Verificar estado de contenedores
```powershell
docker compose ps
```
Resultado:
- `kafkamed-kafka` arriba con imagen `apache/kafka:3.7.2`
- `kafkamed-mongo` arriba con imagen `mongo:7`
- MongoDB en estado `healthy`

### 4.3 Publicar mensajes clínicos en Kafka
```powershell
python kafka\producer.py --limit 5
```
Resultado:
- OK
- Conexión establecida a Kafka en `localhost:9092`
- Broker disponible
- Topic usado: `heart-records`
- Mensajes enviados: `5`
- Offsets entregados: `0, 1, 2, 3, 4`

### 4.4 Construir el job Spark -> Mongo
```powershell
docker compose --profile processing build spark-heart-mongo
```
Resultado:
- OK
- Imagen `kafkamed-spark-heart-mongo` construida correctamente

### 4.5 Ejecutar Spark Structured Streaming con persistencia en MongoDB
```powershell
docker compose --profile processing run --rm spark-heart-mongo
```
Resultado:
- OK
- Spark Structured Streaming inició correctamente
- Configuración usada:
  - `bootstrap=kafka:9092`
  - `topic=heart-records`
  - `startingOffsets=earliest`
  - `checkpoint=/tmp/kafkamed/checkpoints/heart-records-mongo`
- Batch procesado:
  - `batch_id=0`
  - `filas=5`
- Persistencia:
  - `recibidas=5`
  - `insertadas=5`
  - `coincidentes=0`

### 4.6 Verificar documento real en MongoDB
```powershell
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('kafkamed_db').heart_records.findOne()"
```
Resultado:
- OK
- MongoDB devolvió un documento real en la colección `heart_records`

## 5. Evidencia de registros insertados en MongoDB
Se confirmó que Spark leyó 5 registros desde Kafka y los persistió correctamente en MongoDB.

Campos observados en el documento insertado:
- `Age`
- `Sex`
- `ChestPainType`
- `RestingBP`
- `Cholesterol`
- `FastingBS`
- `RestingECG`
- `MaxHR`
- `ExerciseAngina`
- `Oldpeak`
- `ST_Slope`
- `HeartDisease`
- `source_file`
- `row_number`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `ingestion_batch_id`
- `inserted_at`

## 6. Cantidad de documentos insertados
- Mensajes publicados por el productor en esta validación: `5`
- Registros recibidos por Spark en el batch: `5`
- Registros insertados en MongoDB: `5`

## 7. Ejemplo de documento insertado
Resumen no sensible del documento almacenado en `kafkamed_db.heart_records`:

```json
{
  "Age": "<valor clínico>",
  "Sex": "<valor clínico>",
  "ChestPainType": "<valor clínico>",
  "RestingBP": "<valor clínico>",
  "Cholesterol": "<valor clínico>",
  "FastingBS": "<valor clínico>",
  "RestingECG": "<valor clínico>",
  "MaxHR": "<valor clínico>",
  "ExerciseAngina": "<valor clínico>",
  "Oldpeak": "<valor clínico>",
  "ST_Slope": "<valor clínico>",
  "HeartDisease": "<valor clínico>",
  "source_file": "heart_failure_prediction.csv",
  "row_number": "<número de fila>",
  "kafka_partition": "<partición Kafka>",
  "kafka_offset": "<offset Kafka>",
  "kafka_timestamp": "<marca temporal Kafka>",
  "ingestion_batch_id": 0,
  "inserted_at": "<timestamp de inserción>"
}
```

## 8. Problemas encontrados
No se presentaron bloqueos operativos durante esta validación. La integración Kafka -> Spark -> MongoDB funcionó correctamente en Docker.

## 9. Estado final
**Fase 3 aprobada operativamente.**

La persistencia Spark Structured Streaming -> MongoDB quedó validada con datos reales, sin implementar todavía modelo ML, Flask clínico ni Power BI.

## 10. Próximo paso recomendado
Avanzar a la Fase 4 para incorporar el modelo de riesgo cardíaco sobre el flujo ya persistido en MongoDB, manteniendo la separación por fases y sin romper la validación actual.
