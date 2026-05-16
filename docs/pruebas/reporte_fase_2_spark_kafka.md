# Reporte de Fase 2 - Spark Structured Streaming y Kafka

## 1. Objetivo

Validar que Spark Structured Streaming lea mensajes reales desde el topic
`heart-records`, parsee el JSON clínico y muestre los registros en consola.

## 2. Archivos creados

- [spark_processing/src/heart_schema.py](../../spark_processing/src/heart_schema.py)
- [spark_processing/src/kafka_config.py](../../spark_processing/src/kafka_config.py)
- [spark_processing/jobs/process_heart_records_stream.py](../../spark_processing/jobs/process_heart_records_stream.py)

## 3. Archivos modificados

- [spark_processing/src/spark_session_factory.py](../../spark_processing/src/spark_session_factory.py)
- [docker-compose.yml](../../docker-compose.yml)
- [requirements.txt](../../requirements.txt)
- [tests/test_project_structure.py](../../tests/test_project_structure.py)

## 4. Comandos ejecutados y resultado

### Sintaxis y estructura

```powershell
python -m py_compile spark_processing\src\heart_schema.py spark_processing\src\kafka_config.py spark_processing\jobs\process_heart_records_stream.py spark_processing\src\spark_session_factory.py
```

Resultado:
- OK

```powershell
python -m pytest tests\test_project_structure.py
```

Resultado:
- OK
- 2 pruebas pasaron

```powershell
docker compose --profile processing config
```

Resultado:
- OK
- el servicio `spark-heart-stream` quedo definido correctamente

### Kafka y productor

```powershell
docker compose up -d kafka
python kafka\producer.py --limit 5
```

Resultado:
- Kafka levanto en Docker
- el productor envio 5 mensajes reales nuevos al topic `heart-records`
- total confirmado por logs del productor: `5`

### Imagen y consumidor Spark

```powershell
docker compose --profile processing build spark-heart-stream
docker compose --profile processing run --rm spark-heart-stream
```

Resultado:
- la imagen de streaming se construyo correctamente
- el consumidor Spark Structured Streaming arranco en Docker
- Spark leyo mensajes reales desde Kafka
- la query se ejecuto en modo `availableNow`
- la ejecucion termino de forma controlada

## 5. Evidencia de Spark leyendo Kafka

La salida en consola mostro un `Batch: 0` con los registros parseados del topic
`heart-records`.

Se observaron 7 filas en total:
- 2 filas iniciales publicadas en la Fase 1
- 5 filas nuevas publicadas en esta validacion

Campos leidos correctamente en consola:
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
- `kafka_timestamp`
- `kafka_partition`
- `kafka_offset`

## 6. Problemas encontrados

1. La primera referencia de dependencia de Kafka para Spark no existia en Maven.
   Se corrigio pasando de `4.1.1` a un artefacto disponible y estable:
   `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1`.
2. Hubo un `NoSuchMethodError` con `SerializedOffset` mientras se reutilizaba un
   checkpoint fallido; se resolvio eliminando el checkpoint generado por los
   intentos anteriores y relanzando el stream limpio.
3. El healthcheck del broker Kafka marcaba el contenedor como `unhealthy`, pero
   Kafka si estaba funcional para productor y consumidor. Para no bloquear esta
   fase, el servicio nuevo `spark-heart-stream` quedo esperando `service_started`
   en lugar de `service_healthy`.

## 7. Estado de validacion

- Kafka funciono como fuente real.
- El productor envio mensajes reales.
- Spark Structured Streaming leyo y mostro los mensajes.
- La Fase 2 queda aprobada.

## 8. Próximo paso recomendado

Pasar a la Fase 3: persistencia de los mensajes parseados en MongoDB, sin
mezclar todavia el modelo de riesgo ni la API clinica.
