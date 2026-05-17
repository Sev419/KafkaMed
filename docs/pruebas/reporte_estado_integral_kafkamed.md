# Reporte integral de estado del proyecto KafkaMed

## 1. Resumen ejecutivo
KafkaMed es una plataforma de monitoreo cardiaco en streaming construida como proyecto independiente a partir de la arquitectura validada en SentimentStream.

El proyecto implementa actualmente el flujo:

```text
CSV clinico -> Kafka Producer -> Kafka -> Spark Structured Streaming -> ML -> MongoDB -> Flask API
```

La fase Power BI ya cuenta con documentacion tecnica, scripts M y recomendaciones de dashboard, pero queda pendiente crear el archivo visual `.pbix` y adjuntar capturas reales.

Repositorio oficial:

```text
https://github.com/Sev419/KafkaMed.git
```

Ruta local usada:

```text
C:\dev\KafkaMed
```

## 2. Arquitectura actual validada

Componentes implementados:

- Productor Kafka: lee el dataset clinico y publica mensajes JSON en `heart-records`.
- Apache Kafka: broker en Docker con imagen `apache/kafka:3.7.2`.
- Spark Structured Streaming: consume eventos desde Kafka.
- MongoDB: almacena registros clinicos y predicciones.
- Modelo ML: clasificador de riesgo cardiaco entrenado con dataset completo.
- Flask API: expone endpoints clinicos para consumo externo.
- Documentacion Power BI: preparada para consumir la API Flask mediante REST.

Flujo validado:

```text
Kafka -> Spark Structured Streaming -> ML -> MongoDB -> Flask API
```

Flujo objetivo de entrega:

```text
Kafka -> Spark Structured Streaming -> ML -> MongoDB -> Flask API -> Power BI
```

## 3. Dataset

Dataset usado:

```text
data/raw/heart_failure_prediction.csv
```

Estado actual:

- Filas: `918`
- Columnas: `12`
- Variable objetivo: `HeartDisease`
- Distribucion:
  - `HeartDisease=1`: `508`
  - `HeartDisease=0`: `410`

El proyecto tuvo una validacion tecnica inicial con un extracto de 5 filas, pero el modelo vigente fue reentrenado con el dataset completo.

## 4. Estado por fases

| Fase | Nombre | Estado | Evidencia principal |
|---|---|---|---|
| Fase 1 | Kafka + Producer | Validada | `reporte_fase_1_kafka_producer.md` |
| Fase 2 | Spark leyendo Kafka | Validada | `reporte_fase_2_spark_kafka.md` |
| Fase 3 | Spark -> MongoDB | Validada | `reporte_fase_3_spark_mongo.md` |
| Fase 4 | Modelo ML inicial | Validada | `reporte_fase_4_modelo_ml.md` |
| Fase 4.5 | Reentrenamiento dataset completo | Validada | `reporte_fase_4_5_dataset_completo_ml.md` |
| Fase 5 | Spark -> ML -> MongoDB | Validada | `reporte_fase_5_ml_streaming_mongo.md` |
| Fase 6 | API Flask clinica | Validada | `reporte_fase_6_api_flask.md` |
| Fase 7 | Power BI | Pendiente de cierre visual | `reporte_fase_7_powerbi.md` |

## 5. Fase 1: Kafka + producer.py

Objetivo:
- Levantar Kafka en Docker.
- Publicar mensajes reales en el topic `heart-records`.

Resultado:
- Kafka quedo operativo con `apache/kafka:3.7.2`.
- `kafka/producer.py` lee `data/raw/heart_failure_prediction.csv`.
- El productor publica mensajes JSON reales.
- El topic `heart-records` fue validado con consumidor de consola.

Archivos principales:
- `kafka/producer.py`
- `kafka/__init__.py`
- `docker-compose.yml`

## 6. Fase 2: Spark Structured Streaming leyendo Kafka

Objetivo:
- Leer mensajes reales desde Kafka con Spark Structured Streaming.
- Parsear JSON clinico.
- Mostrar registros en consola/logs.

Resultado:
- Spark consume `heart-records`.
- Se parsean correctamente campos clinicos y metadatos Kafka.

Campos confirmados:
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

Archivos principales:
- `spark_processing/jobs/process_heart_records_stream.py`
- `spark_processing/src/heart_schema.py`
- `spark_processing/src/kafka_config.py`
- `spark_processing/src/spark_session_factory.py`

## 7. Fase 3: Spark -> MongoDB

Objetivo:
- Persistir registros clinicos leidos desde Kafka en MongoDB.

Resultado:
- MongoDB se ejecuta en Docker con imagen `mongo:7`.
- Spark escribe registros en la base `kafkamed_db`.
- Coleccion de registros base: `heart_records`.

Archivos principales:
- `spark_processing/jobs/process_heart_records_to_mongo.py`
- `spark_processing/src/mongo_config.py`
- `spark_processing/src/mongo_writer.py`
- `docker-compose.yml`

## 8. Fase 4 y Fase 4.5: Modelo ML

Objetivo:
- Entrenar un modelo de clasificacion para predecir `HeartDisease`.
- Reentrenar con el dataset completo para obtener metricas defendibles.

Modelo vigente:
- `LogisticRegression`
- Archivo: `ml/models/heart_risk_model.joblib`
- Version documentada: `logistic_regression_complete_dataset_v1`

Metricas reales con dataset completo:

- Accuracy: `0.8967391304347826`
- Precision: `0.8878504672897196`
- Recall: `0.9313725490196079`
- F1: `0.9090909090909091`
- ROC AUC: `0.9295791487326638`

Archivos principales:
- `ml/train_heart_model.py`
- `ml/predict_heart_risk.py`
- `ml/config.py`
- `ml/models/heart_risk_model.joblib`
- `ml/metrics/heart_model_metrics.json`
- `ml/reports/heart_model_report.md`

## 9. Fase 5: Spark -> ML -> MongoDB

Objetivo:
- Integrar el modelo ML al flujo de streaming.
- Guardar predicciones enriquecidas en MongoDB.

Resultado validado:
- Spark lee eventos desde Kafka.
- Aplica inferencia con el modelo entrenado.
- Inserta predicciones en `kafkamed_db.heart_predictions`.

Resultado operativo documentado:
- Batch id: `0`
- Filas recibidas: `25`
- Filas insertadas: `25`
- Coincidencias: `0`
- Conteo en MongoDB: `25`

Ejemplo resumido de prediccion:

```json
{
  "prediction": 0,
  "prediction_label": "no_risk",
  "probability": 0.034237246756154346,
  "model_version": "logistic_regression_complete_dataset_v1"
}
```

Archivos principales:
- `spark_processing/jobs/process_heart_records_with_ml_to_mongo.py`
- `spark_processing/src/prediction_writer.py`
- `ml/predict_heart_risk.py`
- `requirements.txt`

## 10. Fase 6: API Flask clinica

Objetivo:
- Exponer datos reales desde MongoDB mediante endpoints REST.

Fuente de datos:

```text
mongodb://mongo:27017
database: kafkamed_db
collection: heart_predictions
```

Endpoints implementados:
- `GET /`
- `GET /health`
- `GET /patients`
- `GET /predictions`
- `GET /stats`
- `GET /risk-summary`

Resultados reales de validacion:
- `/health`: `mongo=ok`
- `/stats`:
  - `total=25`
  - `total_risk=6`
  - `total_no_risk=19`
  - `average_probability=0.26713404364674964`
- `/risk-summary`:
  - `risk_percentage=24.0`
  - distribucion `risk=6`, `no_risk=19`

Archivos principales:
- `api_flask/app.py`
- `api_flask/config.py`
- `api_flask/mongo_client.py`
- `api_flask/repository.py`
- `docker/api/Dockerfile`

## 11. Fase 7: Power BI

Objetivo:
- Construir un dashboard Power BI consumiendo la API Flask.

Estado:
- Documentacion tecnica preparada.
- Scripts M completos preparados.
- Medidas DAX sugeridas preparadas.
- Estructura PBIX sugerida preparada.
- Pendiente crear archivo `.pbix` y adjuntar capturas reales.

Fuente oficial para Power BI:

```text
http://localhost:8000
```

Endpoints para BI:
- `/stats`
- `/risk-summary`
- `/predictions`
- `/patients`

Reporte:
- `docs/pruebas/reporte_fase_7_powerbi.md`

## 12. Comandos principales de ejecucion

Levantar Kafka y MongoDB:

```powershell
docker compose up -d kafka mongo
```

Publicar mensajes:

```powershell
python kafka\producer.py --limit 5
```

Consumir Kafka con Spark y mostrar consola:

```powershell
docker compose --profile processing run --rm spark-heart-stream
```

Persistir registros en MongoDB:

```powershell
docker compose --profile processing run --rm spark-heart-mongo
```

Ejecutar inferencia ML y persistir predicciones:

```powershell
docker compose --profile processing run --rm spark-heart-ml-mongo
```

Levantar API:

```powershell
docker compose up -d api
```

Probar endpoints:

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/stats
Invoke-RestMethod http://localhost:8000/risk-summary
Invoke-RestMethod "http://localhost:8000/predictions?limit=5"
Invoke-RestMethod "http://localhost:8000/patients?limit=5"
```

## 13. Validaciones tecnicas

Pruebas locales:

```powershell
python -m pytest tests
```

Validacion Docker Compose:

```powershell
docker compose config
docker compose --profile processing config
```

Validacion Mongo:

```powershell
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('kafkamed_db').heart_predictions.countDocuments({})"
```

## 14. Documentacion disponible

- `docs/arquitectura/arquitectura_kafkamed.md`
- `docs/pruebas/reporte_fase_1_kafka_producer.md`
- `docs/pruebas/reporte_fase_2_spark_kafka.md`
- `docs/pruebas/reporte_fase_3_spark_mongo.md`
- `docs/pruebas/reporte_fase_4_modelo_ml.md`
- `docs/pruebas/reporte_fase_4_5_dataset_completo_ml.md`
- `docs/pruebas/reporte_fase_5_ml_streaming_mongo.md`
- `docs/pruebas/reporte_fase_6_api_flask.md`
- `docs/pruebas/reporte_fase_7_powerbi.md`
- `docs/pruebas/reporte_separacion_repositorio.md`
- `docs/pruebas/reporte_publicacion_github.md`

## 15. Riesgos y pendientes

Pendientes principales:
- Crear dashboard real en Power BI Desktop.
- Guardar archivo `.pbix`.
- Adjuntar capturas del dashboard consumiendo la API.
- Preparar informe tecnico final de 6 a 10 paginas.
- Preparar demo en vivo.

Riesgos conocidos:
- Power BI Service no puede refrescar `localhost` sin gateway.
- El healthcheck de Kafka puede mostrarse `unhealthy` aunque el broker funcione; la validacion real debe basarse tambien en productor y consumidor.
- Si se borra el volumen de MongoDB, sera necesario reejecutar el pipeline de streaming para repoblar `heart_predictions`.

## 16. Estado final

KafkaMed tiene validado el backend Big Data hasta API:

```text
Kafka -> Spark Structured Streaming -> ML -> MongoDB -> Flask API
```

La parte pendiente para cierre completo de la actividad es:

```text
Power BI dashboard + informe final + demo
```
