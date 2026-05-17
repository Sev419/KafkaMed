# Reporte de validacion Fase 5: ML en streaming con persistencia en MongoDB

## 1. Objetivo
Integrar el modelo de riesgo cardiaco al flujo de streaming para que Spark lea eventos desde Kafka, aplique inferencia y guarde en MongoDB registros enriquecidos con prediccion.

## 2. Modelo usado
- Modelo: `LogisticRegression`
- Archivo: `ml/models/heart_risk_model.joblib`
- Version: `logistic_regression_complete_dataset_v1`

Metricas reales vigentes del modelo entrenado con el dataset completo de 918 filas:
- Accuracy: `0.8967391304347826`
- Precision: `0.8878504672897196`
- Recall: `0.9313725490196079`
- F1: `0.9090909090909091`
- ROC AUC: `0.9295791487326638`

No se reentreno el modelo durante esta fase.

## 3. Arquitectura validada
Flujo aprobado en esta fase:

```text
Kafka -> Spark Structured Streaming -> ML inference -> MongoDB
```

La persistencia se realizo en la coleccion `heart_predictions` de la base `kafkamed_db`.

## 4. Comandos ejecutados

### Ejecucion del job de streaming con ML
```powershell
docker compose --profile processing run --rm spark-heart-ml-mongo
```

Resultado:
- La imagen `kafkamed-spark-heart-ml-mongo` se construyo correctamente.
- Kafka y MongoDB estaban corriendo.
- Spark arranco correctamente.
- Se resolvieron las dependencias Kafka de Spark.
- Se creo `SparkSession` correctamente.
- El job inicio con:
  - `bootstrap=kafka:9092`
  - `topic=heart-records`
  - `checkpoint=/tmp/kafkamed/checkpoints/heart-predictions`
  - `trigger=availableNow`

### Conteo en MongoDB
```powershell
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('kafkamed_db').heart_predictions.countDocuments({})"
```

Resultado:

```text
25
```

## 5. Evidencia del batch
Resultado del microbatch procesado por Spark:

- `Batch id`: `0`
- Filas recibidas: `25`
- Filas insertadas: `25`
- Coincidencias/deduplicadas: `0`

Aunque se publico un lote reciente de 5 eventos, el job proceso 25 registros porque el stream esta configurado con `SPARK_STARTING_OFFSETS=earliest`. Esto es comportamiento esperado: Spark leyo todos los mensajes disponibles desde el inicio del topic y no solo el ultimo lote publicado.

## 6. Documento de muestra en MongoDB
La coleccion `heart_predictions` devolvio un documento real con campos clinicos, metadatos de Kafka y salida del modelo.

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
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `source_file`
- `row_number`
- `prediction`
- `prediction_label`
- `probability`
- `model_version`
- `processed_at`
- `inserted_at`
- `ingestion_batch_id`

Ejemplo resumido sin datos sensibles:

```json
{
  "prediction": 0,
  "prediction_label": "no_risk",
  "probability": 0.034237246756154346,
  "model_version": "logistic_regression_complete_dataset_v1",
  "processed_at": "<timestamp>"
}
```

## 7. Problemas corregidos antes del cierre
Durante la validacion previa se detecto incompatibilidad entre la version de `scikit-learn` usada para serializar el modelo y la version instalada en Docker. El entorno fue corregido para permitir cargar el archivo `joblib` dentro del contenedor.

## 8. Estado final
**Fase 5 aprobada operativamente.**

Quedo validado el flujo:

```text
Kafka -> Spark Structured Streaming -> ML -> MongoDB
```

## 9. Proximo paso recomendado
La siguiente fase recomendada es implementar la API Flask clinica para exponer los datos de MongoDB, sin avanzar todavia a Power BI.
