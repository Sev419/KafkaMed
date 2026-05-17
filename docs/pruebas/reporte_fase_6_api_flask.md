# Reporte de validacion Fase 6: API Flask clinica

## 1. Objetivo
Implementar y validar una API Flask clinica que consulte MongoDB y exponga los datos reales de la coleccion `heart_predictions`.

La fase cubre los endpoints exigidos por la actividad:
- `/patients`
- `/predictions`
- `/stats`
- `/risk-summary`

Tambien se agrego `/health` y `/` para operacion y descubrimiento basico del servicio.

## 2. Fuente de datos
- Motor: MongoDB
- Database: `kafkamed_db`
- Coleccion: `heart_predictions`
- Registros disponibles durante la validacion: `25`

Los documentos consultados contienen campos clinicos, metadatos Kafka y salida del modelo ML:
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
- `prediction`
- `prediction_label`
- `probability`
- `model_version`
- `processed_at`
- `inserted_at`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `source_file`
- `row_number`

## 3. Endpoints implementados

### `GET /`
Retorna informacion basica del servicio y lista de endpoints.

### `GET /health`
Retorna estado del servicio y conectividad MongoDB.

Respuesta validada:
```json
{
  "mongo": "ok",
  "service": "KafkaMed API",
  "status": "ok"
}
```

### `GET /patients`
Consulta registros clinicos base con paginacion.

Parametros:
- `limit`, default `50`
- `skip`, default `0`

Validacion:
```powershell
Invoke-RestMethod "http://localhost:8000/patients?limit=2"
```

Resultado resumido:
- `count`: `2`
- campos clinicos serializados correctamente
- `ObjectId` convertido a `id` string

### `GET /predictions`
Consulta predicciones con campos clinicos, metadatos Kafka y salida ML.

Parametros:
- `limit`, default `50`
- `skip`, default `0`
- `prediction_label`, opcional: `risk` o `no_risk`

Validaciones:
```powershell
Invoke-RestMethod "http://localhost:8000/predictions?limit=2"
Invoke-RestMethod "http://localhost:8000/predictions?prediction_label=risk&limit=1"
```

Resultado resumido:
- filtro `prediction_label=risk` funcionando
- fechas `processed_at`, `inserted_at` y `kafka_timestamp` serializadas como texto ISO

### `GET /stats`
Calcula estadisticas agregadas de `heart_predictions`.

Respuesta real:
```json
{
  "average_probability": 0.26713404364674964,
  "latest_processed_at": "2026-05-17T01:54:02.509000",
  "model_versions": ["logistic_regression_complete_dataset_v1"],
  "total": 25,
  "total_no_risk": 19,
  "total_risk": 6
}
```

### `GET /risk-summary`
Entrega resumen ejecutivo para consumo de dashboard.

Respuesta real resumida:
```json
{
  "distribution": {
    "no_risk": 19,
    "risk": 6
  },
  "no_risk": 19,
  "risk": 6,
  "risk_percentage": 24.0,
  "total_records": 25
}
```

El endpoint tambien retorna `top_high_probability` con los cinco registros de mayor probabilidad.

## 4. Variables de entorno
- `API_HOST`
- `API_PORT`
- `MONGO_URI`
- `MONGO_DB_NAME`
- `MONGO_PREDICTIONS_COLLECTION`

Valores usados en Docker:
- `API_HOST=0.0.0.0`
- `API_PORT=8000`
- `MONGO_URI=mongodb://mongo:27017`
- `MONGO_DB_NAME=kafkamed_db`
- `MONGO_PREDICTIONS_COLLECTION=heart_predictions`

Para ejecucion local se puede usar:
- `MONGO_URI=mongodb://localhost:27017`

## 5. Archivos creados
- `api_flask/__init__.py`
- `api_flask/config.py`
- `api_flask/mongo_client.py`
- `api_flask/repository.py`
- `api_flask/app.py`
- `docker/api/Dockerfile`
- `tests/test_api_structure.py`

## 6. Archivos modificados
- `.env.example`
- `docker-compose.yml`
- `tests/test_project_structure.py`
- `README.md`

## 7. Comandos ejecutados

### Validacion estatica
```powershell
python -m py_compile api_flask/app.py
python -m py_compile api_flask/config.py
python -m py_compile api_flask/repository.py
python -m py_compile api_flask/mongo_client.py
python -m pytest tests
```

Resultado:
- `py_compile`: OK
- `pytest`: OK, `5 passed`

### Docker
```powershell
docker compose config
docker compose up -d --build api
docker compose ps
```

Resultado:
- Compose valido.
- `kafkamed-api` levantado en `0.0.0.0:8000`.
- `kafkamed-mongo` healthy.

Problema encontrado y corregido:
- El primer arranque de API fallo con `ModuleNotFoundError: No module named 'api_flask'` porque Docker ejecutaba `python api_flask/app.py`.
- Se corrigio `docker/api/Dockerfile` para ejecutar `python -m api_flask.app`.

### Endpoints
```powershell
Invoke-RestMethod http://localhost:8000/
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod "http://localhost:8000/patients?limit=2"
Invoke-RestMethod "http://localhost:8000/predictions?limit=2"
Invoke-RestMethod "http://localhost:8000/predictions?prediction_label=risk&limit=1"
Invoke-RestMethod http://localhost:8000/stats
Invoke-RestMethod http://localhost:8000/risk-summary
```

Resultado:
- Todos los endpoints respondieron correctamente.
- La API leyo datos reales desde MongoDB.
- No hubo errores de serializacion de `ObjectId` ni fechas.

## 8. Estado final
**Fase 6 aprobada operativamente.**

Quedo validado:

```text
MongoDB heart_predictions -> Flask API
```

## 9. Proximo paso recomendado
Avanzar a Power BI conectando el dashboard a la API Flask, no directamente a MongoDB.
