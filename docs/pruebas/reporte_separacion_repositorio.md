# Reporte de separacion de repositorio

## 1. Por que se separo KafkaMed

KafkaMed se separo para mantener un repositorio independiente, con nombres y
documentacion propias del dominio clinico. El proyecto reutiliza la base
tecnica probada, pero el dominio de sentimientos ya no forma parte del
repositorio nuevo.

## 2. Estado alcanzado en esta sesion

- Se creo la estructura base de KafkaMed en el repositorio hermano.
- Se copiaron los componentes reutilizables de Kafka y Spark.
- Se ajustaron `docker-compose.yml`, `requirements.txt`, `.env.example`,
  `README.md` y la prueba de estructura para que el repo ya no dependa de
  SentimentStream.
- `python -m py_compile` paso sobre los entrypoints principales.
- `python -m pytest tests/test_project_structure.py` paso con 2 pruebas.
- `docker compose config` valido la definicion del proyecto como `kafkamed`.
- `python kafka/producer.py --help` mostro la CLI correctamente.

## 3. Bloqueo restante

- `docker version` y `docker ps` siguen devolviendo
  `permission denied while trying to connect to the docker API`.
- Se probo el contexto `desktop-linux` y tambien el pipe directo
  `npipe:////./pipe/docker_engine`.
- Docker Desktop esta abierto y sus procesos existen, pero esta sesion no
  logra hablar con el daemon.

## 4. Archivos reutilizados

- `kafka/producer.py`
- `kafka/__init__.py`
- `spark_processing/jobs/process_heart_records_stream.py`
- `spark_processing/jobs/smoke_spark_session.py`
- `spark_processing/src/heart_schema.py`
- `spark_processing/src/kafka_config.py`
- `spark_processing/src/spark_session_factory.py`
- `data/raw/heart_failure_prediction.csv`
- `docker/spark/Dockerfile`
- `docker-compose.yml`
- `requirements.txt`
- `.env.example`
- `tests/test_project_structure.py`
- `docs/arquitectura/arquitectura_kafkamed.md`

## 5. Archivos excluidos

- `api_flask/`
- `database/`
- `alerts/`
- `dashboard/`
- `jenkins/`
- `infra/`
- `spark/`

## 6. Pendientes

- Reactivar el acceso al daemon Docker desde esta sesion.
- Levantar KafkaMed en Docker.
- Correr `python kafka/producer.py --limit 5`.
- Correr `docker compose --profile processing build spark-heart-stream`.
- Correr `docker compose --profile processing run --rm spark-heart-stream`.
- Despues avanzar a MongoDB.
