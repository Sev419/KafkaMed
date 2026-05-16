# KafkaMed

KafkaMed is the independent clinical streaming repository built from the
validated Kafka + Spark pattern. The project keeps the proven infrastructure
shape, but the sentiment domain is not part of this codebase anymore.

## Validated phases

- Kafka broker in Docker.
- Clinical CSV producer that publishes JSON messages to `heart-records`.
- Spark Structured Streaming consumer that reads Kafka and prints parsed rows.

## Structure

- `kafka/`: Kafka producer for the heart failure dataset.
- `spark_processing/`: Spark session helpers and the streaming consumer.
- `docker/`: Dockerfiles used by the streaming runtime.
- `docs/`: architecture notes and validation reports.
- `tests/`: structure checks for the standalone repository.

## Quick start

```powershell
docker compose up -d kafka
python kafka\producer.py --limit 5
docker compose --profile processing run --rm spark-heart-stream
```

## Validation commands

```powershell
python -m py_compile kafka\producer.py
python -m py_compile spark_processing\src\heart_schema.py
python -m py_compile spark_processing\src\kafka_config.py
python -m py_compile spark_processing\jobs\process_heart_records_stream.py
python -m pytest tests\test_project_structure.py
docker compose config
```
