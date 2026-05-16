"""KafkaMed standalone repository structure checks."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_required_directories_exist():
    required = [
        "kafka",
        "ml",
        "ml/models",
        "ml/metrics",
        "ml/reports",
        "spark_processing",
        "data",
        "docker",
        "docs",
        "tests",
    ]
    missing = [path for path in required if not (ROOT / path).exists()]
    assert not missing, f"Missing directories: {missing}"


def test_required_entrypoints_exist():
    required = [
        "kafka/__init__.py",
        "kafka/producer.py",
        "ml/__init__.py",
        "ml/config.py",
        "ml/train_heart_model.py",
        "ml/predict_heart_risk.py",
        "spark_processing/src/__init__.py",
        "spark_processing/src/heart_schema.py",
        "spark_processing/src/kafka_config.py",
        "spark_processing/src/mongo_config.py",
        "spark_processing/src/mongo_writer.py",
        "spark_processing/src/spark_session_factory.py",
        "spark_processing/jobs/smoke_spark_session.py",
        "spark_processing/jobs/process_heart_records_stream.py",
        "spark_processing/jobs/process_heart_records_to_mongo.py",
        "docker/spark/Dockerfile",
        "docker-compose.yml",
        "requirements.txt",
        ".env.example",
        "README.md",
        "docs/arquitectura/arquitectura_kafkamed.md",
        "docs/pruebas/reporte_fase_1_kafka_producer.md",
        "docs/pruebas/reporte_fase_2_spark_kafka.md",
        "docs/pruebas/reporte_separacion_repositorio.md",
        "docs/pruebas/reporte_fase_4_modelo_ml.md",
        "data/raw/heart_failure_prediction.csv",
    ]
    missing = [path for path in required if not (ROOT / path).exists()]
    assert not missing, f"Missing files: {missing}"
