"""MongoDB writer helpers for KafkaMed heart-risk predictions."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any

from ml.predict_heart_risk import load_model, predict_heart_risk_with_model
from spark_processing.src.mongo_config import MongoConfig


LOGGER = logging.getLogger("kafkamed.prediction_writer")
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODEL_PATH = ROOT / "ml" / "models" / "heart_risk_model.joblib"
DEFAULT_MODEL_VERSION = "logistic_regression_complete_dataset_v1"
DEFAULT_PREDICTIONS_COLLECTION = "heart_predictions"


def _get_pymongo_components():
    """Import pymongo lazily to keep module import lightweight."""
    try:
        from pymongo import MongoClient, UpdateOne
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "No se pudo importar pymongo. Ejecute `python -m pip install -r requirements.txt`."
        ) from exc

    return MongoClient, UpdateOne


def _env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    if not value:
        return default
    return Path(value)


def _build_prediction_document(row: dict[str, Any], prediction: dict[str, Any], batch_id: int, position: int) -> dict[str, Any]:
    document = dict(row)
    document.update(
        {
            "prediction": int(prediction["prediction"]),
            "prediction_label": prediction["prediction_label"],
            "probability": float(prediction["probability"]),
            "model_version": os.environ.get("MODEL_VERSION", DEFAULT_MODEL_VERSION),
            "processed_at": datetime.now(timezone.utc),
            "ingestion_batch_id": int(batch_id),
            "inserted_at": datetime.now(timezone.utc),
            "batch_row_number": position,
        }
    )
    return document


def write_prediction_microbatch_to_mongo(
    batch_df,
    batch_id: int,
    config: MongoConfig | None = None,
    logger: logging.Logger | None = None,
) -> int:
    """Write a Spark microbatch to MongoDB with ML predictions."""
    active_logger = logger or LOGGER
    mongo_config = config or MongoConfig(
        uri=os.environ.get("MONGO_URI", "mongodb://mongo:27017"),
        db_name=os.environ.get("MONGO_DB_NAME", "kafkamed_db"),
        collection_name=os.environ.get("MONGO_PREDICTIONS_COLLECTION", DEFAULT_PREDICTIONS_COLLECTION),
        server_selection_timeout_ms=int(os.environ.get("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000")),
    )
    MongoClient, UpdateOne = _get_pymongo_components()

    records = [row.asDict(recursive=True) for row in batch_df.collect()]
    active_logger.info("Batch recibido para prediccion: id=%s filas=%s", batch_id, len(records))
    if not records:
        active_logger.info("Batch vacio. No se escriben predicciones en MongoDB.")
        return 0

    model_path = _env_path("MODEL_PATH", DEFAULT_MODEL_PATH)
    model = load_model(model_path)

    client = None
    try:
        client = MongoClient(
            mongo_config.uri,
            serverSelectionTimeoutMS=mongo_config.server_selection_timeout_ms,
        )
        client.admin.command("ping")
        collection = client[mongo_config.db_name][mongo_config.collection_name]
        collection.create_index(
            [("kafka_partition", 1), ("kafka_offset", 1)],
            unique=True,
            name="uniq_kafka_partition_offset_predictions",
        )

        operations = []
        inserted_candidates = 0
        for position, row in enumerate(records, start=1):
            clinical_row = {
                key: row.get(key)
                for key in [
                    "Age",
                    "Sex",
                    "ChestPainType",
                    "RestingBP",
                    "Cholesterol",
                    "FastingBS",
                    "RestingECG",
                    "MaxHR",
                    "ExerciseAngina",
                    "Oldpeak",
                    "ST_Slope",
                ]
            }
            prediction = predict_heart_risk_with_model(clinical_row, model)
            document = _build_prediction_document(row, prediction, batch_id, position)

            kafka_partition = document.get("kafka_partition")
            kafka_offset = document.get("kafka_offset")
            if kafka_partition is not None and kafka_offset is not None:
                filter_doc = {
                    "kafka_partition": kafka_partition,
                    "kafka_offset": kafka_offset,
                }
            else:
                filter_doc = {
                    "ingestion_batch_id": document["ingestion_batch_id"],
                    "batch_row_number": document["batch_row_number"],
                }

            operations.append(
                UpdateOne(
                    filter_doc,
                    {"$setOnInsert": document},
                    upsert=True,
                )
            )
            inserted_candidates += 1

        result = collection.bulk_write(operations, ordered=False)
        inserted = result.upserted_count
        active_logger.info(
            "Batch persistido con ML en MongoDB: id=%s recibidas=%s insertadas=%s coincidencias=%s",
            batch_id,
            len(records),
            inserted,
            result.matched_count,
        )
        if inserted_candidates != len(records):
            active_logger.warning(
                "Se generaron %s candidatos, pero el batch tenia %s filas. Revisar si hubo deduplicacion.",
                inserted_candidates,
                len(records),
            )
        return int(inserted)
    except Exception:
        active_logger.exception("Error al escribir predicciones del batch %s en MongoDB.", batch_id)
        raise
    finally:
        if client is not None:
            client.close()

