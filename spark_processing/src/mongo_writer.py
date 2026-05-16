"""MongoDB writer helpers for KafkaMed streaming batches."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from spark_processing.src.mongo_config import MongoConfig, load_mongo_config


LOGGER = logging.getLogger("kafkamed.mongo_writer")


def _get_pymongo_components():
    """Import pymongo lazily to keep module import lightweight."""
    try:
        from pymongo import MongoClient, UpdateOne
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "No se pudo importar pymongo. Ejecute `python -m pip install -r requirements.txt`."
        ) from exc

    return MongoClient, UpdateOne


def _build_document(row: dict[str, Any], batch_id: int, position: int) -> dict[str, Any]:
    document = dict(row)
    document.setdefault("ingestion_batch_id", int(batch_id))
    document.setdefault("inserted_at", datetime.now(timezone.utc))
    document.setdefault("batch_row_number", position)
    return document


def write_microbatch_to_mongo(batch_df, batch_id: int, config: MongoConfig | None = None, logger: logging.Logger | None = None) -> int:
    """Write a Spark microbatch to MongoDB using idempotent upserts."""
    active_logger = logger or LOGGER
    mongo_config = config or load_mongo_config()
    MongoClient, UpdateOne = _get_pymongo_components()

    records = [row.asDict(recursive=True) for row in batch_df.collect()]
    active_logger.info("Batch recibido: id=%s filas=%s", batch_id, len(records))
    if not records:
        active_logger.info("Batch vacio. No se escriben documentos en MongoDB.")
        return 0

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
            name="uniq_kafka_partition_offset",
        )

        operations = []
        for position, row in enumerate(records, start=1):
            document = _build_document(row, batch_id, position)
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

        result = collection.bulk_write(operations, ordered=False)
        inserted = result.upserted_count
        active_logger.info(
            "Batch persistido en MongoDB: id=%s recibidas=%s insertadas=%s coincidentes=%s",
            batch_id,
            len(records),
            inserted,
            result.matched_count,
        )
        return int(inserted)
    except Exception:
        active_logger.exception("Error al escribir batch %s en MongoDB.", batch_id)
        raise
    finally:
        if client is not None:
            client.close()
