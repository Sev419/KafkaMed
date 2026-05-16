"""Kafka producer for the KafkaMed Fase 1 ingestion step.

This script reads a CSV file row by row and publishes each clinical record as a
JSON message into the Kafka topic configured via environment variables.

Fase 1 intentionally keeps the scope small:
- no Spark consumer yet
- no risk model yet
- no Flask clinical API yet
- no Power BI yet

The producer is designed to run from the repository root. It fails fast if the
CSV does not exist or if Kafka is unreachable.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = PROJECT_ROOT / "data" / "raw" / "heart_failure_prediction.csv"
DEFAULT_TOPIC = "heart-records"
LOGGER = logging.getLogger("kafkamed.producer")

EXPECTED_COLUMNS = {
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
    "HeartDisease",
}

INTEGER_FIELDS = {
    "Age",
    "RestingBP",
    "Cholesterol",
    "FastingBS",
    "MaxHR",
    "HeartDisease",
}

FLOAT_FIELDS = {"Oldpeak"}


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable with fallback."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    """Read a float environment variable with fallback."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def setup_logger() -> None:
    """Configure a simple console logger."""
    LOGGER.handlers.clear()
    LOGGER.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)


def resolve_path(raw_path: str | None) -> Path:
    """Resolve a path relative to the repository root."""
    candidate = Path(raw_path or str(DEFAULT_DATASET))
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def normalize_value(column: str, value: str | None) -> Any:
    """Convert CSV text values into JSON-friendly Python values."""
    if value is None:
        return None

    cleaned = value.strip()
    if cleaned == "":
        return None

    if column in INTEGER_FIELDS:
        try:
            return int(float(cleaned))
        except ValueError:
            return cleaned

    if column in FLOAT_FIELDS:
        try:
            return float(cleaned)
        except ValueError:
            return cleaned

    return cleaned


def iter_clinical_records(dataset_path: Path, limit: int | None = None) -> Iterable[dict[str, Any]]:
    """Yield normalized CSV records one by one."""
    with dataset_path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        fieldnames = set(reader.fieldnames or [])
        missing = EXPECTED_COLUMNS.difference(fieldnames)
        if missing:
            raise ValueError(
                "El CSV clinico no tiene las columnas esperadas. Faltan: "
                f"{sorted(missing)}. Archivo: {dataset_path}"
            )

        for index, row in enumerate(reader, start=1):
            if limit is not None and limit > 0 and index > limit:
                break

            document = {column: normalize_value(column, row.get(column)) for column in reader.fieldnames or []}
            document["source_file"] = dataset_path.name
            document["row_number"] = index
            yield document


def build_producer(bootstrap_servers: str):
    """Create and validate a Kafka producer lazily."""
    try:
        from confluent_kafka import Producer
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Falta la dependencia confluent-kafka. Ejecute 'python -m pip install -r requirements.txt'."
        ) from exc

    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "kafkamed-producer",
        "message.timeout.ms": 10000,
        "socket.timeout.ms": 10000,
        "retries": 3,
    }

    producer = Producer(config)
    LOGGER.info("Conectando a Kafka en %s", bootstrap_servers)
    metadata = producer.list_topics(timeout=10.0)
    LOGGER.info(
        "Kafka disponible. Cluster ID: %s | brokers: %s",
        getattr(metadata, "cluster_id", "<desconocido>"),
        len(metadata.brokers),
    )
    return producer


def delivery_report(err, msg, row_number: int | None = None) -> None:  # noqa: ANN001 - callback signature from confluent_kafka
    """Log Kafka delivery outcomes."""
    row_label = f"fila {row_number}" if row_number is not None else "registro"
    if err is not None:
        LOGGER.error(
            "Fallo la entrega de %s al topic=%s particion=%s: %s",
            row_label,
            msg.topic(),
            msg.partition(),
            err,
        )
        return

    LOGGER.info(
        "Registro entregado a Kafka (%s) topic=%s partition=%s offset=%s",
        row_label,
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def send_records(
    bootstrap_servers: str,
    topic: str,
    dataset_path: Path,
    interval_seconds: float,
    limit: int | None,
) -> int:
    """Read the CSV and publish each record to Kafka."""
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"No existe el CSV de entrada para KafkaMed: {dataset_path}. "
            "Descargue el dataset Heart Failure Prediction y ubique el archivo en esa ruta."
        )

    producer = build_producer(bootstrap_servers)
    total_sent = 0

    try:
        LOGGER.info("Leyendo CSV clinico: %s", dataset_path)
        for row_number, record in enumerate(iter_clinical_records(dataset_path, limit=limit), start=1):
            payload = json.dumps(record, ensure_ascii=False)
            try:
                producer.produce(
                    topic=topic,
                    value=payload.encode("utf-8"),
                    callback=lambda err, msg, row=row_number: delivery_report(err, msg, row),
                )
                producer.poll(0)
                total_sent += 1
                LOGGER.info("Registro enviado a Kafka (%s/%s)", total_sent, "limitado" if limit else "sin limite")
            except Exception as exc:  # noqa: BLE001 - controlado en frontera de ingestión
                LOGGER.exception("Error al enviar la fila %s a Kafka: %s", row_number, exc)
                raise

            if interval_seconds > 0:
                time.sleep(interval_seconds)

        producer.flush(30)
        LOGGER.info("Total de mensajes enviados a Kafka: %s", total_sent)
        return total_sent
    finally:
        producer.flush(5)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="KafkaMed producer: publica registros del dataset cardiaco en Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Direccion de Kafka, por ejemplo localhost:9092 o kafka:9092.",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC),
        help="Topic destino en Kafka.",
    )
    parser.add_argument(
        "--dataset",
        default=os.environ.get("HEART_DATASET_PATH", str(DEFAULT_DATASET)),
        help="Ruta al CSV de Heart Failure Prediction.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=env_float("PRODUCER_INTERVAL_SECONDS", 0.5),
        help="Pausa entre mensajes para simular streaming.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=env_int("PRODUCER_LIMIT", 0),
        help="Maximo numero de filas a publicar. 0 significa sin limite.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the Kafka producer."""
    setup_logger()
    args = parse_args()
    dataset_path = resolve_path(args.dataset)
    limit = None if args.limit <= 0 else args.limit

    LOGGER.info("Proyecto raiz: %s", PROJECT_ROOT)
    LOGGER.info("Dataset configurado: %s", dataset_path)
    LOGGER.info("Topic configurado: %s", args.topic)
    LOGGER.info("Bootstrap servers: %s", args.bootstrap_servers)

    try:
        total = send_records(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            dataset_path=dataset_path,
            interval_seconds=args.interval_seconds,
            limit=limit,
        )
    except FileNotFoundError as exc:
        LOGGER.error("%s", exc)
        return 2
    except RuntimeError as exc:
        LOGGER.error("%s", exc)
        return 3
    except Exception as exc:  # noqa: BLE001 - controlado para salida limpia
        LOGGER.exception("El productor Kafka fallo de forma controlada: %s", exc)
        return 1

    LOGGER.info("Proceso finalizado correctamente. Mensajes enviados: %s", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
