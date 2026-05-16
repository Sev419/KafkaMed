"""Kafka configuration helpers for the KafkaMed streaming consumer."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_KAFKA_TOPIC = "heart-records"
DEFAULT_SPARK_STARTING_OFFSETS = "earliest"
DEFAULT_CHECKPOINT_LOCATION = PROJECT_ROOT / "data" / "streaming" / "checkpoints" / "heart-records"
DEFAULT_SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


@dataclass(frozen=True)
class KafkaStreamConfig:
    """Resolved Kafka streaming configuration for the Spark consumer."""

    bootstrap_servers: str
    topic: str
    checkpoint_location: str
    starting_offsets: str
    spark_packages: str
    trigger_mode: str
    timeout_seconds: int


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def load_kafka_stream_config() -> KafkaStreamConfig:
    """Load Kafka streaming settings from environment variables."""
    return KafkaStreamConfig(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
        topic=os.environ.get("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC),
        checkpoint_location=os.environ.get(
            "SPARK_CHECKPOINT_LOCATION",
            str(DEFAULT_CHECKPOINT_LOCATION),
        ),
        starting_offsets=os.environ.get("SPARK_STARTING_OFFSETS", DEFAULT_SPARK_STARTING_OFFSETS),
        spark_packages=os.environ.get("SPARK_JARS_PACKAGES", DEFAULT_SPARK_PACKAGES),
        trigger_mode=os.environ.get("STREAM_TRIGGER_MODE", "availableNow"),
        timeout_seconds=_env_int("STREAM_TIMEOUT_SECONDS", 120),
    )
