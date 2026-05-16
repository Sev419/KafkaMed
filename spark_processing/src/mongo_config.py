"""MongoDB configuration helpers for KafkaMed."""

from __future__ import annotations

from dataclasses import dataclass
import os


DEFAULT_MONGO_URI = "mongodb://mongo:27017"
DEFAULT_MONGO_DB_NAME = "kafkamed_db"
DEFAULT_MONGO_COLLECTION = "heart_records"
DEFAULT_MONGO_TIMEOUT_MS = 5000


@dataclass(frozen=True)
class MongoConfig:
    """Resolved MongoDB configuration for Spark structured streaming."""

    uri: str
    db_name: str
    collection_name: str
    server_selection_timeout_ms: int


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def load_mongo_config() -> MongoConfig:
    """Load MongoDB settings from environment variables."""
    return MongoConfig(
        uri=os.environ.get("MONGO_URI", DEFAULT_MONGO_URI),
        db_name=os.environ.get("MONGO_DB_NAME", DEFAULT_MONGO_DB_NAME),
        collection_name=os.environ.get("MONGO_COLLECTION", DEFAULT_MONGO_COLLECTION),
        server_selection_timeout_ms=_env_int("MONGO_SERVER_SELECTION_TIMEOUT_MS", DEFAULT_MONGO_TIMEOUT_MS),
    )
