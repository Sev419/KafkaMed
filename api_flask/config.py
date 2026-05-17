"""Configuration helpers for the KafkaMed Flask API."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ApiConfig:
    host: str
    port: int
    mongo_uri: str
    mongo_db_name: str
    predictions_collection: str
    server_selection_timeout_ms: int


def load_config() -> ApiConfig:
    """Load API and MongoDB configuration from environment variables."""
    return ApiConfig(
        host=os.environ.get("API_HOST", "0.0.0.0"),
        port=int(os.environ.get("API_PORT", "8000")),
        mongo_uri=os.environ.get("MONGO_URI", "mongodb://mongo:27017"),
        mongo_db_name=os.environ.get("MONGO_DB_NAME", "kafkamed_db"),
        predictions_collection=os.environ.get("MONGO_PREDICTIONS_COLLECTION", "heart_predictions"),
        server_selection_timeout_ms=int(os.environ.get("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000")),
    )
