"""MongoDB client helpers for the KafkaMed Flask API."""

from __future__ import annotations

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from api_flask.config import ApiConfig, load_config


def create_mongo_client(config: ApiConfig | None = None) -> MongoClient:
    """Create a MongoDB client using API configuration."""
    active_config = config or load_config()
    return MongoClient(
        active_config.mongo_uri,
        serverSelectionTimeoutMS=active_config.server_selection_timeout_ms,
    )


def get_database(config: ApiConfig | None = None):
    """Return the configured KafkaMed MongoDB database."""
    active_config = config or load_config()
    return create_mongo_client(active_config)[active_config.mongo_db_name]


def check_mongo_health(config: ApiConfig | None = None) -> tuple[bool, str | None]:
    """Check whether MongoDB is reachable."""
    client = None
    try:
        active_config = config or load_config()
        client = create_mongo_client(active_config)
        client.admin.command("ping")
        return True, None
    except PyMongoError as exc:
        return False, str(exc)
    finally:
        if client is not None:
            client.close()
