"""MongoDB read repository for KafkaMed clinical predictions."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from bson import ObjectId

from api_flask.config import ApiConfig, load_config
from api_flask.mongo_client import create_mongo_client


CLINICAL_FIELDS = [
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
    "source_file",
    "row_number",
]

PREDICTION_FIELDS = [
    *CLINICAL_FIELDS,
    "prediction",
    "prediction_label",
    "probability",
    "model_version",
    "processed_at",
    "inserted_at",
    "ingestion_batch_id",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
]


def serialize_value(value: Any) -> Any:
    """Convert MongoDB/Python values into JSON-safe values."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_value(item) for key, item in value.items()}
    return value


def serialize_document(document: dict[str, Any]) -> dict[str, Any]:
    """Serialize one MongoDB document for Flask JSON responses."""
    serialized = serialize_value(document)
    if "_id" in serialized:
        serialized["id"] = serialized.pop("_id")
    return serialized


def _projection(fields: list[str]) -> dict[str, int]:
    projection = {field: 1 for field in fields}
    projection["_id"] = 1
    return projection


def _collection(config: ApiConfig | None = None):
    active_config = config or load_config()
    client = create_mongo_client(active_config)
    return client, client[active_config.mongo_db_name][active_config.predictions_collection]


def get_patients(limit: int = 50, skip: int = 0, config: ApiConfig | None = None) -> dict[str, Any]:
    """Return clinical records from prediction documents."""
    client, collection = _collection(config)
    try:
        cursor = (
            collection.find({}, _projection(CLINICAL_FIELDS))
            .sort([("processed_at", -1), ("kafka_offset", -1)])
            .skip(skip)
            .limit(limit)
        )
        items = [serialize_document(document) for document in cursor]
        return {"items": items, "limit": limit, "skip": skip, "count": len(items)}
    finally:
        client.close()


def get_predictions(
    limit: int = 50,
    skip: int = 0,
    prediction_label: str | None = None,
    config: ApiConfig | None = None,
) -> dict[str, Any]:
    """Return prediction documents with optional label filtering."""
    query: dict[str, Any] = {}
    if prediction_label:
        query["prediction_label"] = prediction_label

    client, collection = _collection(config)
    try:
        cursor = (
            collection.find(query, _projection(PREDICTION_FIELDS))
            .sort([("processed_at", -1), ("kafka_offset", -1)])
            .skip(skip)
            .limit(limit)
        )
        items = [serialize_document(document) for document in cursor]
        return {
            "items": items,
            "limit": limit,
            "skip": skip,
            "count": len(items),
            "prediction_label": prediction_label,
        }
    finally:
        client.close()


def get_stats(config: ApiConfig | None = None) -> dict[str, Any]:
    """Return aggregate statistics for stored heart-risk predictions."""
    client, collection = _collection(config)
    try:
        total = collection.count_documents({})
        total_risk = collection.count_documents({"prediction_label": "risk"})
        total_no_risk = collection.count_documents({"prediction_label": "no_risk"})

        aggregate = list(
            collection.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "average_probability": {"$avg": "$probability"},
                            "latest_processed_at": {"$max": "$processed_at"},
                            "model_versions": {"$addToSet": "$model_version"},
                        }
                    }
                ]
            )
        )
        summary = aggregate[0] if aggregate else {}

        return serialize_value(
            {
                "total": total,
                "total_risk": total_risk,
                "total_no_risk": total_no_risk,
                "average_probability": summary.get("average_probability"),
                "latest_processed_at": summary.get("latest_processed_at"),
                "model_versions": sorted(summary.get("model_versions", [])),
            }
        )
    finally:
        client.close()


def get_risk_summary(config: ApiConfig | None = None) -> dict[str, Any]:
    """Return an executive risk summary for dashboard/API consumers."""
    client, collection = _collection(config)
    try:
        total = collection.count_documents({})
        total_risk = collection.count_documents({"prediction_label": "risk"})
        total_no_risk = collection.count_documents({"prediction_label": "no_risk"})
        risk_percentage = (total_risk / total * 100) if total else 0.0

        distribution = {
            item["_id"] or "unknown": item["count"]
            for item in collection.aggregate(
                [{"$group": {"_id": "$prediction_label", "count": {"$sum": 1}}}]
            )
        }

        top_cursor = (
            collection.find({}, _projection(PREDICTION_FIELDS))
            .sort([("probability", -1), ("processed_at", -1)])
            .limit(5)
        )

        return serialize_value(
            {
                "total_records": total,
                "risk": total_risk,
                "no_risk": total_no_risk,
                "risk_percentage": risk_percentage,
                "distribution": distribution,
                "top_high_probability": [serialize_document(document) for document in top_cursor],
            }
        )
    finally:
        client.close()
