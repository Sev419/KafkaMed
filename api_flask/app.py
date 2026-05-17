"""KafkaMed clinical Flask API."""

from __future__ import annotations

from flask import Flask, jsonify, request
from pymongo.errors import PyMongoError

from api_flask.config import load_config
from api_flask.mongo_client import check_mongo_health
from api_flask.repository import get_patients, get_predictions, get_risk_summary, get_stats


def create_app() -> Flask:
    """Create and configure the KafkaMed Flask application."""
    app = Flask(__name__)
    config = load_config()

    @app.get("/")
    def index():
        return jsonify(
            {
                "service": "KafkaMed API",
                "status": "ok",
                "endpoints": ["/health", "/patients", "/predictions", "/stats", "/risk-summary"],
            }
        )

    @app.get("/health")
    def health():
        mongo_ok, mongo_error = check_mongo_health(config)
        response = {
            "status": "ok" if mongo_ok else "degraded",
            "service": "KafkaMed API",
            "mongo": "ok" if mongo_ok else "error",
        }
        if mongo_error:
            response["mongo_error"] = mongo_error
        return jsonify(response), 200 if mongo_ok else 503

    @app.get("/patients")
    def patients():
        limit, skip = _pagination_args()
        try:
            return jsonify(get_patients(limit=limit, skip=skip, config=config))
        except PyMongoError as exc:
            return jsonify({"error": "mongo_error", "message": str(exc)}), 503

    @app.get("/predictions")
    def predictions():
        limit, skip = _pagination_args()
        prediction_label = request.args.get("prediction_label")
        if prediction_label and prediction_label not in {"risk", "no_risk"}:
            return jsonify({"error": "invalid_prediction_label", "allowed": ["risk", "no_risk"]}), 400
        try:
            return jsonify(
                get_predictions(
                    limit=limit,
                    skip=skip,
                    prediction_label=prediction_label,
                    config=config,
                )
            )
        except PyMongoError as exc:
            return jsonify({"error": "mongo_error", "message": str(exc)}), 503

    @app.get("/stats")
    def stats():
        try:
            return jsonify(get_stats(config=config))
        except PyMongoError as exc:
            return jsonify({"error": "mongo_error", "message": str(exc)}), 503

    @app.get("/risk-summary")
    def risk_summary():
        try:
            return jsonify(get_risk_summary(config=config))
        except PyMongoError as exc:
            return jsonify({"error": "mongo_error", "message": str(exc)}), 503

    return app


def _pagination_args() -> tuple[int, int]:
    limit = max(1, min(int(request.args.get("limit", "50")), 500))
    skip = max(0, int(request.args.get("skip", "0")))
    return limit, skip


app = create_app()


if __name__ == "__main__":
    api_config = load_config()
    app.run(host=api_config.host, port=api_config.port)
