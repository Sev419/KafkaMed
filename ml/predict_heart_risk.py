"""Local inference helper for the KafkaMed heart disease model."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any
import sys

import pandas as pd
from joblib import load

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml.config import DATASET_PATH, FEATURE_COLUMNS, MODEL_PATH


LOGGER = logging.getLogger("kafkamed.ml.predict")


def setup_logging() -> None:
    if LOGGER.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


def load_model(model_path: Path = MODEL_PATH):
    if not model_path.exists():
        raise FileNotFoundError(f"Trained model not found at: {model_path}")
    return load(model_path)


def predict_heart_risk_with_model(record: dict[str, Any], model) -> dict[str, Any]:
    missing = [column for column in FEATURE_COLUMNS if column not in record]
    if missing:
        raise ValueError(f"Missing clinical fields for inference: {missing}")

    features = pd.DataFrame([record], columns=FEATURE_COLUMNS)
    prediction = int(model.predict(features)[0])
    probabilities = model.predict_proba(features)[0]
    classes = list(model.named_steps["classifier"].classes_)
    positive_index = classes.index(1)
    probability = float(probabilities[positive_index])

    return {
        "prediction": prediction,
        "prediction_label": "risk" if prediction == 1 else "no_risk",
        "probability": probability,
    }


def predict_heart_risk(record: dict[str, Any], model_path: Path = MODEL_PATH) -> dict[str, Any]:
    model = load_model(model_path)
    return predict_heart_risk_with_model(record, model)


def predict_heart_risk_batch(records: list[dict[str, Any]], model_path: Path = MODEL_PATH) -> list[dict[str, Any]]:
    model = load_model(model_path)
    return [predict_heart_risk_with_model(record, model) for record in records]


def load_sample_record(dataset_path: Path = DATASET_PATH) -> dict[str, Any]:
    if not dataset_path.exists():
        raise FileNotFoundError(f"Clinical CSV not found: {dataset_path}")
    df = pd.read_csv(dataset_path)
    if df.empty:
        raise ValueError("The clinical dataset is empty.")
    return df.iloc[0][FEATURE_COLUMNS].to_dict()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predict heart disease risk locally.")
    parser.add_argument("--input-json", type=str, help="JSON string with clinical features.")
    parser.add_argument("--model-path", type=Path, default=MODEL_PATH, help="Path to the trained joblib model.")
    return parser.parse_args()


def main() -> int:
    setup_logging()
    args = parse_args()
    try:
        record = json.loads(args.input_json) if args.input_json else load_sample_record()
        result = predict_heart_risk(record, args.model_path)
        LOGGER.info("Local prediction: %s", result)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as exc:  # pragma: no cover - explicit CLI failure
        LOGGER.error("Local inference failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
