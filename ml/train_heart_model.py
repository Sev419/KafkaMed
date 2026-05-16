"""Train a heart disease classifier for KafkaMed."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sys

import pandas as pd
from joblib import dump
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml.config import (
    CATEGORICAL_COLUMNS,
    DATASET_PATH,
    DEFAULT_RANDOM_STATE,
    DEFAULT_TEST_SIZE,
    EXPECTED_COLUMNS,
    FEATURE_COLUMNS,
    DOCS_REPORT_PATH,
    MIN_ROWS_FOR_TRAINING,
    METRICS_PATH,
    MODEL_PATH,
    NUMERIC_COLUMNS,
    REPORT_PATH,
    TARGET_COLUMN,
)


LOGGER = logging.getLogger("kafkamed.ml.train")


def setup_logging() -> None:
    if LOGGER.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


def ensure_output_dirs() -> None:
    for directory in (MODEL_PATH.parent, METRICS_PATH.parent, REPORT_PATH.parent):
        directory.mkdir(parents=True, exist_ok=True)


def load_dataset(dataset_path: Path) -> pd.DataFrame:
    if not dataset_path.exists():
        raise FileNotFoundError(f"Clinical CSV not found: {dataset_path}")

    df = pd.read_csv(dataset_path)
    missing = [column for column in EXPECTED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


def build_pipeline() -> Pipeline:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, NUMERIC_COLUMNS),
            ("cat", categorical_pipeline, CATEGORICAL_COLUMNS),
        ]
    )
    classifier = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        solver="liblinear",
        random_state=DEFAULT_RANDOM_STATE,
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )


def choose_test_size(dataset_size: int) -> float:
    if dataset_size < 10:
        return DEFAULT_TEST_SIZE
    if dataset_size < 50:
        return 0.3
    return 0.2


def split_dataset(
    X: pd.DataFrame,
    y: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, float]:
    test_size = choose_test_size(len(X))
    stratify = y if y.nunique() > 1 and y.value_counts().min() >= 2 else None

    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=DEFAULT_RANDOM_STATE,
            stratify=stratify,
        )
    except ValueError as exc:
        LOGGER.warning("Stratified split failed (%s). Retrying without stratification.", exc)
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=DEFAULT_RANDOM_STATE,
            stratify=None,
        )

    return X_train, X_test, y_train, y_test, test_size


def safe_roc_auc(y_true: pd.Series, y_prob: pd.Series | list[float]) -> float | None:
    if y_true.nunique() < 2:
        return None
    try:
        return float(roc_auc_score(y_true, y_prob))
    except ValueError as exc:
        LOGGER.warning("ROC AUC could not be computed: %s", exc)
        return None


def build_report(dataset_info: dict[str, Any], metrics: dict[str, Any], output_path: Path) -> str:
    lines = [
        "# KafkaMed heart disease model report",
        "",
        "## Dataset",
        f"- Rows: {dataset_info['rows']}",
        f"- Columns: {', '.join(dataset_info['columns'])}",
        f"- Target distribution: {dataset_info['class_distribution']}",
        f"- Train rows: {dataset_info['train_rows']}",
        f"- Test rows: {dataset_info['test_rows']}",
        f"- Test size: {dataset_info['test_size']}",
        "",
        "## Model",
        "- LogisticRegression",
        "- Preprocessing: median imputation + StandardScaler for numeric features, most-frequent imputation + OneHotEncoder for categorical features",
        "- class_weight=balanced",
        "",
        "## Metrics",
        f"- Accuracy: {metrics['accuracy']}",
        f"- Precision: {metrics['precision']}",
        f"- Recall: {metrics['recall']}",
        f"- F1: {metrics['f1']}",
        f"- ROC AUC: {metrics['roc_auc']}",
        "",
        "## Limitations",
        "- The local CSV extract is very small, so the metrics are only a technical validation of the pipeline.",
        "- This model is not yet integrated with Spark streaming, MongoDB, Flask, or Power BI.",
        "",
        f"## Model artifact",
        f"- Saved at: `{MODEL_PATH}`",
    ]
    content = "\n".join(lines) + "\n"
    output_path.write_text(content, encoding="utf-8")
    return content


def train_model(dataset_path: Path) -> dict[str, Any]:
    ensure_output_dirs()
    df = load_dataset(dataset_path)

    if len(df) < MIN_ROWS_FOR_TRAINING:
        LOGGER.warning(
            "Dataset is very small (%s rows). Training will be used as a technical validation, not as a production model.",
            len(df),
        )

    class_distribution = df[TARGET_COLUMN].value_counts(dropna=False).to_dict()
    if len(class_distribution) < 2:
        raise ValueError(
            f"The target {TARGET_COLUMN} must contain at least two classes. Current distribution: {class_distribution}"
        )

    X = df[FEATURE_COLUMNS].copy()
    y = df[TARGET_COLUMN].astype(int).copy()

    X_train, X_test, y_train, y_test, test_size = split_dataset(X, y)

    LOGGER.info("Dataset size: %s rows", len(df))
    LOGGER.info("Used columns: %s", FEATURE_COLUMNS)
    LOGGER.info("Class distribution: %s", class_distribution)
    LOGGER.info("Train rows: %s | Test rows: %s | test_size=%.2f", len(X_train), len(X_test), test_size)

    model = build_pipeline()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    probabilities = model.predict_proba(X_test)
    class_labels = list(model.named_steps["classifier"].classes_)
    positive_class_index = class_labels.index(1)
    positive_probabilities = probabilities[:, positive_class_index]

    metrics = {
        "accuracy": float(accuracy_score(y_test, predictions)),
        "precision": float(precision_score(y_test, predictions, zero_division=0)),
        "recall": float(recall_score(y_test, predictions, zero_division=0)),
        "f1": float(f1_score(y_test, predictions, zero_division=0)),
        "roc_auc": safe_roc_auc(y_test, positive_probabilities),
    }

    dataset_info = {
        "rows": int(len(df)),
        "columns": FEATURE_COLUMNS,
        "class_distribution": class_distribution,
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "test_size": test_size,
    }
    model_info = {
        "model_name": "LogisticRegression",
        "class_weight": "balanced",
        "solver": "liblinear",
        "random_state": DEFAULT_RANDOM_STATE,
        "features": FEATURE_COLUMNS,
    }
    artifact_info = {
        "model_path": str(MODEL_PATH),
        "metrics_path": str(METRICS_PATH),
        "report_path": str(REPORT_PATH),
    }
    payload = {
        "dataset": dataset_info,
        "model": model_info,
        "metrics": metrics,
        "artifacts": artifact_info,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    dump(model, MODEL_PATH)
    METRICS_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    report_content = build_report(dataset_info, metrics, REPORT_PATH)
    DOCS_REPORT_PATH.write_text(report_content, encoding="utf-8")

    LOGGER.info("Metrics calculated: %s", metrics)
    LOGGER.info("Model saved at: %s", MODEL_PATH)
    LOGGER.info("Metrics saved at: %s", METRICS_PATH)
    LOGGER.info("Report saved at: %s", REPORT_PATH)
    LOGGER.info("Docs report saved at: %s", DOCS_REPORT_PATH)

    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the KafkaMed heart disease model.")
    parser.add_argument("--dataset", type=Path, default=DATASET_PATH, help="Path to the clinical CSV dataset.")
    return parser.parse_args()


def main() -> int:
    setup_logging()
    args = parse_args()
    try:
        train_model(args.dataset)
    except Exception as exc:  # pragma: no cover - explicit failure path for CLI
        LOGGER.error("Training failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
