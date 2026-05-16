"""Shared configuration for the KafkaMed heart disease ML phase."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

DATASET_PATH = ROOT / "data" / "raw" / "heart_failure_prediction.csv"
MODEL_PATH = ROOT / "ml" / "models" / "heart_risk_model.joblib"
METRICS_PATH = ROOT / "ml" / "metrics" / "heart_model_metrics.json"
REPORT_PATH = ROOT / "ml" / "reports" / "heart_model_report.md"
DOCS_REPORT_PATH = ROOT / "docs" / "pruebas" / "reporte_fase_4_modelo_ml.md"

TARGET_COLUMN = "HeartDisease"
EXPECTED_COLUMNS = [
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
    TARGET_COLUMN,
]

CATEGORICAL_COLUMNS = [
    "Sex",
    "ChestPainType",
    "RestingECG",
    "ExerciseAngina",
    "ST_Slope",
]

NUMERIC_COLUMNS = [
    "Age",
    "RestingBP",
    "Cholesterol",
    "FastingBS",
    "MaxHR",
    "Oldpeak",
]

FEATURE_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS

DEFAULT_RANDOM_STATE = 42
DEFAULT_TEST_SIZE = 0.4
MIN_ROWS_FOR_TRAINING = 5
