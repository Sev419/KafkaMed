"""Lightweight checks for the KafkaMed ML phase."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_ml_artifacts_exist():
    required = [
        "ml/train_heart_model.py",
        "ml/predict_heart_risk.py",
        "ml/config.py",
        "ml/models",
        "ml/metrics",
        "ml/reports",
    ]
    missing = [path for path in required if not (ROOT / path).exists()]
    assert not missing, f"Missing ML artifacts: {missing}"


def test_ml_columns_documented():
    from ml.config import CATEGORICAL_COLUMNS, EXPECTED_COLUMNS, FEATURE_COLUMNS, NUMERIC_COLUMNS, TARGET_COLUMN

    assert TARGET_COLUMN == "HeartDisease"
    assert CATEGORICAL_COLUMNS == [
        "Sex",
        "ChestPainType",
        "RestingECG",
        "ExerciseAngina",
        "ST_Slope",
    ]
    assert NUMERIC_COLUMNS == [
        "Age",
        "RestingBP",
        "Cholesterol",
        "FastingBS",
        "MaxHR",
        "Oldpeak",
    ]
    assert FEATURE_COLUMNS == NUMERIC_COLUMNS + CATEGORICAL_COLUMNS
    assert EXPECTED_COLUMNS == [
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
