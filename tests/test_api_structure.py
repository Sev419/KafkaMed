"""Structure checks for the KafkaMed Flask API."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_api_files_exist():
    required = [
        "api_flask/__init__.py",
        "api_flask/app.py",
        "api_flask/config.py",
        "api_flask/mongo_client.py",
        "api_flask/repository.py",
        "docker/api/Dockerfile",
    ]
    missing = [path for path in required if not (ROOT / path).exists()]
    assert not missing, f"Missing API files: {missing}"
