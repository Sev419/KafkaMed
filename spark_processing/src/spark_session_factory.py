"""Centralized SparkSession startup helpers for KafkaMed."""

from __future__ import annotations

import logging
import os
from tempfile import gettempdir
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def configure_python_for_pyspark(logger: logging.Logger | None = None) -> str:
    """Force PySpark driver and workers to use the current Python executable."""
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    if logger:
        logger.info("PYSPARK_PYTHON=%s", python_executable)
        logger.info("PYSPARK_DRIVER_PYTHON=%s", python_executable)
    return python_executable


def configure_temp_dirs(project_path: Path, logger: logging.Logger | None = None) -> Path:
    """Ensure PySpark and Python use a stable temp directory."""
    temp_dir = project_path / "data" / "streaming" / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    current_temp = Path(os.environ.get("TMP", gettempdir()))
    if not current_temp.exists():
        if logger:
            logger.warning(
                "El temp actual de Windows no existe: %s. Se usara un temp local estable: %s",
                current_temp,
                temp_dir,
            )
    os.environ["TMP"] = str(temp_dir)
    os.environ["TEMP"] = str(temp_dir)
    os.environ["TMPDIR"] = str(temp_dir)
    if logger:
        logger.info("TMP=%s", temp_dir)
        logger.info("TEMP=%s", temp_dir)
        logger.info("TMPDIR=%s", temp_dir)
    return temp_dir


def warn_if_windows_path_has_spaces(project_path: Path, logger: logging.Logger) -> None:
    """Warn about a known Windows/Spark risk: paths containing spaces."""
    if os.name == "nt" and " " in str(project_path):
        logger.warning(
            "La ruta del proyecto contiene espacios: %s. En Windows esto puede "
            "afectar el arranque de Spark/JVM. Si Spark se cuelga, pruebe mover "
            "el proyecto a una ruta sin espacios o ejecutar la capa Spark en Docker.",
            project_path,
        )


def import_pyspark_components() -> dict[str, Any]:
    """Import PySpark lazily so callers can log a clean error if it is missing."""
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "No se pudo importar PySpark. Ejecute `python -m pip install -r requirements.txt` "
            "y confirme que pyspark y Java esten disponibles."
        ) from exc

    return {"SparkSession": SparkSession}


def create_spark_session(
    components: dict[str, Any],
    logger: logging.Logger,
    app_name: str = "KafkaMed",
    extra_packages: str | None = None,
):
    """Create a small local SparkSession with conservative settings."""
    configure_temp_dirs(PROJECT_ROOT, logger)
    spark_master = os.environ.get("SPARK_MASTER", "local[1]")
    shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "1")
    default_parallelism = os.environ.get("SPARK_DEFAULT_PARALLELISM", "1")
    bind_address = os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")
    driver_host = os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1")

    logger.info(
        "Creando SparkSession app=%s master=%s shuffle=%s parallelism=%s bind=%s host=%s",
        app_name,
        spark_master,
        shuffle_partitions,
        default_parallelism,
        bind_address,
        driver_host,
    )

    spark_builder = (
        components["SparkSession"]
        .builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.default.parallelism", default_parallelism)
        .config("spark.ui.enabled", os.environ.get("SPARK_UI_ENABLED", "false"))
        .config("spark.sql.adaptive.enabled", os.environ.get("SPARK_SQL_ADAPTIVE_ENABLED", "false"))
        .config("spark.driver.bindAddress", bind_address)
        .config("spark.driver.host", driver_host)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .config("spark.python.worker.faulthandler.enabled", "true")
    )
    if extra_packages:
        spark_builder = spark_builder.config("spark.jars.packages", extra_packages)

    spark = spark_builder.getOrCreate()
    logger.info("SparkSession creada correctamente.")
    return spark
