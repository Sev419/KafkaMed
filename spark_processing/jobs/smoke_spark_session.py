"""Minimal SparkSession smoke test for KafkaMed."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import subprocess
import sys
import time

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_processing.src.spark_session_factory import (
    configure_python_for_pyspark,
    create_spark_session,
    import_pyspark_components,
    warn_if_windows_path_has_spaces,
)


LOG_DIR = PROJECT_ROOT / "data" / "streaming" / "processed"
LOG_PATH = LOG_DIR / "smoke_spark_session.log"
LOGGER = logging.getLogger("kafkamed.spark_smoke")


def configure_logger() -> None:
    """Configure console and persistent logging for the smoke test."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LOGGER.handlers.clear()
    LOGGER.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def run_child_smoke() -> int:
    """Create SparkSession, execute spark.range(1).count(), and close Spark."""
    configure_logger()
    spark = None
    start = time.time()
    try:
        LOGGER.info("Inicio smoke test SparkSession.")
        LOGGER.info("Proyecto: %s", PROJECT_ROOT)
        LOGGER.info("Python: %s", sys.executable)
        LOGGER.info("Log persistente: %s", LOG_PATH)
        warn_if_windows_path_has_spaces(PROJECT_ROOT, LOGGER)
        configure_python_for_pyspark(LOGGER)

        components = import_pyspark_components()
        spark = create_spark_session(
            components,
            LOGGER,
            app_name="KafkaMed_SparkSession_Smoke",
        )
        spark.sparkContext.setLogLevel("WARN")

        LOGGER.info("Ejecutando spark.range(1).count().")
        count = spark.range(1).count()
        LOGGER.info("Resultado spark.range(1).count(): %s", count)
        if count != 1:
            raise RuntimeError(f"Resultado inesperado del smoke test: {count}")

        LOGGER.info("Smoke test SparkSession OK en %.2f segundos.", time.time() - start)
        return 0
    except Exception:
        LOGGER.exception("Smoke test SparkSession fallo.")
        return 1
    finally:
        if spark is not None:
            LOGGER.info("Cerrando SparkSession.")
            spark.stop()
            LOGGER.info("SparkSession cerrada.")


def run_parent_with_timeout(timeout_seconds: int) -> int:
    """Run the child smoke test with a timeout and report a clean diagnosis."""
    configure_logger()
    LOGGER.info("Ejecutando smoke test en proceso hijo con timeout=%s segundos.", timeout_seconds)
    command = [sys.executable, str(Path(__file__).resolve()), "--child"]

    try:
        completed = subprocess.run(command, cwd=PROJECT_ROOT, timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        LOGGER.error(
            "SparkSession no arranco en %s segundos. Esto confirma un bloqueo "
            "del runtime Spark/Java en el entorno local. Recomendacion: probar "
            "Docker o mover el proyecto a una ruta Windows sin espacios.",
            timeout_seconds,
        )
        return 2

    if completed.returncode == 0:
        LOGGER.info("Smoke test completado correctamente.")
    else:
        LOGGER.error("Smoke test fallo con codigo de salida %s.", completed.returncode)
    return completed.returncode


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Smoke test minimo de SparkSession.")
    parser.add_argument("--child", action="store_true", help="Ejecuta la prueba real en este proceso.")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout del proceso hijo en segundos.")
    return parser.parse_args()


def main() -> int:
    """Run parent watchdog or child smoke mode."""
    args = parse_args()
    if args.child:
        return run_child_smoke()
    return run_parent_with_timeout(args.timeout)


if __name__ == "__main__":
    sys.exit(main())
