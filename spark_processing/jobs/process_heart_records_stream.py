"""KafkaMed Spark Structured Streaming consumer for heart-record messages."""

from __future__ import annotations

import logging
from pathlib import Path
import sys
import time

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql.functions import col, from_json

from spark_processing.src.heart_schema import HEART_RECORD_SCHEMA
from spark_processing.src.kafka_config import load_kafka_stream_config
from spark_processing.src.spark_session_factory import (
    configure_python_for_pyspark,
    create_spark_session,
    import_pyspark_components,
    warn_if_windows_path_has_spaces,
)


LOGGER = logging.getLogger("kafkamed.spark_heart_stream")


def setup_logger() -> None:
    """Configure console logging for the structured streaming job."""
    LOGGER.handlers.clear()
    LOGGER.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)


def build_stream():
    """Create the structured streaming DataFrame from Kafka."""
    components = import_pyspark_components()
    config = load_kafka_stream_config()
    LOGGER.info("Configuracion Kafka: bootstrap=%s topic=%s offsets=%s checkpoint=%s",
                config.bootstrap_servers,
                config.topic,
                config.starting_offsets,
                config.checkpoint_location)

    spark = create_spark_session(
        components,
        LOGGER,
        app_name="KafkaMed_HeartRecords_Stream",
        extra_packages=config.spark_packages,
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream.selectExpr(
            "CAST(value AS STRING) AS json_value",
            "timestamp AS kafka_timestamp",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
        )
        .withColumn("record", from_json(col("json_value"), HEART_RECORD_SCHEMA))
        .select(
            col("record.Age").alias("Age"),
            col("record.Sex").alias("Sex"),
            col("record.ChestPainType").alias("ChestPainType"),
            col("record.RestingBP").alias("RestingBP"),
            col("record.Cholesterol").alias("Cholesterol"),
            col("record.FastingBS").alias("FastingBS"),
            col("record.RestingECG").alias("RestingECG"),
            col("record.MaxHR").alias("MaxHR"),
            col("record.ExerciseAngina").alias("ExerciseAngina"),
            col("record.Oldpeak").alias("Oldpeak"),
            col("record.ST_Slope").alias("ST_Slope"),
            col("record.HeartDisease").alias("HeartDisease"),
            col("record.source_file").alias("source_file"),
            col("record.row_number").alias("row_number"),
            col("kafka_timestamp"),
            col("kafka_partition"),
            col("kafka_offset"),
        )
    )

    return spark, parsed_stream, config


def run() -> int:
    """Run the KafkaMed streaming consumer and print parsed rows to console."""
    setup_logger()
    start = time.time()
    spark = None
    query = None

    try:
        LOGGER.info("Inicio del consumidor Spark Structured Streaming de KafkaMed.")
        LOGGER.info("Proyecto raiz: %s", PROJECT_ROOT)
        warn_if_windows_path_has_spaces(PROJECT_ROOT, LOGGER)
        configure_python_for_pyspark(LOGGER)

        spark, parsed_stream, config = build_stream()
        LOGGER.info("Iniciando escritura a consola con checkpoint: %s", config.checkpoint_location)

        writer = (
            parsed_stream.writeStream.format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 50)
            .option("checkpointLocation", config.checkpoint_location)
            .queryName("kafkamed_heart_records_console")
        )

        if config.trigger_mode.lower() == "availablenow":
            writer = writer.trigger(availableNow=True)
            LOGGER.info("Modo de disparo: availableNow (terminacion controlada).")
        else:
            writer = writer.trigger(processingTime="5 seconds")
            LOGGER.info("Modo de disparo: processingTime=5 seconds.")

        query = writer.start()
        LOGGER.info("Query iniciada. Esperando finalizacion controlada (%s s).", config.timeout_seconds)
        query.awaitTermination(config.timeout_seconds)

        if query.isActive:
            LOGGER.warning("La query sigue activa tras el timeout. Se detendra para cerrar la prueba.")
            query.stop()

        LOGGER.info("Consumidor KafkaMed finalizado en %.2f segundos.", time.time() - start)
        return 0
    except Exception:
        LOGGER.exception("El consumidor Spark Structured Streaming fallo.")
        return 1
    finally:
        if query is not None and query.isActive:
            LOGGER.info("Deteniendo query activa.")
            query.stop()
        if spark is not None:
            LOGGER.info("Cerrando SparkSession.")
            spark.stop()
            LOGGER.info("SparkSession cerrada.")


if __name__ == "__main__":
    raise SystemExit(run())
