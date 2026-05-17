"""KafkaMed streaming job: Kafka -> Spark ML inference -> MongoDB."""

from __future__ import annotations

import logging
from pathlib import Path
import sys
import time

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parents[1]
LOG_DIR = PROJECT_ROOT / "data" / "streaming" / "processed"
LOG_PATH = LOG_DIR / "process_heart_records_with_ml_to_mongo.log"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql.functions import col, from_json  # noqa: E402

from spark_processing.src.heart_schema import HEART_RECORD_SCHEMA  # noqa: E402
from spark_processing.src.kafka_config import load_kafka_stream_config  # noqa: E402
from spark_processing.src.prediction_writer import write_prediction_microbatch_to_mongo  # noqa: E402
from spark_processing.src.spark_session_factory import (  # noqa: E402
    configure_python_for_pyspark,
    create_spark_session,
    import_pyspark_components,
    warn_if_windows_path_has_spaces,
)


LOGGER = logging.getLogger("kafkamed.spark_heart_ml_mongo")


def setup_logger() -> None:
    """Configure console logging for the ML persistence job."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LOGGER.handlers.clear()
    LOGGER.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)

    LOGGER.info("Log persistente: %s", LOG_PATH)


def build_stream():
    """Create the structured streaming DataFrame from Kafka."""
    components = import_pyspark_components()
    kafka_config = load_kafka_stream_config()
    LOGGER.info(
        "Configuracion Kafka: bootstrap=%s topic=%s offsets=%s checkpoint=%s",
        kafka_config.bootstrap_servers,
        kafka_config.topic,
        kafka_config.starting_offsets,
        kafka_config.checkpoint_location,
    )

    spark = create_spark_session(
        components,
        LOGGER,
        app_name="KafkaMed_HeartRecords_With_ML_To_Mongo",
        extra_packages=kafka_config.spark_packages,
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
        .option("subscribe", kafka_config.topic)
        .option("startingOffsets", kafka_config.starting_offsets)
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

    return spark, parsed_stream, kafka_config


def run() -> int:
    """Run the Spark job and persist predictions to MongoDB."""
    setup_logger()
    start = time.time()
    spark = None
    query = None

    try:
        LOGGER.info("Inicio del consumidor Spark ML -> MongoDB.")
        LOGGER.info("Proyecto raiz: %s", PROJECT_ROOT)
        warn_if_windows_path_has_spaces(PROJECT_ROOT, LOGGER)
        configure_python_for_pyspark(LOGGER)

        spark, parsed_stream, kafka_config = build_stream()
        checkpoint_location = kafka_config.checkpoint_location
        LOGGER.info("Persistencia con checkpoint: %s", checkpoint_location)

        writer = (
            parsed_stream.writeStream.foreachBatch(
                lambda batch_df, batch_id: write_prediction_microbatch_to_mongo(
                    batch_df,
                    batch_id,
                    logger=LOGGER,
                )
            )
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .queryName("kafkamed_heart_predictions_mongo")
        )

        if kafka_config.trigger_mode.lower() == "availablenow":
            writer = writer.trigger(availableNow=True)
            LOGGER.info("Modo de disparo: availableNow (terminacion controlada).")
        else:
            writer = writer.trigger(processingTime="5 seconds")
            LOGGER.info("Modo de disparo: processingTime=5 seconds.")

        query = writer.start()
        LOGGER.info("Query iniciada. Esperando finalizacion controlada (%s s).", kafka_config.timeout_seconds)
        query.awaitTermination(kafka_config.timeout_seconds)

        if query.isActive:
            LOGGER.warning("La query sigue activa tras el timeout. Se detendra para cerrar la prueba.")
            query.stop()

        LOGGER.info("Consumidor Spark ML -> Mongo finalizado en %.2f segundos.", time.time() - start)
        return 0
    except Exception:
        LOGGER.exception("El consumidor Spark ML -> Mongo fallo.")
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
