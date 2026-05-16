"""Explicit Spark schema for KafkaMed heart-record messages."""

from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DoubleType,
)


HEART_RECORD_SCHEMA = StructType(
    [
        StructField("Age", IntegerType(), True),
        StructField("Sex", StringType(), True),
        StructField("ChestPainType", StringType(), True),
        StructField("RestingBP", IntegerType(), True),
        StructField("Cholesterol", IntegerType(), True),
        StructField("FastingBS", IntegerType(), True),
        StructField("RestingECG", StringType(), True),
        StructField("MaxHR", IntegerType(), True),
        StructField("ExerciseAngina", StringType(), True),
        StructField("Oldpeak", DoubleType(), True),
        StructField("ST_Slope", StringType(), True),
        StructField("HeartDisease", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("row_number", IntegerType(), True),
    ]
)

