"""Shared Spark helpers and the medallion layer paths.

Imported by every job (staging + marts). run.sh puts src/ on PYTHONPATH so
`from common import ...` resolves from any subfolder.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year

RAW = "/home/jovyan/work/data/raw"          # bronze — downloaded parquet, untouched
STAGING = "/home/jovyan/work/data/staging"  # silver — cleaned & conformed
MARTS = "/home/jovyan/work/data/marts"      # gold   — business-ready aggregations

STUDY_YEARS = [2019, 2020, 2025]
STUDY_START = "2019-02-01"  # HVFHV data begins Feb 2019; align yellow so both modes match


def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def in_study_window(df, ts="pickup_datetime"):
    """Study years, aligned to the rideshare start so yellow and rideshare span the
    same months (drops yellow's Jan 2019)."""
    return df.filter(year(col(ts)).isin(*STUDY_YEARS) & (col(ts) >= STUDY_START))
