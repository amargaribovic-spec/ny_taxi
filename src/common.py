"""Shared Spark helpers and the medallion layer paths.

Imported by every job (staging + marts). run.sh puts src/ on PYTHONPATH so
`from common import ...` resolves from any subfolder.
"""
from pyspark.sql import SparkSession

# Absolute container paths (mounted from the host) so jobs resolve them anywhere.
RAW = "/home/jovyan/work/data/raw"          # bronze — downloaded parquet, untouched
STAGING = "/home/jovyan/work/data/staging"  # silver — cleaned & conformed
MARTS = "/home/jovyan/work/data/marts"      # gold   — business-ready aggregations

STUDY_YEARS = [2019, 2020, 2025]


def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
