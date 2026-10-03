"""Silver: conform yellow taxi across years -> data/staging/yellow.

The TLC schema drifts (passenger_count is double in 2019/2020, bigint in 2025), so we
read each consistent-schema group on its own, cast to explicit types, union, and keep
only the study window.
"""
from pyspark.sql.functions import col

from common import get_spark, RAW, STAGING, in_study_window

spark = get_spark("stg_yellow")


def conform(df):
    return df.select(
        col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        col("passenger_count").cast("double").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("PULocationID").cast("int").alias("pu_location_id"),
        col("DOLocationID").cast("int").alias("do_location_id"),
        col("payment_type").cast("int").alias("payment_type"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("total_amount").cast("double").alias("total_amount"),
    )


# read each consistent-schema group separately, then union
older = spark.read.parquet(
    f"{RAW}/yellow_tripdata_2019-*.parquet",
    f"{RAW}/yellow_tripdata_2020-*.parquet",
)
newer = spark.read.parquet(f"{RAW}/yellow_tripdata_2025-*.parquet")

yellow = in_study_window(conform(older).unionByName(conform(newer)))

yellow.write.mode("overwrite").parquet(f"{STAGING}/yellow")
print(f"staging/yellow written: {yellow.count():,} rows")

spark.stop()
