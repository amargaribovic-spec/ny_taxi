"""Silver: conform rideshare (HVFHV) to the yellow silver schema -> data/staging/fhvhv.

Renames HVFHV's columns to match stg_yellow (trip_miles -> trip_distance,
base_passenger_fare -> fare_amount, ...) so the two modes can be unionByName'd in
the marts. Casts absorb year-to-year drift; then filter to the study years.
"""
from pyspark.sql.functions import col

from common import get_spark, RAW, STAGING, in_study_window

spark = get_spark("stg_fhvhv")

# large files that compress hard: cap read-partition size so each task decodes a
# smaller slice (pair with SPARK_CORES to limit concurrent tasks)
spark.conf.set("spark.sql.files.maxPartitionBytes", "32m")

fhvhv = spark.read.parquet(f"{RAW}/fhvhv_tripdata_*.parquet").select(
    col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
    col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
    col("PULocationID").cast("int").alias("pu_location_id"),
    col("DOLocationID").cast("int").alias("do_location_id"),
    col("trip_miles").cast("double").alias("trip_distance"),
    col("base_passenger_fare").cast("double").alias("fare_amount"),
    col("tips").cast("double").alias("tip_amount"),
)

fhvhv = in_study_window(fhvhv)

fhvhv.write.mode("overwrite").parquet(f"{STAGING}/fhvhv")

n = spark.read.parquet(f"{STAGING}/fhvhv").count()
print(f"staging/fhvhv written: {n:,} rows")

spark.stop()
