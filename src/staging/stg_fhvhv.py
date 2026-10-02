"""Silver: conform rideshare (HVFHV) to align with stg_yellow -> data/staging/fhvhv.

Keeps only the fields the marts need and RENAMES HVFHV's columns to match the
yellow silver names (trip_miles -> trip_distance, base_passenger_fare ->
fare_amount, tips -> tip_amount, PULocationID -> pu_location_id). That alignment
lets the taxi and rideshare silver tables be unionByName'd in the comparison marts.

Casts to explicit types absorb any year-to-year drift; then we filter to the
study years. The marts stamp the 'mode' label (yellow vs rideshare) themselves.
"""
from pyspark.sql.functions import col, year

from common import get_spark, RAW, STAGING, STUDY_YEARS

spark = get_spark("stg_fhvhv")

# HVFHV files are big (~10x the yellow files) and compress hard, so each input
# split explodes in memory when decoded. Cap the read-partition size so each task
# holds a smaller slice; pair it with fewer cores (SPARK_CORES) so fewer of these
# slices sit in the single local-mode heap at once. That combination — not heap
# size alone — is what keeps the write stage from OOMing.
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

fhvhv = fhvhv.filter(year("pickup_datetime").isin(*STUDY_YEARS))

fhvhv.write.mode("overwrite").parquet(f"{STAGING}/fhvhv")

# count the written output (the pruned silver), not the full raw lineage
n = spark.read.parquet(f"{STAGING}/fhvhv").count()
print(f"staging/fhvhv written: {n:,} rows")

spark.stop()
