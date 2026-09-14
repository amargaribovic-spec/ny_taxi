"""
Mini test — a first safe query that will NOT blow up your machine.

Why it's safe, even on the full ~16 GB:
- Spark is LAZY: read() / select() only build a plan; no data moves yet.
- show(n) materialises just n rows, and Spark reads only the COLUMNS a query
  needs (column pruning) — memory stays tiny no matter how big the data is.
- We never call .collect() / .toPandas() on the full data.

Run inside the container:
    ./run.sh submit src/mini_test.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col

spark = SparkSession.builder.appName("ny_taxi_mini_test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # quiet the INFO log flood — show results, not noise

# Read ALL yellow files (2019/2020/2025), lazily.
df = spark.read.parquet("data/raw/yellow_tripdata_*.parquet")

# 1) a safe SELECT: a few columns, 10 rows
print("\n=== select + show (10 rows) ===")
df.select("tpep_pickup_datetime", "trip_distance", "total_amount", "PULocationID") \
  .show(10, truncate=False)

# 2) a safe FILTER: long trips, 5 rows
print("\n=== filter + show (trips over 10 miles) ===")
df.filter(df.trip_distance > 10).select("trip_distance", "total_amount").show(5)

# 3) a safe AGGREGATION: trips per year — the project's headline metric.
#
#    SCHEMA DRIFT NOTE: passenger_count is `double` in 2019/2020 but `bigint` in
#    2025, so grouping on THAT column fails on a mixed-year read (Spark can't
#    reconcile the two types). Here we group on the pickup *year* instead — a
#    consistently-typed column — and column pruning means passenger_count is
#    never even read. Combining the drifted column properly (cast + unionByName)
#    is real Phase-3 data-cleaning work.
#    DATA QUALITY: a few rows have mis-recorded pickup timestamps for years we
#    never loaded (2001, 2021, 2058, ...). A date RANGE still admits strays that
#    fall inside it, so we restrict to exactly the study years.
print("\n=== trips per year (study years only) ===")
df.withColumn("year", year("tpep_pickup_datetime")) \
  .filter(col("year").isin(2019, 2020, 2025)) \
  .groupBy("year").count().orderBy("year").show()

spark.stop()
