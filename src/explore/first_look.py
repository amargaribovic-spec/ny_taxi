"""
Day-1 first look: read the NYC taxi parquet and profile it.

Run inside the container:
    ./run.sh submit src/explore/first_look.py
    ./run.sh submit src/explore/first_look.py "data/raw/fhvhv_tripdata_*.parquet"

Reads a directory/glob of parquet as one Spark DataFrame, then prints the
schema, the row count, and a sample. Defaults to the yellow-taxi files so the
schema is consistent (don't mix yellow + fhvhv in one read — different columns).
"""
import sys

from pyspark.sql import SparkSession

path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/yellow_tripdata_*.parquet"

spark = SparkSession.builder.appName("ny_taxi_first_look").getOrCreate()

df = spark.read.parquet(path)

print(f"\n=== reading: {path} ===")
print("\n=== schema ===")
df.printSchema()

print("\n=== row count ===")
print(f"{df.count():,} rows")

print("\n=== sample rows ===")
df.show(5, truncate=False)

spark.stop()
