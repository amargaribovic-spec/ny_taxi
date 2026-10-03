"""
Day 4 — read the execution plan and see partitions.

Run inside the container:
    ./run.sh submit src/explore/explain_demo.py

explain() does NOT run the query — it just prints the plan Spark *would* run, so
this is instant and safe. Read the physical plan BOTTOM-UP:

  FileScan parquet ....... reads the files. Look at `ReadSchema` (column pruning —
                           only the columns the query needs) and `PushedFilters`.
  HashAggregate .......... partial aggregation done per partition (the "map" side).
  Exchange hashpartition . a SHUFFLE — data is moved across the network between
                           partitions so equal keys land together. This is the
                           expensive step; optimization is mostly about avoiding /
                           shrinking shuffles.
  HashAggregate .......... final aggregation after the shuffle (the "reduce" side).

Partitions = how the work is split into parallel tasks. On a plain read that's
roughly one partition per file, which is why your job ran as ~36 tasks.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import year

spark = SparkSession.builder.appName("ny_taxi_explain").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("data/raw/yellow_tripdata_*.parquet")

# how many partitions did the read create? (= parallel tasks)
print(f"\n=== partitions on read: {df.rdd.getNumPartitions()} ===")

trips_per_year = df.groupBy(year("tpep_pickup_datetime").alias("year")).count()

# the physical plan (what actually runs) — note the `Exchange` = the shuffle
print("\n=== explain() — physical plan ===")
trips_per_year.explain()

# the full story: parsed -> analyzed -> optimized -> physical
print("\n=== explain(True) — all four plan stages ===")
trips_per_year.explain(True)

spark.stop()
