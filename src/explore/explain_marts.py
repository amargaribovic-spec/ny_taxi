"""Week-3: read the physical plans of two marts without running them.

explain() only PLANS -- it never triggers a job, so this is instant and safe. Run:
    ./run.sh submit src/explore/explain_marts.py

Read each physical plan BOTTOM-UP. What to spot:

  trips_by_zone (the broadcast join):
    FileScan parquet ... ReadSchema: [pu_location_id]   <- column pruning: 1 of 7 cols
    BroadcastHashJoin ...                                <- map-side join, NO big shuffle
    BroadcastExchange ...                                <- the tiny zone table shipped out
    (no Exchange on the 780M side = the win)

  trips_by_hour (the aggregation shuffle):
    FileScan parquet ... ReadSchema: [pickup_datetime]  <- 1 column again
    HashAggregate ...                                    <- partial counts, map side
    Exchange hashpartitioning(hour, mode, 200)          <- THE shuffle
    HashAggregate ...                                    <- final counts, reduce side

The outer `AdaptiveSparkPlan` wrapper is Adaptive Query Execution: at runtime Spark
collapses those 200 shuffle partitions down to a handful because the output is tiny
(~48 groups). That's why hand-tuning spark.sql.shuffle.partitions here isn't worth it.
"""
from pyspark.sql.functions import hour, broadcast, col

from common import get_spark, RAW, trips_with_mode

spark = get_spark("explain_marts")

trips_z = trips_with_mode(spark, "pu_location_id")
zones = spark.read.option("header", True).csv(f"{RAW}/taxi_zone_lookup.csv").select(
    col("LocationID").cast("int").alias("location_id"), "Borough"
)
zone_mart = trips_z.join(broadcast(zones), trips_z.pu_location_id == zones.location_id).groupBy(
    "Borough", "mode"
).count()
print("\n=== trips_by_zone -- physical plan (broadcast join) ===")
zone_mart.explain()

hour_mart = trips_with_mode(spark, "pickup_datetime").groupBy(
    hour("pickup_datetime").alias("hour"), "mode"
).count()
print("\n=== trips_by_hour -- physical plan (aggregation shuffle) ===")
hour_mart.explain()

spark.stop()
