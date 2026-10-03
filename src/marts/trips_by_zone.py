"""Gold: pickup volume by borough and mode -> data/marts/trips_by_zone.

Joins trips to the TLC zone lookup -- a tiny dimension, broadcast so the ~780M-row
trip side never shuffles -- to see which boroughs each mode serves.
"""
from pyspark.sql.functions import broadcast, col

from common import get_spark, RAW, MARTS, trips_with_mode

spark = get_spark("mart_trips_by_zone")

trips = trips_with_mode(spark, "pu_location_id")

zones = spark.read.option("header", True).csv(f"{RAW}/taxi_zone_lookup.csv").select(
    col("LocationID").cast("int").alias("location_id"),
    "Borough",
)

mart = (
    trips.join(broadcast(zones), trips.pu_location_id == zones.location_id)
    .groupBy("Borough", "mode")
    .count()
    .orderBy("Borough", "mode")
)

mart.write.mode("overwrite").parquet(f"{MARTS}/trips_by_zone")
spark.read.parquet(f"{MARTS}/trips_by_zone").orderBy("Borough", "mode").show(100)

spark.stop()
