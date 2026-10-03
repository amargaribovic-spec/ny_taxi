"""Gold: trip volume by hour of day and mode -> data/marts/trips_by_hour.

When each mode peaks across the day (commute vs nightlife), pooled over the study window.
"""
from pyspark.sql.functions import hour

from common import get_spark, MARTS, trips_with_mode

spark = get_spark("mart_trips_by_hour")

trips = trips_with_mode(spark, "pickup_datetime")

mart = (
    trips.groupBy(hour("pickup_datetime").alias("hour"), "mode")
    .count()
    .orderBy("hour", "mode")
)

mart.write.mode("overwrite").parquet(f"{MARTS}/trips_by_hour")
spark.read.parquet(f"{MARTS}/trips_by_hour").orderBy("hour", "mode").show(100)

spark.stop()
