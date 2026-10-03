"""Gold: monthly trip volume by mode (yellow vs rideshare) -> data/marts/trips_by_period.

Trips per (year, month, mode) from both silver tables.
"""
from pyspark.sql.functions import year, month

from common import get_spark, MARTS, trips_with_mode

spark = get_spark("mart_trips_by_period")

trips = trips_with_mode(spark, "pickup_datetime")

mart = (
    trips.groupBy(
        year("pickup_datetime").alias("year"),
        month("pickup_datetime").alias("month"),
        "mode",
    )
    .count()
    .orderBy("year", "month", "mode")
)

mart.write.mode("overwrite").parquet(f"{MARTS}/trips_by_period")

# read the written mart back instead of re-running the full aggregation to show it
spark.read.parquet(f"{MARTS}/trips_by_period").orderBy("year", "month", "mode").show(100)

spark.stop()
