"""Gold: monthly trip volume by mode (yellow vs rideshare) -> data/marts/trips_by_period.

Stamps a `mode` label on each silver table and unions them -- the shape the other
cross-mode marts reuse.
"""
from pyspark.sql.functions import year, month, lit

from common import get_spark, STAGING, MARTS

spark = get_spark("mart_trips_by_period")


def trips(table, mode):
    return (
        spark.read.parquet(f"{STAGING}/{table}")
        .select("pickup_datetime")
        .withColumn("mode", lit(mode))
    )


trips_all = trips("yellow", "yellow").unionByName(trips("fhvhv", "rideshare"))

mart = (
    trips_all.groupBy(
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
