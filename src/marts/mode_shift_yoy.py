"""Gold: each mode's share of trips per study year and how it shifted
-> data/marts/mode_shift_yoy.

What fraction of trips each checkpoint year were yellow vs rideshare, and how each
share moved vs the previous checkpoint. Window functions: one over the year (share),
one over mode ordered by year (the lag delta). Years are non-contiguous, so 2025's
change is vs 2020.
"""
from pyspark.sql.functions import year, col, lag, sum as _sum, round as _round
from pyspark.sql.window import Window

from common import get_spark, MARTS, trips_with_mode

spark = get_spark("mart_mode_shift_yoy")

trips = trips_with_mode(spark, "pickup_datetime")

yearly = trips.groupBy(year("pickup_datetime").alias("year"), "mode").count()

# each mode's share of its year's trips
by_year = Window.partitionBy("year")
shares = yearly.withColumn("pct", _round(100 * col("count") / _sum("count").over(by_year), 1))

# how each mode's share moved vs the previous checkpoint year
over_time = Window.partitionBy("mode").orderBy("year")
mart = (
    shares.withColumn("change_vs_prev_pts", _round(col("pct") - lag("pct").over(over_time), 1))
    .select("year", "mode", col("count").alias("trips"), "pct", "change_vs_prev_pts")
    .orderBy("year", "mode")
)

mart.write.mode("overwrite").parquet(f"{MARTS}/mode_shift_yoy")
spark.read.parquet(f"{MARTS}/mode_shift_yoy").orderBy("year", "mode").show()

spark.stop()
