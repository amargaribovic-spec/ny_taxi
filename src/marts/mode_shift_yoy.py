"""Gold: rideshare's share of trips per study year and the shift between them
-> data/marts/mode_shift_yoy.

The headline metric -- what fraction of trips each checkpoint year were rideshare,
and how that share moved vs the previous checkpoint. Two window functions: one
partitioned by year (the within-year share), one ordered by year (the delta via lag).

The study years are non-contiguous (2019, 2020, 2025), so the 2025 change is vs 2020
-- a jump across the recovery gap, not a single calendar year.
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

# rideshare's share and how it moved vs the previous checkpoint year
over_time = Window.orderBy("year")
mart = (
    shares.filter(col("mode") == "rideshare")
    .select("year", col("count").alias("rideshare_trips"), col("pct").alias("rideshare_pct"))
    .withColumn("change_vs_prev_pts", _round(col("rideshare_pct") - lag("rideshare_pct").over(over_time), 1))
    .orderBy("year")
)

mart.write.mode("overwrite").parquet(f"{MARTS}/mode_shift_yoy")
spark.read.parquet(f"{MARTS}/mode_shift_yoy").orderBy("year").show()

spark.stop()
