"""Gold: trips per year, from the conformed yellow staging table.

Reads silver (data/staging/yellow), not raw — so the schema is already clean.
"""
from pyspark.sql.functions import year

from common import get_spark, STAGING, MARTS

spark = get_spark("mart_trips_by_year")

yellow = spark.read.parquet(f"{STAGING}/yellow")

mart = (
    yellow.groupBy(year("pickup_datetime").alias("year"))
    .count()
    .orderBy("year")
)

mart.write.mode("overwrite").parquet(f"{MARTS}/trips_by_year")
mart.show()

spark.stop()
