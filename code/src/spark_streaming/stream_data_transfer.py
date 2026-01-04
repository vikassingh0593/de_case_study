# Databricks notebook source
import __init__
from src.config.config_store import *

# COMMAND ----------

helper = UCSetup(spark, dbutils)

# COMMAND ----------

landing_zone = helper.get_paths()["landing_zone_path"]
checkpoint = helper.get_paths()['checkpoint_path']

bronze_table = f"dev.bronze.spark_streaming_save"

# COMMAND ----------

from pyspark.sql.functions import window, current_timestamp

# 1) Create streaming source (rate generator)
rate_df = (
    spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()
)

# 2) Apply watermark and windowed aggregation
agg_df = (
    rate_df
    .withWatermark("timestamp", "1 minute")  # Accept data up to 1 minute late
    .groupBy(window("timestamp", "10 seconds"))
    .count()
    .withColumn("batch_time", current_timestamp())
)

# 3) Write data to Delta table with checkpointing
(
    agg_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint}/spark_streaming_save")
        .option("mergeSchema", "true")
        # .trigger(availableNow=True)
        .toTable(bronze_table)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from dev.bronze.spark_streaming_save

# COMMAND ----------


