# Databricks notebook source
import __init__
from src.config.config_store import *

# COMMAND ----------

helper = UCSetup(spark, dbutils)

# COMMAND ----------

pip install -U polygon-api-client

# COMMAND ----------

landing_zone = helper.get_paths()["landing_zone_path"]
checkpoint = helper.get_paths()['checkpoint_path']

csv_path = f"{landing_zone}/AAPL_minute_*.csv"
table_name = "aapl_minutes_autoloader"
bronze_table = f"dev.bronze.{table_name}"

# COMMAND ----------

import json
from polygon import RESTClient
client = RESTClient(api_key=APIKEY)

resp = client.get_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="month",
    from_="2025-09-19",
    to="2025-09-19",
    limit=50000,
    raw=True
)
data = json.loads(resp.data)
data

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.streaming import DataStreamWriter

# Ingest files continuously
df_stream = (spark.readStream
    .format("cloudFiles")  # Use 'cloudFiles' for Auto Loader
    .option("cloudFiles.format", "json")
    .load("/mnt/landing_zone"))  # Or use 'readStream.json' directly for few files

# Example: Flatten nested field and extract timestamp
df_stream = df_stream.selectExpr("explode(results) as result", "ticker")
df_stream = df_stream.withColumn("timestamp", expr("CAST(result.t AS TIMESTAMP)"))

# JOIN with another static or streaming table (example: reference data or lookup table)
reference_table = spark.read.table("my_reference_table")
joined = df_stream.join(reference_table, on="ticker", how="left")

# AGGREGATION with WATERMARK for late/delayed data handling
from pyspark.sql.functions import window
agg_df = (joined
    .withWatermark("timestamp", "10 minutes")  # allow lateness up to 10 minutes
    .groupBy(
        window("timestamp", "5 minutes"),  # aggregate in 5-minute windows
        "ticker"
    )
    .agg({"result.v": "sum", "result.n": "max"}))

# DISPLAY as a streaming table
query = (agg_df
    .writeStream
    .outputMode("update")
    .format("console")
    .start())

