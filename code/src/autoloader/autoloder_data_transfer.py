# Databricks notebook source
import __init__
from src.config.config_store import *

# COMMAND ----------
# Install the Polygon API client
pip install -U polygon-api-client

# COMMAND ----------
from polygon import RESTClient

# COMMAND ----------
helper = UCSetup(spark, dbutils)

# COMMAND ----------
landing_zone = helper.get_paths()["landing_zone_path"]
checkpoint = helper.get_paths()['checkpoint_path']
landing_zone, checkpoint

# COMMAND ----------
csv_path = f"{landing_zone}/AAPL_minute_*.csv"
table_name = "aapl_minutes_autoloader"
bronze_table = f"dev.bronze.{table_name}"

# COMMAND ----------
import pyspark.sql.functions as sf

schema = """
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume LONG,
    vwap DOUBLE,
    timestamp LONG,
    transactions LONG,
    otc STRING,
    TimestampIst STRING
"""

df_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .schema(schema)
        .load(csv_path)
        .withColumn("load_time", sf.current_timestamp())
        .withColumn("source_file", sf.col("_metadata.file_path"))
)

bronze_writer = (
    df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint}/{table_name}")
        .queryName(f"{table_name}_to_bronze")
        .trigger(availableNow=True)
        .toTable(bronze_table)
)

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {bronze_table}"))