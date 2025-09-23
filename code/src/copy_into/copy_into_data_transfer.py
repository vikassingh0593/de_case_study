# Databricks notebook source
import __init__
from src.config.config_store import *

# COMMAND ----------
helper = UCSetup(spark, dbutils)

# COMMAND ----------
landing_zone = helper.get_paths()["landing_zone_path"]
checkpoint = helper.get_paths()['checkpoint_path']
landing_zone, checkpoint

# COMMAND ----------
csv_path = f"{landing_zone}/AAPL_minute_*.csv"
bronze_table = "dev.bronze.aapl_minutes_copy_into"

# COMMAND ----------
query = f"""
COPY INTO {bronze_table}
FROM '{csv_path}'
FILEFORMAT = 'CSV'
"""
spark.sql(query)

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {bronze_table}"))