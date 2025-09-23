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

csv_path = f'{landing_zone}/AAPL_minute_20250813_20250814'
bronze_table = "dev.bronze.aapl_minutes_copy_into"

# COMMAND ----------

# %sql

# drop table dev.bronze.aapl_minutes_copy_into;
# CREATE TABLE IF NOT EXISTS dev.bronze.aapl_minutes_copy_into
# USING DELTA;

# COMMAND ----------

query = f"""
COPY INTO {bronze_table}
FROM '{csv_path}'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true'
)
COPY_OPTIONS (
    'mergeSchema' = 'true'
)
"""
spark.sql(query)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {bronze_table}"))
