# Databricks notebook source
import __init__
from src.config.config_store import *

# COMMAND ----------
# Install the Polygon API client
pip install -U polygon-api-client

# COMMAND ----------
helper = UCSetup(spark, dbutils)

# COMMAND ----------
def fetch_minute_bars_to_spark(spark, client, ticker, multiplier, start_date, end_date, lmt):
    # 1) Fetch aggregates (minute bars)
    aggs = []
    for a in client.list_aggs(
        ticker,
        multiplier,
        timespan,
        from_=start_date,
        to=end_date,
        limit=lmt
    ):
        aggs.append(a)

    # 2) Convert SDK objects to dicts with canonical Polygon keys
    aggs_dicts = []
    for a in aggs:
        d = a.__dict__ if hasattr(a, "__dict__") else (a._asdict() if hasattr(a, "_asdict") else dict(a))
        aggs_dicts.append({
            "open": d.get("open", d.get("o")),
            "high": d.get("high", d.get("h")),
            "low":  d.get("low",  d.get("l")),
            "close": d.get("close", d.get("c")),
            "volume": d.get("volume", d.get("v")),
            "vwap": d.get("vwap", d.get("vw")),
            "timestamp": d.get("timestamp", d.get("t")),
            "transactions": d.get("transactions", d.get("n")),
            "otc": str(d.get("otc")).lower() if d.get("otc") is not None else None,
        })

    # 3) pandas DataFrame
    df_pd = pd.DataFrame(aggs_dicts)

    # 4) Spark schema (matches your request)
    schema = StructType([
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("vwap", DoubleType(), True),
        StructField("timestamp", LongType(), True),
        StructField("transactions", LongType(), True),
        StructField("otc", StringType(), True),
    ])

    # 5) Set Spark session timezone to IST and create Spark DataFrame
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    spark_df = (
        spark.createDataFrame(df_pd, schema=schema)
             .withColumn("TimestampIst", (col("timestamp") / 1000).cast("double"))
             .withColumn("TimestampIst", to_timestamp(from_unixtime(col("TimestampIst"))))
    )

    # Compute formatted date strings
    start_int = int(datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d"))
    end_int   = int(datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d"))

    # Build output path
    out_path = f"{helper.get_paths()['landing_zone_path']}/{ticker}_{timespan}_{start_int}_{end_int}"
    print(f"Saving CSV to: {out_path}")

    # Write dataframe to CSV
    spark_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(out_path)

# COMMAND ----------
from polygon import RESTClient
client = RESTClient(api_key=APIKEY)

ticker = "AAPL"
timespan = "minute"
multiplier = 1
start_date = "2025-08-15"
end_date = "2025-08-18"
lmt = 50000
fetch_minute_bars_to_spark(spark, client, ticker, multiplier, start_date, end_date, lmt)

# COMMAND ----------
df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load(f"{helper.get_paths()['landing_zone_path']}/AAPL_minute_*")
)
# Saving CSV to: dbfs:/Volumes/dev/infra/pipeline_artifacts/landing_zone/AAPL_minute_20250819_20250820
df.display()

# COMMAND ----------
import json
from datetime import datetime

# --- Get/save JSON response ---
resp = client.get_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="month",
    from_="2025-09-16",
    to="2025-09-17",
    limit=50000,
    raw=True
)
data = json.loads(resp.data)

# --- Build output path (matching CSV logic) ---
start_int = int(datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d"))
end_int   = int(datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d"))

json_out_path = f"{helper.get_paths()['landing_zone_path']}/{ticker}_{timespan}_{start_int}_{end_int}.json"

print(f"Saving JSON to: {json_out_path}")

# json.dumps returns string; dbutils.fs.put writes to the volume
dbutils.fs.put(
    json_out_path,
    json.dumps(data, ensure_ascii=False, indent=2),
    overwrite=True
)

# COMMAND ----------
import json

json_path = f"{helper.get_paths()['landing_zone_path']}/AAPL_minute_20250819_20250820.json"
json_str = dbutils.fs.head(json_path, 10000000)  # read up to 10MB (adjust if needed)
data = json.loads(json_str)
print(data)