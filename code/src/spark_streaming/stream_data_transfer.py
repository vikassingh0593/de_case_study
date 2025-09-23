# Databricks notebook source
import pandas as pd
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from pyspark.sql.functions import col, from_unixtime, to_timestamp

# COMMAND ----------
apiKey = "XQf22C5alIB5vXvUqKsjb4f7BZIQIKaH"

# COMMAND ----------
pip install -U polygon-api-client

# COMMAND ----------
from polygon import RESTClient
import pandas as pd
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from pyspark.sql.functions import col, to_timestamp, from_unixtime
from pyspark.sql import SparkSession

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

    return spark_df

client = RESTClient(api_key=apiKey)
ticker = "AAPL"
timespan = "minute"
multiplier = 1
start_date = "2025-09-19"
end_date = "2025-09-20"
lmt = 50000
spark_df = fetch_minute_bars_to_spark(spark, client, ticker, multiplier, start_date, end_date, lmt)
spark_df.display()

# COMMAND ----------
import json

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