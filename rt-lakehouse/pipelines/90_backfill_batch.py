# Batch backfill: append historical data into bronze
from pyspark.sql import functions as F
from config_rt import *

hist = (spark.read.format("json")
        .schema(event_schema)
        .load("dbfs:/FileStore/history/ecomm/*.json"))

to_bronze = (hist
             .select(F.to_json(F.struct([F.col(c) for c in hist.columns])).alias("raw_json"))
             .withColumn("kafka_ts", F.col("ts"))
             .withColumn("topic", F.lit("history"))
             .withColumn("partition", F.lit(-1))
             .withColumn("offset", F.monotonically_increasing_id()))

(to_bronze.write
 .format("delta")
 .mode("append")
 .saveAsTable(bronze_tbl))
