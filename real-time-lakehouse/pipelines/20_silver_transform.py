# Silver: parse, validate, dedupe
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config_rt import *

silver_chkpt = f"{checkpoints}/silver_transform"

bronze = spark.readStream.table(bronze_tbl)

parsed = (bronze
  .withColumn("json", F.from_json(F.col("raw_json"), event_schema))
  .select("kafka_ts","topic","partition","offset","json.*"))

clean = (parsed
  .withColumn("ts", F.coalesce(F.col("ts"), F.col("kafka_ts")))
  .withColumn("price", F.when(F.col("price").isNull(), F.lit(0.0)).otherwise(F.col("price")))
  .withColumn("quantity", F.when(F.col("quantity").isNull(), F.lit(0)).otherwise(F.col("quantity")))
  .withColumn("event_type", F.lower(F.col("event_type")))
  .filter(F.col("event_id").isNotNull())
  .filter(F.col("event_type").isin("page_view","add_to_cart","purchase"))
)

# Watermark + dedupe
deduped = (clean
  .withWatermark("ts", "10 minutes")
  .dropDuplicates(["event_id"]))

(deduped.writeStream
  .format("delta")
  .option("checkpointLocation", silver_chkpt)
  .outputMode("append")
  .table(silver_tbl))
