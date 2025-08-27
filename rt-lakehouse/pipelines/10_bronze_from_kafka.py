# Bronze: Kafka -> Delta (raw)
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config_rt import *

bronze_chkpt = f"{checkpoints}/bronze_from_kafka"

raw_stream = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_bootstrap)
              .option("subscribe", kafka_topic)
              .option("startingOffsets", "latest")
              .option("failOnDataLoss", "false")
              .option("includeHeaders", "true")
              .load())

bronze = (raw_stream
          .select(
              F.col("value").cast("string").alias("raw_json"),
              F.col("timestamp").alias("kafka_ts"),
              F.col("topic"), F.col("partition"), F.col("offset"),
              F.col("headers").alias("headers")
          ))

(bronze.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_chkpt)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(bronze_tbl))
