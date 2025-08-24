# Databricks Lakehouse RT config
# Run this at the top of every notebook via `%run ./00_config`

catalog = "demo"
schema  = "ecommerce_rt"
db      = f"{catalog}.{schema}"

bronze_tbl   = f"{db}.bronze_events"
silver_tbl   = f"{db}.silver_events"
gold_kpi_tbl = f"{db}.gold_kpis"
alerts_tbl   = f"{db}.alerts"

checkpoints  = "dbfs:/pipelines/checkpoints/ecommerce_rt"
storage_root = "dbfs:/pipelines/data/ecommerce_rt"

kafka_bootstrap = "localhost:9092"    
kafka_topic     = "ecommerce_events"

from pyspark.sql.types import *
event_schema = StructType([
  StructField("event_id", StringType()),
  StructField("user_id", StringType()),
  StructField("product_id", StringType()),
  StructField("event_type", StringType()),
  StructField("price", DoubleType()),
  StructField("quantity", IntegerType()),
  StructField("currency", StringType()),
  StructField("ts", TimestampType()),
  StructField("ua", StringType()),
  StructField("country", StringType())
])
