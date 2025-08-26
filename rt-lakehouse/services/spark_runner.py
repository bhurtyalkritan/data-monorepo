import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
DELTA_PATH = os.getenv("DELTA_PATH", "/delta")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/delta/lakehouse.db")

# Tunables
SILVER_WATERMARK = os.getenv("SILVER_WATERMARK", "10 minutes")
GOLD_WATERMARK = os.getenv("GOLD_WATERMARK", "30 seconds")
ENABLE_GOLD_MERGE_BACKFILL = os.getenv("ENABLE_GOLD_MERGE_BACKFILL", "0").lower() in ("1","true","yes")
MERGE_INTERVAL_SEC = int(os.getenv("MERGE_INTERVAL_SEC", "120"))

# Event schema
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

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    builder = (SparkSession.builder
        .appName("RT-Lakehouse-Streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true"))
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_bronze_pipeline(spark):
    """Bronze: Kafka -> Delta (raw)"""
    logger.info("Starting Bronze pipeline...")
    
    bronze_path = f"{DELTA_PATH}/bronze_events"
    checkpoint_path = f"/checkpoints/bronze"
    
    # Read from Kafka
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())
    
    # Transform to bronze format
    bronze = (raw_stream
        .select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("topic"), F.col("partition"), F.col("offset")
        ))
    
    # Write to Delta
    query = (bronze.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("path", bronze_path)
        .start())
    
    return query

def wait_for_delta_table(spark, table_path, max_wait_minutes=10):
    """Wait for a Delta table to exist and have data"""
    logger.info(f"Waiting for Delta table at {table_path}...")
    
    for i in range(max_wait_minutes * 12):  # Check every 5 seconds
        try:
            # Try to read the table
            df = spark.read.format("delta").load(table_path)
            count = df.count()
            if count > 0:
                logger.info(f"Delta table {table_path} is ready with {count} records")
                return True
            else:
                logger.info(f"Delta table {table_path} exists but is empty, waiting...")
        except Exception as e:
            if i == 0:
                logger.info(f"Delta table {table_path} doesn't exist yet, waiting...")
        
        time.sleep(5)
    
    logger.warning(f"Delta table {table_path} still not ready after {max_wait_minutes} minutes")
    return False

def run_silver_pipeline(spark):
    """Silver: parse, validate, dedupe"""
    logger.info("Starting Silver pipeline...")
    
    bronze_path = f"{DELTA_PATH}/bronze_events"
    silver_path = f"{DELTA_PATH}/silver_events"
    checkpoint_path = f"/checkpoints/silver"
    
    # Wait for bronze table to have data
    if not wait_for_delta_table(spark, bronze_path):
        raise Exception("Bronze table not ready, cannot start silver pipeline")
    
    # Read from Bronze Delta table
    bronze = spark.readStream.format("delta").load(bronze_path)
    
    # Parse JSON and clean
    parsed = (bronze
        .withColumn("json", F.from_json(F.col("raw_json"), event_schema))
        .select("kafka_ts", "topic", "partition", "offset", "json.*"))
    
    clean = (parsed
        .withColumn("ts", F.coalesce(F.col("ts"), F.col("kafka_ts")))
        .withColumn("price", F.when(F.col("price").isNull(), F.lit(0.0)).otherwise(F.col("price")))
        .withColumn("quantity", F.when(F.col("quantity").isNull(), F.lit(0)).otherwise(F.col("quantity")))
        .withColumn("event_type", F.lower(F.col("event_type")))
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isin("page_view", "add_to_cart", "purchase")))
    
    # Dedupe
    deduped = (clean
        .withWatermark("ts", SILVER_WATERMARK)
        .dropDuplicates(["event_id"]))
    
    # Write to Delta
    query = (deduped.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .option("path", silver_path)
        .start())
    
    return query

def run_gold_pipeline(spark):
    """Gold: KPI aggregates"""
    logger.info("Starting Gold pipeline...")
    
    silver_path = f"{DELTA_PATH}/silver_events"
    gold_path = f"{DELTA_PATH}/gold_kpis"
    checkpoint_path = f"/checkpoints/gold"
    
    # Wait for silver table to have data
    if not wait_for_delta_table(spark, silver_path):
        raise Exception("Silver table not ready, cannot start gold pipeline")
    
    # Read from Silver
    events = (spark.readStream
              .format("delta")
              .load(silver_path)
              .withWatermark("ts", GOLD_WATERMARK))
    
    # 1-minute windows
    win = F.window("ts", "1 minute")
    
    # Aggregations
    views = (events.filter(F.col("event_type") == "page_view")
             .groupBy(win)
             .agg(F.approx_count_distinct("user_id").alias("view_users")))
    
    purchases = (events.filter(F.col("event_type") == "purchase")
                 .withColumn("revenue", F.col("price") * F.col("quantity"))
                 .groupBy(win)
                 .agg(
                     F.count("*").alias("orders"),
                     F.sum("revenue").alias("gmv"),
                     F.approx_count_distinct("user_id").alias("purchase_users")))
    
    active = (events.groupBy(win)
              .agg(F.approx_count_distinct("user_id").alias("active_users")))
    
    # Join and calculate conversion rate
    gold = (purchases.join(views, on="window", how="fullouter")
                     .join(active, on="window", how="fullouter")
                     .select(
                         F.col("window.start").alias("window_start"),
                         F.col("window.end").alias("window_end"),
                         F.coalesce("orders", F.lit(0)).alias("orders"),
                         F.coalesce("gmv", F.lit(0.0)).alias("gmv"),
                         F.coalesce("purchase_users", F.lit(0)).alias("purchase_users"),
                         F.coalesce("view_users", F.lit(0)).alias("view_users"),
                         F.coalesce("active_users", F.lit(0)).alias("active_users"),
                         (F.col("purchase_users") / F.when(F.col("view_users") == 0, F.lit(1)).otherwise(F.col("view_users"))).alias("conversion_rate")))
    
    # Write to Delta
    query = (gold.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("complete")
        .trigger(processingTime="10 seconds")
        .option("path", gold_path)
        .start())
    
    return query

def setup_duckdb_integration(spark):
    """Setup DuckDB views for external access"""
    logger.info("Setting up DuckDB integration...")
    
    try:
        # Create batch views for DuckDB access
        silver_path = f"{DELTA_PATH}/silver_events"
        gold_path = f"{DELTA_PATH}/gold_kpis"
        
        # Read latest data and write to parquet for DuckDB
        silver_df = spark.read.format("delta").load(silver_path)
        gold_df = spark.read.format("delta").load(gold_path)
        
        # Write to parquet (DuckDB can read this efficiently)
        silver_df.write.mode("overwrite").parquet(f"{DELTA_PATH}/silver_latest.parquet")
        gold_df.write.mode("overwrite").parquet(f"{DELTA_PATH}/gold_latest.parquet")
        
        logger.info("DuckDB integration setup complete")
    except Exception as e:
        logger.warning(f"DuckDB integration setup failed: {e}")

def run_gold_incremental_merge(spark):
    """Optional DP-style incremental upsert into gold table (disabled by default).
    WARNING: Running alongside a streaming writer to the same path can cause contention.
    Enable only if you disable the streaming gold writer, or schedule during idle windows.
    """
    silver_path = f"{DELTA_PATH}/silver_events"
    gold_path = f"{DELTA_PATH}/gold_kpis"

    try:
        silver_df = spark.read.format("delta").load(silver_path)
        minute_agg = (
            silver_df
            .withColumn("window_start", F.date_trunc("minute", F.col("ts").cast("timestamp")))
            .groupBy("window_start")
            .agg(
                F.sum(F.when(F.col("event_type")=="purchase", 1).otherwise(0)).alias("orders"),
                F.sum(F.when(F.col("event_type")=="purchase", F.col("price")*F.col("quantity")).otherwise(0.0)).alias("gmv"),
                F.countDistinct(F.when(F.col("event_type")=="purchase", F.col("user_id"))).alias("purchase_users"),
                F.countDistinct(F.when(F.col("event_type")=="page_view", F.col("user_id"))).alias("view_users"),
                F.countDistinct("user_id").alias("active_users")
            )
            .withColumn(
                "conversion_rate",
                (F.col("purchase_users") / F.when(F.col("view_users")==0, F.lit(1)).otherwise(F.col("view_users"))).cast("double")
            )
            .withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 1 MINUTE"))
            .select("window_start","window_end","orders","gmv","purchase_users","view_users","active_users","conversion_rate")
        )
        minute_agg.createOrReplaceTempView("minute_agg")
        spark.sql(f"""
        MERGE INTO delta.`{gold_path}` AS g
        USING minute_agg AS m
        ON g.window_start = m.window_start
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info("Gold incremental MERGE completed")
    except Exception as e:
        logger.warning(f"Gold incremental MERGE failed: {e}")

def main():
    """Main streaming pipeline runner"""
    logger.info("Starting RT-Lakehouse streaming pipeline...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka...")
    time.sleep(30)
    
    try:
        # Start all streaming queries
        bronze_query = run_bronze_pipeline(spark)
        time.sleep(10)  # Let bronze start
        
        silver_query = run_silver_pipeline(spark)
        time.sleep(10)  # Let silver start
        
        gold_query = run_gold_pipeline(spark)
        
        # Optional: background incremental MERGE job (disabled by default)
        if ENABLE_GOLD_MERGE_BACKFILL:
            import threading
            def _merge_loop():
                while True:
                    try:
                        run_gold_incremental_merge(spark)
                    except Exception as e:
                        logger.error(f"Merge loop error: {e}")
                    time.sleep(max(10, MERGE_INTERVAL_SEC))
            threading.Thread(target=_merge_loop, daemon=True).start()
        
        # Setup DuckDB integration (periodic)
        def update_duckdb():
            while True:
                try:
                    setup_duckdb_integration(spark)
                    time.sleep(60)  # Update every minute
                except Exception as e:
                    logger.error(f"DuckDB update failed: {e}")
                    time.sleep(60)
        
        import threading
        duckdb_thread = threading.Thread(target=update_duckdb, daemon=True)
        duckdb_thread.start()
        
        logger.info("All streaming queries started successfully")
        
        # Wait for queries to finish
        bronze_query.awaitTermination()
        silver_query.awaitTermination()
        gold_query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Streaming pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
