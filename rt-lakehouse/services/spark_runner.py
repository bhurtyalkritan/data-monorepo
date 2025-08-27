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
GOLD_WATERMARK = os.getenv("GOLD_WATERMARK", "10 minutes")
ENABLE_GOLD_MERGE_BACKFILL = os.getenv("ENABLE_GOLD_MERGE_BACKFILL", "1").lower() in ("1","true","yes")
MERGE_INTERVAL_SEC = int(os.getenv("MERGE_INTERVAL_SEC", "30"))

# Event schema (updated with new attributes)
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
    StructField("country", StringType()),
    # New optional attributes from producer
    StructField("session_id", StringType()),
    StructField("device", StringType()),
    StructField("campaign", StringType()),
    StructField("referrer", StringType()),
])

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    # Memory configuration from environment
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    max_result_size = os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "2g")
    
    builder = (SparkSession.builder
        .appName("RT-Lakehouse-Streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Memory configuration
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.maxResultSize", max_result_size)
        # Additional memory optimizations
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.executor.memoryFraction", "0.8")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        # Delta Lake time travel configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    
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
        .option("includeHeaders", "true")
        .load())
    
    # Transform to bronze format
    bronze = (raw_stream
        .select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("topic"), F.col("partition"), F.col("offset"),
            F.col("headers").alias("headers")
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
    
    silver_path = f"{DELTA_PATH}/silver_events"
    checkpoint_path = f"/checkpoints/silver"
    bronze_path = f"{DELTA_PATH}/bronze_events"
    
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
    
    # 1-minute windows with comprehensive aggregation
    win = F.window("ts", "1 minute")
    
    # Single aggregation with all metrics
    gold = (events
            .groupBy(win)
            .agg(
                F.count("*").alias("total_events"),
                F.approx_count_distinct("user_id").alias("active_users"),
                F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("orders"),
                F.sum(F.when(F.col("event_type") == "purchase", F.col("price") * F.col("quantity")).otherwise(0.0)).alias("gmv"),
                F.approx_count_distinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("purchase_users"),
                F.approx_count_distinct(F.when(F.col("event_type") == "page_view", F.col("user_id"))).alias("view_users")
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("orders"),
                F.col("gmv"),
                F.col("purchase_users"),
                F.col("view_users"),
                F.col("active_users"),
                (F.col("purchase_users").cast("double") / 
                 F.when(F.col("view_users") == 0, F.lit(1)).otherwise(F.col("view_users"))).alias("conversion_rate")
            ))

    # Write to Delta
    query = (gold.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
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
        try:
            silver_df = spark.read.format("delta").load(silver_path)
            silver_count = silver_df.count()
            if silver_count > 0:
                silver_df.write.mode("overwrite").parquet(f"{DELTA_PATH}/silver_latest.parquet")
                logger.info(f"Exported {silver_count} silver records to parquet")
        except Exception as e:
            logger.warning(f"Silver export failed: {e}")
        
        try:
            gold_df = spark.read.format("delta").load(gold_path)
            gold_count = gold_df.count()
            if gold_count > 0:
                gold_df.write.mode("overwrite").parquet(f"{DELTA_PATH}/gold_latest.parquet")
                logger.info(f"Exported {gold_count} gold records to parquet")
            else:
                logger.warning("Gold table is empty, no export performed")
        except Exception as e:
            logger.warning(f"Gold export failed: {e}")
        
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

def get_table_history(spark, table_path, limit=10):
    """Get Delta table version history for time travel"""
    try:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, table_path)
        history = delta_table.history(limit).collect()
        
        logger.info(f"Table {table_path} history (last {limit} versions):")
        for row in history:
            version = row['version']
            timestamp = row['timestamp']
            operation = row['operation']
            logger.info(f"  Version {version}: {operation} at {timestamp}")
        
        return history
    except Exception as e:
        logger.warning(f"Failed to get history for {table_path}: {e}")
        return []

def read_table_at_version(spark, table_path, version=None, timestamp=None):
    """Read Delta table at specific version or timestamp"""
    try:
        if version is not None:
            df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
            logger.info(f"Reading {table_path} at version {version}")
        elif timestamp is not None:
            df = spark.read.format("delta").option("timestampAsOf", timestamp).load(table_path)
            logger.info(f"Reading {table_path} at timestamp {timestamp}")
        else:
            df = spark.read.format("delta").load(table_path)
            logger.info(f"Reading {table_path} latest version")
        
        return df
    except Exception as e:
        logger.error(f"Failed to read {table_path} at version/timestamp: {e}")
        return None

def setup_time_travel_views(spark):
    """Create SQL views for easy time travel access"""
    try:
        bronze_path = f"{DELTA_PATH}/bronze_events"
        silver_path = f"{DELTA_PATH}/silver_events"
        gold_path = f"{DELTA_PATH}/gold_kpis"
        
        # Register Delta tables as SQL views for time travel queries
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW bronze_events USING DELTA LOCATION '{bronze_path}'")
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW silver_events USING DELTA LOCATION '{silver_path}'")
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW gold_kpis USING DELTA LOCATION '{gold_path}'")
        
        logger.info("Time travel views created successfully")
        
        # Log table histories for debugging
        get_table_history(spark, bronze_path, 5)
        get_table_history(spark, silver_path, 5)
        get_table_history(spark, gold_path, 5)
        
    except Exception as e:
        logger.warning(f"Failed to setup time travel views: {e}")

def export_time_travel_data(spark):
    """Export time travel capabilities to DuckDB-accessible format"""
    try:
        bronze_path = f"{DELTA_PATH}/bronze_events"
        silver_path = f"{DELTA_PATH}/silver_events"
        gold_path = f"{DELTA_PATH}/gold_kpis"
        
        # Export table histories as JSON for API access
        histories = {}
        
        for table_name, table_path in [
            ("bronze", bronze_path),
            ("silver", silver_path), 
            ("gold", gold_path)
        ]:
            try:
                from delta.tables import DeltaTable
                delta_table = DeltaTable.forPath(spark, table_path)
                history_df = delta_table.history(20)  # Last 20 versions
                
                # Convert to pandas and then to JSON
                history_rows = history_df.select(
                    "version", "timestamp", "operation", "operationParameters"
                ).collect()
                
                histories[table_name] = [
                    {
                        "version": row.version,
                        "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                        "operation": row.operation,
                        "parameters": row.operationParameters
                    }
                    for row in history_rows
                ]
                
                # Export history to JSON file for API access
                import json
                with open(f"{DELTA_PATH}/{table_name}_history.json", "w") as f:
                    json.dump(histories[table_name], f, indent=2)
                
                logger.info(f"Exported {table_name} history: {len(histories[table_name])} versions")
                
            except Exception as e:
                logger.warning(f"Failed to export {table_name} history: {e}")
                
        return histories
        
    except Exception as e:
        logger.warning(f"Time travel data export failed: {e}")
        return {}

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
                    time.sleep(max(30, MERGE_INTERVAL_SEC))
            threading.Thread(target=_merge_loop, daemon=True).start()
        
        # Setup DuckDB integration (periodic)
        def update_duckdb():
            while True:
                try:
                    setup_duckdb_integration(spark)
                    # Setup time travel views and export history data
                    setup_time_travel_views(spark)
                    export_time_travel_data(spark)
                    time.sleep(30)  # Update every 30 seconds for faster refresh
                except Exception as e:
                    logger.error(f"DuckDB update failed: {e}")
                    time.sleep(30)
        
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
