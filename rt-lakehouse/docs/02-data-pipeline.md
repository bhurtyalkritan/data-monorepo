# Data Pipeline Documentation

## Overview

The RT-Lakehouse implements a medallion architecture with three distinct layers for data refinement: Bronze (raw ingestion), Silver (cleaned and validated), and Gold (business-ready aggregates).

## Bronze Layer - Raw Data Ingestion

### Purpose
Land raw events from Kafka into Delta Lake with minimal processing, preserving all original data and metadata.

### Implementation (`pipelines/10_bronze_from_kafka.py`)

```python
# Key configuration
bronze_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Add metadata and write to Delta
bronze_with_metadata = bronze_df.select(
    col("value").cast("string").alias("raw_data"),
    col("key").cast("string").alias("kafka_key"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_time")
)
```

### Key Features
- **Raw Preservation**: Complete event data stored as JSON strings
- **Kafka Metadata**: Topic, partition, offset tracking
- **Ingestion Timestamps**: Audit trail for debugging
- **Schema Flexibility**: No schema enforcement at bronze level
- **Checkpoint Management**: Automatic recovery from failures

### Output Schema
```sql
CREATE TABLE bronze_events (
    raw_data STRING,
    kafka_key STRING,
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingestion_time TIMESTAMP
) USING DELTA
```

## Silver Layer - Data Transformation

### Purpose
Parse, validate, clean, and deduplicate bronze data into a structured, analytics-ready format.

### Implementation (`pipelines/20_silver_transform.py`)

```python
# Parse JSON and validate schema
silver_parsed = bronze_stream.select(
    get_json_object(col("raw_data"), "$.event_id").alias("event_id"),
    get_json_object(col("raw_data"), "$.user_id").alias("user_id"),
    get_json_object(col("raw_data"), "$.event_type").alias("event_type"),
    # ... additional fields
    col("ingestion_time")
).filter(
    col("event_type").isin(["page_view", "add_to_cart", "purchase"])
)

# Add watermark for late data handling
silver_with_watermark = silver_parsed.withWatermark("ts", "15 seconds")

# Deduplicate on event_id
silver_deduped = silver_with_watermark.dropDuplicates(["event_id"])
```

### Data Quality Features
- **Schema Validation**: Enforce event contract compliance
- **Type Casting**: Convert strings to appropriate data types
- **Event Filtering**: Only allow valid event types
- **Deduplication**: Remove duplicate events based on event_id
- **Watermarking**: Handle late-arriving data (15-second tolerance)
- **Data Enrichment**: Add calculated fields and normalizations

### Watermark Strategy
```python
# Late data handling configuration
WATERMARK_DELAY = "15 seconds"  # Allow 15 seconds for late arrivals
CHECKPOINT_INTERVAL = "10 seconds"  # Recovery checkpoint frequency
```

### Output Schema
```sql
CREATE TABLE silver_events (
    event_id STRING,
    user_id STRING,
    product_id STRING,
    event_type STRING,
    price DECIMAL(10,2),
    quantity INT,
    currency STRING,
    ts TIMESTAMP,
    country STRING,
    device STRING,
    campaign STRING,
    ingestion_time TIMESTAMP
) USING DELTA
```

## Gold Layer - Business Aggregates

### Purpose
Create business-ready KPIs and metrics through windowed aggregations for real-time analytics.

### Implementation (`pipelines/30_gold_kpis.py`)

```python
# 1-minute windowed aggregations
gold_kpis = silver_stream.groupBy(
    window(col("ts"), "1 minute")
).agg(
    # Core business metrics
    sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("orders"),
    sum(when(col("event_type") == "purchase", col("price") * col("quantity")).otherwise(0)).alias("gmv"),
    
    # User engagement metrics
    countDistinct(when(col("event_type") == "purchase", col("user_id"))).alias("purchase_users"),
    countDistinct(when(col("event_type") == "page_view", col("user_id"))).alias("view_users"),
    countDistinct(col("user_id")).alias("active_users"),
    
    # Calculated KPIs
    (countDistinct(when(col("event_type") == "purchase", col("user_id"))) / 
     greatest(countDistinct(when(col("event_type") == "page_view", col("user_id"))), lit(1))).alias("conversion_rate")
)
```

### Business Metrics

#### Core KPIs
- **Orders**: Count of completed purchases
- **GMV (Gross Merchandise Value)**: Total revenue (price × quantity)
- **Active Users**: Unique users with any activity
- **Purchase Users**: Unique users who made purchases
- **View Users**: Unique users who viewed products
- **Conversion Rate**: Purchase users / View users

#### Window Configuration
- **Window Size**: 1 minute (configurable)
- **Window Type**: Tumbling windows (non-overlapping)
- **Watermark**: Aligned with Silver layer (15 seconds)
- **Output Mode**: Append (for incremental updates)

### Output Schema
```sql
CREATE TABLE gold_kpis (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    orders BIGINT,
    gmv DECIMAL(15,2),
    purchase_users BIGINT,
    view_users BIGINT,
    active_users BIGINT,
    conversion_rate DECIMAL(5,4)
) USING DELTA
```

## Streaming Configuration

### Memory Optimization
```yaml
# Spark configuration for optimal streaming
SPARK_DRIVER_MEMORY: 4g
SPARK_EXECUTOR_MEMORY: 4g
SPARK_DRIVER_MAX_RESULT_SIZE: 2g

# Container resource limits
memory_limit: 6g
memory_reservation: 4g
```

### Checkpoint Management
```python
# Checkpoint configuration for fault tolerance
.option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
.option("maxFilesPerTrigger", 1000)
.trigger(processingTime="10 seconds")
```

### Output Modes
- **Bronze**: Append mode (continuous ingestion)
- **Silver**: Append mode (incremental processing)
- **Gold**: Append mode (windowed aggregates)

## Data Export Strategy

### Parquet Snapshots
Periodic export of latest data to Parquet format for fast analytical queries:

```python
# Export strategy for API consumption
def export_to_parquet():
    # Silver latest snapshot
    silver_df.write.mode("overwrite").parquet("/delta/silver_latest.parquet")
    
    # Gold latest snapshot
    gold_df.write.mode("overwrite").parquet("/delta/gold_latest.parquet")
```

### Benefits
- **Fast Queries**: DuckDB optimized for Parquet
- **Decoupled Serving**: API doesn't impact streaming
- **Snapshot Consistency**: Point-in-time data views
- **Cache Invalidation**: ETag-based cache management

## Monitoring and Observability

### Streaming Metrics
- Processing rate (events/second)
- End-to-end latency (ingestion to gold)
- Watermark progression
- Checkpoint lag

### Data Quality Metrics
- Schema compliance rate
- Duplicate detection rate
- Late arrival statistics
- Data freshness indicators

### Operational Metrics
- Memory utilization
- Checkpoint duration
- Query execution times
- Error rates and retry logic
