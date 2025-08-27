# Delta Lake & Time Travel Documentation

## Overview

The RT-Lakehouse leverages Delta Lake for ACID transactions, schema evolution, and advanced time travel capabilities. Every table maintains complete versioning history with point-in-time query support.

## Delta Lake Configuration

### Spark Integration
```python
# Delta Lake Spark configuration
spark = SparkSession.builder \
    .appName("RT-Lakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()
```

### Storage Structure
```
/delta/
├── bronze_events/           # Raw event ingestion
│   ├── _delta_log/         # Transaction log
│   ├── part-*.parquet      # Data files
│   └── _committed_*        # Commit markers
├── silver_events/          # Cleaned, validated events
│   ├── _delta_log/
│   └── part-*.parquet
├── gold_kpis/             # Business aggregates
│   ├── _delta_log/
│   └── part-*.parquet
├── silver_latest.parquet/ # API query snapshots
├── gold_latest.parquet/   # API query snapshots
└── lakehouse.db          # DuckDB database file
```

## Time Travel Capabilities

### Version-Based Queries
Query specific table versions using Delta Lake's transaction log:

```python
# Read data at a specific version
df_v10 = spark.read.format("delta").option("versionAsOf", 10).load("/delta/gold_kpis")

# Read data at current version
df_current = spark.read.format("delta").load("/delta/gold_kpis")

# Compare versions
current_count = df_current.count()
v10_count = df_v10.count()
print(f"Records added since v10: {current_count - v10_count}")
```

### Timestamp-Based Queries
Query data as it existed at a specific point in time:

```python
# Read data as of specific timestamp
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2025-08-26T10:00:00Z") \
    .load("/delta/gold_kpis")

# Read data 1 hour ago
from datetime import datetime, timedelta
one_hour_ago = datetime.now() - timedelta(hours=1)
df_1h_ago = spark.read.format("delta") \
    .option("timestampAsOf", one_hour_ago.isoformat()) \
    .load("/delta/gold_kpis")
```

## Time Travel Implementation

### History Tracking (`services/spark_runner.py`)
```python
def get_table_history(table_path: str, limit: int = 50):
    """Get complete version history for a Delta table"""
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        history_df = delta_table.history(limit)
        
        history = []
        for row in history_df.collect():
            history.append({
                "version": row.version,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "operation": row.operation,
                "parameters": dict(row.operationParameters) if row.operationParameters else {},
                "readVersion": row.readVersion,
                "isolationLevel": row.isolationLevel,
                "isBlindAppend": row.isBlindAppend
            })
        
        return history
    except Exception as e:
        logger.error(f"Failed to get history for {table_path}: {e}")
        return []
```

### Version Comparison
```python
def read_table_at_version(table_path: str, version: int):
    """Read Delta table at specific version"""
    try:
        df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
        return df.toPandas().to_dict('records')
    except Exception as e:
        logger.error(f"Failed to read {table_path} at version {version}: {e}")
        return []

def compare_versions(table_path: str, version1: int, version2: int):
    """Compare two versions of a Delta table"""
    data1 = read_table_at_version(table_path, version1)
    data2 = read_table_at_version(table_path, version2)
    
    return {
        "version1": {"version": version1, "count": len(data1), "sample": data1[:5]},
        "version2": {"version": version2, "count": len(data2), "sample": data2[:5]},
        "difference": {"records_added": len(data2) - len(data1)}
    }
```

## Time Travel API Endpoints

### History Endpoint (`services/assistant_api.py`)
```python
@app.get("/timetravel/history")
async def time_travel_history(
    table: str = Query(..., description="Table name: bronze, silver, or gold"),
    limit: int = Query(20, description="Number of versions to return")
):
    """Get version history for Delta tables"""
    table_paths = {
        "bronze": "/delta/bronze_events",
        "silver": "/delta/silver_events", 
        "gold": "/delta/gold_kpis"
    }
    
    if table not in table_paths:
        raise HTTPException(status_code=400, detail="Invalid table name")
    
    history = get_table_history(table_paths[table], limit)
    return {"table": table, "history": history}
```

### Point-in-Time Data Endpoint
```python
@app.get("/timetravel/data")
async def time_travel_data(
    table: str = Query(...),
    version: Optional[int] = Query(None),
    timestamp: Optional[str] = Query(None),
    limit: int = Query(100)
):
    """Get data from specific version or timestamp"""
    if not version and not timestamp:
        raise HTTPException(status_code=400, detail="Either version or timestamp required")
    
    table_paths = {
        "bronze": "/delta/bronze_events",
        "silver": "/delta/silver_events",
        "gold": "/delta/gold_kpis"
    }
    
    if version:
        data = read_table_at_version(table_paths[table], version)
    else:
        data = read_table_at_timestamp(table_paths[table], timestamp)
    
    return {"table": table, "version": version, "timestamp": timestamp, "data": data[:limit]}
```

### Version Comparison Endpoint
```python
@app.get("/timetravel/compare")
async def time_travel_compare(
    table: str = Query(...),
    version1: int = Query(...),
    version2: int = Query(...)
):
    """Compare two versions of a Delta table"""
    table_paths = {
        "bronze": "/delta/bronze_events",
        "silver": "/delta/silver_events",
        "gold": "/delta/gold_kpis"
    }
    
    comparison = compare_versions(table_paths[table], version1, version2)
    return {"table": table, "comparison": comparison}
```

## Time Travel CLI Tool

### Time Travel Manager (`services/time_travel_manager.py`)
Comprehensive CLI tool for Delta Lake time travel operations:

```bash
# Get table history
python time_travel_manager.py history --table gold --limit 10

# Read data at specific version
python time_travel_manager.py read --table gold --version 5

# Read data at timestamp
python time_travel_manager.py read --table gold --timestamp "2025-08-27T10:00:00Z"

# Compare versions
python time_travel_manager.py compare --table gold --version1 5 --version2 10

# Export version data
python time_travel_manager.py export --table gold --version 5 --output /tmp/gold_v5.json
```

### Implementation Features
- **Interactive Mode**: Browse history with arrow keys
- **Export Capabilities**: JSON, CSV, Parquet output formats
- **Validation**: Version and timestamp validation
- **Error Handling**: Graceful failure with informative messages
- **Batch Operations**: Multiple table operations in single command

## Time Travel Test Suite

### Automated Testing (`test_time_travel.py`)
```python
#!/usr/bin/env python3
"""Comprehensive time travel validation"""

def test_time_travel_functionality():
    """Test all time travel capabilities"""
    
    # Test version history
    history = get_table_history("/delta/gold_kpis", 10)
    assert len(history) > 0, "Should have version history"
    
    # Test version-based reads
    if len(history) >= 2:
        latest_version = history[0]["version"]
        prev_version = history[1]["version"]
        
        current_data = read_table_at_version("/delta/gold_kpis", latest_version)
        prev_data = read_table_at_version("/delta/gold_kpis", prev_version)
        
        print(f"✅ Version {latest_version}: {len(current_data)} records")
        print(f"✅ Version {prev_version}: {len(prev_data)} records")
        print(f"✅ Change: {len(current_data) - len(prev_data):+d} records")
    
    # Export history for analysis
    with open("/delta/gold_history.json", "w") as f:
        json.dump(history, f, indent=2)
    
    print("✅ Time travel test completed successfully")
```

## Advanced Time Travel Features

### Schema Evolution Tracking
```sql
-- View schema changes over time
DESCRIBE HISTORY delta.`/delta/gold_kpis`
WHERE operation = 'ADD COLUMNS' OR operation = 'CHANGE COLUMN'
```

### Data Lineage Through Time
```python
def trace_data_lineage(event_id: str):
    """Trace an event through all pipeline stages and versions"""
    
    # Find in bronze
    bronze_versions = find_event_in_versions("/delta/bronze_events", event_id)
    
    # Find in silver  
    silver_versions = find_event_in_versions("/delta/silver_events", event_id)
    
    # Find impact in gold
    gold_impact = find_gold_impact(event_id)
    
    return {
        "event_id": event_id,
        "bronze_history": bronze_versions,
        "silver_history": silver_versions,
        "gold_impact": gold_impact
    }
```

### Time Travel Performance

#### Query Optimization
- **Version Pruning**: Automatic cleanup of old versions
- **File Skipping**: Leverage Delta Lake's file statistics
- **Caching**: Cache frequently accessed historical data
- **Parallel Reads**: Multi-threaded version comparisons

#### Storage Management
```python
# Optimize and vacuum old versions
def maintain_time_travel():
    """Maintain Delta tables for optimal time travel performance"""
    
    for table in ["/delta/bronze_events", "/delta/silver_events", "/delta/gold_kpis"]:
        # Optimize file layout
        spark.sql(f"OPTIMIZE delta.`{table}`")
        
        # Clean up old versions (keep 30 days)
        spark.sql(f"VACUUM delta.`{table}` RETAIN 720 HOURS")
        
        # Z-order for query performance
        spark.sql(f"OPTIMIZE delta.`{table}` ZORDER BY (ts)")
```

## Real-World Use Cases

### Audit and Compliance
- **Regulatory Reporting**: Point-in-time financial metrics
- **Data Lineage**: Track data transformations over time
- **Change Auditing**: See what changed and when

### Debugging and Development
- **Pipeline Issues**: Compare before/after pipeline changes
- **Data Quality**: Identify when bad data was introduced
- **Performance Analysis**: Compare query performance across versions

### Business Analytics
- **Historical Analysis**: Recreate reports as they existed in the past
- **A/B Testing**: Compare metrics across different time periods
- **Trend Analysis**: Analyze how business metrics evolved over time
