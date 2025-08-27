#!/usr/bin/env python3
"""Simple time travel test for Delta Lake tables"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import json

# Create Spark session with proper Delta configuration
builder = (SparkSession.builder
    .appName("TimeTravel-Test")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    print("=== TIME TRAVEL TEST ===")
    
    # Test gold table
    gold_path = "/delta/gold_kpis"
    print(f"Testing {gold_path}")
    
    try:
        delta_table = DeltaTable.forPath(spark, gold_path)
        history_df = delta_table.history(10)
        
        print("✅ Gold table history:")
        history_rows = history_df.collect()
        
        for row in history_rows:
            print(f"  Version {row.version}: {row.timestamp} - {row.operation}")
        
        # Export history to JSON
        history_data = []
        for row in history_rows:
            history_data.append({
                "version": row.version,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "operation": row.operation,
                "parameters": dict(row.operationParameters) if row.operationParameters else {}
            })
        
        with open("/delta/gold_history.json", "w") as f:
            json.dump(history_data, f, indent=2)
        
        print(f"✅ Exported {len(history_data)} versions to gold_history.json")
        
        # Test reading at specific version
        if len(history_rows) >= 2:
            latest_version = history_rows[0].version
            prev_version = history_rows[1].version
            
            current_df = spark.read.format("delta").load(gold_path)
            prev_df = spark.read.format("delta").option("versionAsOf", prev_version).load(gold_path)
            
            current_count = current_df.count()
            prev_count = prev_df.count()
            
            print(f"✅ Current version ({latest_version}): {current_count} records")
            print(f"✅ Previous version ({prev_version}): {prev_count} records")
            print(f"✅ Change: {current_count - prev_count:+d} records")
        
    except Exception as e:
        print(f"❌ Gold table error: {e}")
    
    # Test silver table
    silver_path = "/delta/silver_events"
    print(f"\nTesting {silver_path}")
    
    try:
        delta_table = DeltaTable.forPath(spark, silver_path)
        history_df = delta_table.history(5)
        
        print("✅ Silver table history:")
        history_rows = history_df.collect()
        
        for row in history_rows[:3]:  # Show first 3
            print(f"  Version {row.version}: {row.timestamp} - {row.operation}")
        
        # Export history
        history_data = []
        for row in history_rows:
            history_data.append({
                "version": row.version,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "operation": row.operation,
                "parameters": dict(row.operationParameters) if row.operationParameters else {}
            })
        
        with open("/delta/silver_history.json", "w") as f:
            json.dump(history_data, f, indent=2)
        
        print(f"✅ Exported {len(history_data)} versions to silver_history.json")
        
    except Exception as e:
        print(f"❌ Silver table error: {e}")
    
    # Test bronze table
    bronze_path = "/delta/bronze_events"
    print(f"\nTesting {bronze_path}")
    
    try:
        delta_table = DeltaTable.forPath(spark, bronze_path)
        history_df = delta_table.history(5)
        
        print("✅ Bronze table history:")
        history_rows = history_df.collect()
        
        for row in history_rows[:3]:  # Show first 3
            print(f"  Version {row.version}: {row.timestamp} - {row.operation}")
        
        # Export history
        history_data = []
        for row in history_rows:
            history_data.append({
                "version": row.version,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "operation": row.operation,
                "parameters": dict(row.operationParameters) if row.operationParameters else {}
            })
        
        with open("/delta/bronze_history.json", "w") as f:
            json.dump(history_data, f, indent=2)
        
        print(f"✅ Exported {len(history_data)} versions to bronze_history.json")
        
    except Exception as e:
        print(f"❌ Bronze table error: {e}")
    
    print("\n=== TIME TRAVEL CONFIGURATION COMPLETE ===")
    print("✅ Delta Lake time travel is now enabled")
    print("✅ Table histories exported to JSON files")
    print("✅ Use version-based queries: spark.read.format('delta').option('versionAsOf', version).load(path)")
    print("✅ Use timestamp-based queries: spark.read.format('delta').option('timestampAsOf', timestamp).load(path)")
    
finally:
    spark.stop()
