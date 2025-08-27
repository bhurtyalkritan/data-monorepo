#!/usr/bin/env python3
"""
Time Travel Manager for RT-Lakehouse Delta Tables

This script provides utilities for:
1. Querying table history
2. Reading data at specific versions/timestamps
3. Comparing versions
4. Managing retention policies
5. Creating time travel snapshots

Usage:
  python time_travel_manager.py history --table silver
  python time_travel_manager.py read --table gold --version 5
  python time_travel_manager.py read --table silver --timestamp "2025-08-27 10:30:00"
  python time_travel_manager.py compare --table gold --version1 3 --version2 5
  python time_travel_manager.py vacuum --table bronze --dry-run
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Configuration
DELTA_PATH = os.getenv("DELTA_PATH", "/delta")
DEFAULT_RETENTION_DAYS = int(os.getenv("DELTA_RETENTION_DAYS", "30"))

def create_spark_session():
    """Create Spark session optimized for time travel operations"""
    builder = (SparkSession.builder
        .appName("RT-Lakehouse-TimeTravel")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Memory settings for time travel operations
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.maxResultSize", "1g"))
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def get_table_path(table_name):
    """Get full path for table"""
    table_map = {
        "bronze": f"{DELTA_PATH}/bronze_events",
        "silver": f"{DELTA_PATH}/silver_events", 
        "gold": f"{DELTA_PATH}/gold_kpis"
    }
    
    if table_name not in table_map:
        raise ValueError(f"Unknown table: {table_name}. Must be one of: {list(table_map.keys())}")
    
    return table_map[table_name]

def show_history(spark, table_name, limit=20):
    """Show Delta table version history"""
    table_path = get_table_path(table_name)
    
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        history_df = delta_table.history(limit)
        
        print(f"\n=== {table_name.upper()} TABLE HISTORY (Last {limit} versions) ===")
        print(f"Table Path: {table_path}")
        print("-" * 100)
        
        history_rows = history_df.select(
            "version", "timestamp", "operation", "operationParameters", 
            "readVersion", "isBlindAppend"
        ).collect()
        
        if not history_rows:
            print("No history available")
            return
        
        for row in history_rows:
            print(f"Version {row.version:3d} | {row.timestamp} | {row.operation:15s} | "
                  f"Read: {row.readVersion or 'N/A':3s} | Append: {row.isBlindAppend}")
            
            # Show operation parameters if available
            if row.operationParameters:
                params = dict(row.operationParameters)
                if params:
                    key_params = ["outputMode", "queryId", "batchId"]
                    relevant_params = {k: v for k, v in params.items() if k in key_params}
                    if relevant_params:
                        print(f"         Parameters: {relevant_params}")
        
        print(f"\nTotal versions available: {len(history_rows)}")
        
        # Show current table stats
        current_df = spark.read.format("delta").load(table_path)
        current_count = current_df.count()
        print(f"Current record count: {current_count:,}")
        
        return history_rows
        
    except Exception as e:
        print(f"Error reading history for {table_name}: {e}")
        return []

def read_at_version(spark, table_name, version=None, timestamp=None, limit=10, show_schema=False):
    """Read Delta table at specific version or timestamp"""
    table_path = get_table_path(table_name)
    
    try:
        if version is not None:
            df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
            print(f"\n=== {table_name.upper()} DATA AT VERSION {version} ===")
        elif timestamp is not None:
            df = spark.read.format("delta").option("timestampAsOf", timestamp).load(table_path)
            print(f"\n=== {table_name.upper()} DATA AT TIMESTAMP {timestamp} ===")
        else:
            df = spark.read.format("delta").load(table_path)
            print(f"\n=== {table_name.upper()} CURRENT DATA ===")
        
        print(f"Table Path: {table_path}")
        
        # Show schema if requested
        if show_schema:
            print("\nSchema:")
            df.printSchema()
        
        # Show record count
        total_count = df.count()
        print(f"\nTotal records: {total_count:,}")
        
        # Show sample data
        if limit > 0:
            print(f"\nSample data (limit {limit}):")
            df.limit(limit).show(truncate=False)
        
        return df
        
    except Exception as e:
        print(f"Error reading {table_name} at version/timestamp: {e}")
        return None

def compare_versions(spark, table_name, version1, version2):
    """Compare two versions of a table"""
    table_path = get_table_path(table_name)
    
    try:
        # Read both versions
        df1 = spark.read.format("delta").option("versionAsOf", version1).load(table_path)
        df2 = spark.read.format("delta").option("versionAsOf", version2).load(table_path)
        
        print(f"\n=== COMPARING {table_name.upper()} VERSIONS {version1} vs {version2} ===")
        print(f"Table Path: {table_path}")
        print("-" * 80)
        
        # Basic stats comparison
        count1 = df1.count()
        count2 = df2.count()
        count_diff = count2 - count1
        
        print(f"Version {version1} records: {count1:,}")
        print(f"Version {version2} records: {count2:,}")
        print(f"Difference: {count_diff:+,} records")
        
        # Schema comparison
        schema1 = set(df1.columns)
        schema2 = set(df2.columns)
        
        if schema1 != schema2:
            print(f"\nSchema changes detected:")
            added_cols = schema2 - schema1
            removed_cols = schema1 - schema2
            
            if added_cols:
                print(f"  Added columns: {added_cols}")
            if removed_cols:
                print(f"  Removed columns: {removed_cols}")
        else:
            print(f"Schema unchanged ({len(schema1)} columns)")
        
        # For gold table, show metric differences
        if table_name == "gold" and "orders" in df1.columns and "gmv" in df1.columns:
            print(f"\n=== METRIC COMPARISON ===")
            
            # Aggregate metrics for each version
            agg1 = df1.agg(
                F.sum("orders").alias("total_orders"),
                F.sum("gmv").alias("total_gmv"),
                F.avg("conversion_rate").alias("avg_conversion")
            ).collect()[0]
            
            agg2 = df2.agg(
                F.sum("orders").alias("total_orders"),
                F.sum("gmv").alias("total_gmv"),
                F.avg("conversion_rate").alias("avg_conversion")
            ).collect()[0]
            
            print(f"Orders:     {agg1.total_orders or 0:8.0f} → {agg2.total_orders or 0:8.0f} "
                  f"({(agg2.total_orders or 0) - (agg1.total_orders or 0):+.0f})")
            print(f"GMV:        {agg1.total_gmv or 0:8.2f} → {agg2.total_gmv or 0:8.2f} "
                  f"({(agg2.total_gmv or 0) - (agg1.total_gmv or 0):+.2f})")
            print(f"Conversion: {agg1.avg_conversion or 0:8.4f} → {agg2.avg_conversion or 0:8.4f} "
                  f"({(agg2.avg_conversion or 0) - (agg1.avg_conversion or 0):+.4f})")
        
        return {"count_diff": count_diff, "schema_changes": schema1 != schema2}
        
    except Exception as e:
        print(f"Error comparing versions: {e}")
        return None

def vacuum_table(spark, table_name, retention_hours=None, dry_run=True):
    """Vacuum old versions from Delta table"""
    table_path = get_table_path(table_name)
    
    if retention_hours is None:
        retention_hours = DEFAULT_RETENTION_DAYS * 24
    
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        
        print(f"\n=== VACUUM {table_name.upper()} TABLE ===")
        print(f"Table Path: {table_path}")
        print(f"Retention: {retention_hours} hours ({retention_hours/24:.1f} days)")
        print(f"Dry run: {dry_run}")
        print("-" * 50)
        
        if dry_run:
            # Show what would be deleted
            print("Files that would be deleted:")
            deleted_files = delta_table.vacuum(retentionHours=retention_hours, dryRun=True)
            print(f"Number of files to delete: {len(deleted_files)}")
            
            if deleted_files:
                for f in deleted_files[:10]:  # Show first 10
                    print(f"  {f}")
                if len(deleted_files) > 10:
                    print(f"  ... and {len(deleted_files) - 10} more files")
        else:
            # Actually perform vacuum
            print("Performing vacuum operation...")
            deleted_files = delta_table.vacuum(retentionHours=retention_hours, dryRun=False)
            print(f"Deleted {len(deleted_files)} files")
        
        # Show remaining history after vacuum
        remaining_history = delta_table.history(100).count()
        print(f"Versions remaining after vacuum: {remaining_history}")
        
    except Exception as e:
        print(f"Error vacuuming {table_name}: {e}")

def create_snapshot(spark, table_name, description="Manual snapshot"):
    """Create a snapshot of the current table state"""
    table_path = get_table_path(table_name)
    
    try:
        # Create a checkpoint by triggering an empty operation
        df = spark.read.format("delta").load(table_path)
        
        # Force a new version by writing the same data back
        snapshot_path = f"{table_path}_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        df.write.format("delta").mode("overwrite").save(snapshot_path)
        
        print(f"\n=== SNAPSHOT CREATED ===")
        print(f"Source: {table_path}")
        print(f"Snapshot: {snapshot_path}")
        print(f"Description: {description}")
        print(f"Records: {df.count():,}")
        
        return snapshot_path
        
    except Exception as e:
        print(f"Error creating snapshot for {table_name}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Delta Lake Time Travel Manager")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # History command
    history_parser = subparsers.add_parser("history", help="Show table version history")
    history_parser.add_argument("--table", required=True, choices=["bronze", "silver", "gold"])
    history_parser.add_argument("--limit", type=int, default=20, help="Number of versions to show")
    
    # Read command
    read_parser = subparsers.add_parser("read", help="Read table at specific version/timestamp")
    read_parser.add_argument("--table", required=True, choices=["bronze", "silver", "gold"])
    read_parser.add_argument("--version", type=int, help="Version number")
    read_parser.add_argument("--timestamp", help="Timestamp (YYYY-MM-DD HH:MM:SS)")
    read_parser.add_argument("--limit", type=int, default=10, help="Number of records to show")
    read_parser.add_argument("--schema", action="store_true", help="Show schema")
    
    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare two table versions")
    compare_parser.add_argument("--table", required=True, choices=["bronze", "silver", "gold"])
    compare_parser.add_argument("--version1", type=int, required=True)
    compare_parser.add_argument("--version2", type=int, required=True)
    
    # Vacuum command
    vacuum_parser = subparsers.add_parser("vacuum", help="Vacuum old table versions")
    vacuum_parser.add_argument("--table", required=True, choices=["bronze", "silver", "gold"])
    vacuum_parser.add_argument("--retention-hours", type=int, help="Retention period in hours")
    vacuum_parser.add_argument("--dry-run", action="store_true", default=True, help="Show what would be deleted")
    vacuum_parser.add_argument("--execute", action="store_true", help="Actually perform vacuum")

    # Snapshot command
    snapshot_parser = subparsers.add_parser("snapshot", help="Create table snapshot")
    snapshot_parser.add_argument("--table", required=True, choices=["bronze", "silver", "gold"])
    snapshot_parser.add_argument("--description", default="Manual snapshot", help="Snapshot description")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Create Spark session
    print("Initializing Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.command == "history":
            show_history(spark, args.table, args.limit)

        elif args.command == "read":
            read_at_version(spark, args.table, args.version,
                          args.timestamp, args.limit, args.schema)

        elif args.command == "compare":
            compare_versions(spark, args.table, args.version1, args.version2)

        elif args.command == "vacuum":
            dry_run = not args.execute
            vacuum_table(spark, args.table, args.retention_hours, dry_run)

        elif args.command == "snapshot":
            create_snapshot(spark, args.table, args.description)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
