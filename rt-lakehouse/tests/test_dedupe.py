# Placeholder: implement a small local Spark test verifying dedupe by event_id
import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
def test_dedupe_by_event_id():
    """Test deduplication by event_id using PySpark"""
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    data = [
        ("e1", "purchase", 1.0, 1, "2025-01-01T00:00:00Z"),
        ("e1", "purchase", 1.0, 1, "2025-01-01T00:00:10Z"),
    ]
    df = spark.createDataFrame(data, ["event_id", "event_type", "price", "quantity", "ts"])
    deduped = df.dropDuplicates(["event_id"])
    assert deduped.count() == 1
    spark.stop()


def test_dedupe_by_event_id_pandas():
    """Test deduplication using pandas as fallback"""
    import pandas as pd
    
    data = [
        ("e1", "purchase", 1.0, 1, "2025-01-01T00:00:00Z"),
        ("e1", "purchase", 1.0, 1, "2025-01-01T00:00:10Z"),
        ("e2", "view", 0.0, 1, "2025-01-01T00:00:20Z"),
    ]
    df = pd.DataFrame(data, columns=["event_id", "event_type", "price", "quantity", "ts"])
    deduped = df.drop_duplicates(subset=["event_id"])
    assert len(deduped) == 2
    assert "e1" in deduped["event_id"].values
    assert "e2" in deduped["event_id"].values
