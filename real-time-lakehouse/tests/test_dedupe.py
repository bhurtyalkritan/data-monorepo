# Placeholder: implement a small local Spark test verifying dedupe by event_id
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def test_dedupe_by_event_id():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    data = [
        ("e1","purchase",1.0,1,"2025-01-01T00:00:00Z"),
        ("e1","purchase",1.0,1,"2025-01-01T00:00:10Z"),
    ]
    df = spark.createDataFrame(data, ["event_id","event_type","price","quantity","ts"])
    deduped = df.dropDuplicates(["event_id"])
    assert deduped.count() == 1
    spark.stop()
