# Gold: KPI aggregates for dashboard
from pyspark.sql import functions as F
from config_rt import *

gold_chkpt = f"{checkpoints}/gold_kpis"

events = spark.readStream.table(silver_tbl).withWatermark("ts", "15 minutes")

win = F.window("ts", "1 minute")

views = (events.filter(F.col("event_type") == "page_view")
         .groupBy(win)
         .agg(F.approx_count_distinct("user_id").alias("view_users")))

purchases = (events.filter(F.col("event_type") == "purchase")
             .withColumn("revenue", F.col("price") * F.col("quantity"))
             .groupBy(win)
             .agg(
                 F.count("*").alias("orders"),
                 F.sum("revenue").alias("gmv"),
                 F.approx_count_distinct("user_id").alias("purchase_users"))
             )

active = (events.groupBy(win)
          .agg(F.approx_count_distinct("user_id").alias("active_users")))

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
            (F.col("purchase_users") / F.when(F.col("view_users") == 0, F.lit(1)).otherwise(F.col("view_users"))).alias("conversion_rate")
        ))

(gold.writeStream
 .format("delta")
 .option("checkpointLocation", gold_chkpt)
 .outputMode("append")
 .table(gold_kpi_tbl))
