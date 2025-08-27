# Stretch: real-time anomaly alerts
from pyspark.sql import functions as F
from config_rt import *

alerts_chkpt = f"{checkpoints}/alerts"


def detect_anoms(microbatch_df, batch_id):
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    cur = microbatch_df.select("window_start", "window_end", "orders", "gmv", "conversion_rate")
    history = spark.table(gold_kpi_tbl).where(F.col("window_start") >= F.current_timestamp() - F.expr("INTERVAL 6 HOURS"))
    stats_row = (history.agg(
        F.avg("orders").alias("orders_mu"), F.stddev_pop("orders").alias("orders_sigma"),
        F.avg("gmv").alias("gmv_mu"), F.stddev_pop("gmv").alias("gmv_sigma"),
        F.avg("conversion_rate").alias("cr_mu"), F.stddev_pop("conversion_rate").alias("cr_sigma")
    ).collect())

    if not stats_row or stats_row[0][1] is None:
        return
    stats = stats_row[0].asDict()

    thresholds = {
        "orders_hi": stats["orders_mu"] + 3 * stats["orders_sigma"],
        "orders_lo": max(0.0, stats["orders_mu"] - 3 * stats["orders_sigma"]),
        "gmv_hi": stats["gmv_mu"] + 3 * stats["gmv_sigma"],
        "gmv_lo": max(0.0, stats["gmv_mu"] - 3 * stats["gmv_sigma"]),
        "cr_hi": stats["cr_mu"] + 3 * stats["cr_sigma"],
        "cr_lo": max(0.0, stats["cr_mu"] - 3 * stats["cr_sigma"])
    }

    flagged = (cur
               .withColumn("orders_anom", (F.col("orders") < F.lit(thresholds["orders_lo"])) | (F.col("orders") > F.lit(thresholds["orders_hi"])))
               .withColumn("gmv_anom", (F.col("gmv") < F.lit(thresholds["gmv_lo"])) | (F.col("gmv") > F.lit(thresholds["gmv_hi"])))
               .withColumn("cr_anom", (F.col("conversion_rate") < F.lit(thresholds["cr_lo"])) | (F.col("conversion_rate") > F.lit(thresholds["cr_hi"])))
               .withColumn("any_anom", F.col("orders_anom") | F.col("gmv_anom") | F.col("cr_anom"))
               .withColumn("generated_at", F.current_timestamp()))
    
    (flagged.filter("any_anom = true")
     .write.mode("append")
     .saveAsTable(alerts_tbl))


# Stream from gold KPIs
(gold_df := spark.readStream.table(gold_kpi_tbl))
(gold_df.writeStream
 .foreachBatch(detect_anoms)
 .option("checkpointLocation", alerts_chkpt)
 .outputMode("update")
 .start())
