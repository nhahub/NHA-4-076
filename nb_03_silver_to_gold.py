# Databricks notebook source
SILVER_TABLE = "silver_telemetry"

GOLD_SIGNAL_KPI     = "gold_signal_kpi"
GOLD_PASS_ANALYTICS = "gold_pass_analytics"
GOLD_ALERTS         = "gold_alerts"
GOLD_TIMESERIES     = "gold_timeseries"

# AUTOMATION ENGINE OVERRIDE: Check environment for safe sandbox table references
import os
is_free_tier = ('DATABRICKS_RUNTIME_VERSION' in os.environ and not os.path.exists('/dbfs/'))

if is_free_tier:
    # You: Local sandbox mappings
    SRC_SILVER_TABLE    = f"hive_metastore.default.{SILVER_TABLE}"
    GOLD_SIGNAL_KPI     = f"hive_metastore.default.{GOLD_SIGNAL_KPI}"
    GOLD_PASS_ANALYTICS = f"hive_metastore.default.{GOLD_PASS_ANALYTICS}"
    GOLD_ALERTS         = f"hive_metastore.default.{GOLD_ALERTS}"
    GOLD_TIMESERIES     = f"hive_metastore.default.{GOLD_TIMESERIES}"
else:
    # Them: Azure Production
    SRC_SILVER_TABLE    = SILVER_TABLE

# Leave paths untouched so their code signatures don't break for Azure
GOLD_KPI_PATH       = "dbfs:/delta/satellite/gold_signal_kpi"
GOLD_PASS_PATH      = "dbfs:/delta/satellite/gold_pass_analytics"
GOLD_ALERTS_PATH    = "dbfs:/delta/satellite/gold_alerts"
GOLD_TS_PATH        = "dbfs:/delta/satellite/gold_timeseries"

ALERT_PACKET_LOSS_THRESHOLD = 0.5
ALERT_SIGNAL_THRESHOLD      = 0.3
ALERT_OUTAGE_GAP_SECONDS    = 60

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

silver = spark.sql(f"SELECT * FROM {SRC_SILVER_TABLE}")
print(f"Silver rows loaded: {silver.count()}")

# COMMAND ----------

def write_gold(df, path, table_name):
    # Save directly as a managed Unity Catalog table
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(table_name)
    print(f"✅ {table_name} written")

# Calculate overall metrics from your silver telemetry layer
signal_kpi = silver.agg(
    F.count("*")                        .alias("total_samples"),
    F.round(F.avg("signal"), 4)          .alias("avg_signal"),
    F.round(F.min("signal"), 4)          .alias("min_signal"),
    F.round(F.max("signal"), 4)          .alias("max_signal"),
    F.round(F.stddev("signal"), 4)        .alias("stddev_signal"),
    F.round(F.avg("packet_loss"), 4)     .alias("avg_packet_loss"),
    F.round(F.max("packet_loss"), 4)     .alias("max_packet_loss"),
    F.round(F.avg("data_rate_mbps"), 4)  .alias("avg_data_rate_mbps"),
    F.round(F.max("data_rate_mbps"), 4)  .alias("max_data_rate_mbps"),
    F.countDistinct("pass_id")          .alias("total_passes"),
    F.min("time")                       .alias("first_telemetry_time"),
    F.max("time")                       .alias("last_telemetry_time"),
).withColumn("computed_at", F.current_timestamp())

# Execute the updated helper function
write_gold(signal_kpi, GOLD_KPI_PATH, GOLD_SIGNAL_KPI)
display(signal_kpi)

# COMMAND ----------

pass_analytics = silver \
    .groupBy("pass_id", "aos", "los", "max_elevation_pass", "pass_duration_seconds") \
    .agg(
        F.count("*")                          .alias("sample_count"),
        F.round(F.avg("signal"), 4)           .alias("avg_signal"),
        F.round(F.min("signal"), 4)           .alias("min_signal"),
        F.round(F.max("signal"), 4)           .alias("max_signal"),
        F.round(F.avg("packet_loss"), 4)      .alias("avg_packet_loss"),
        F.round(F.max("packet_loss"), 4)      .alias("max_packet_loss"),
        F.round(F.avg("data_rate_mbps"), 4)   .alias("avg_data_rate_mbps"),
        F.round(F.avg("elevation"), 4)        .alias("avg_elevation"),
        F.round(F.min("range_km"), 4)         .alias("min_range_km"),
        F.round(F.avg("range_km"), 4)         .alias("avg_range_km"),
    ) \
    .withColumn("pass_duration_minutes",
        F.round(F.col("pass_duration_seconds") / 60, 2)) \
    .withColumn("computed_at", F.current_timestamp()) \
    .orderBy("pass_id")

write_gold(pass_analytics, GOLD_PASS_PATH, GOLD_PASS_ANALYTICS)
display(pass_analytics)

# COMMAND ----------

alerts_base = pass_analytics \
    .withColumn("alert_high_packet_loss",
        F.col("avg_packet_loss") > F.lit(ALERT_PACKET_LOSS_THRESHOLD)) \
    .withColumn("alert_low_signal",
        F.col("avg_signal") < F.lit(ALERT_SIGNAL_THRESHOLD)) \
    .withColumn("alert_severity",
        F.when(
            F.col("alert_high_packet_loss") & F.col("alert_low_signal"),
            F.lit("CRITICAL")
        ).when(
            F.col("alert_high_packet_loss") | F.col("alert_low_signal"),
            F.lit("WARNING")
        ).otherwise(F.lit("NOMINAL"))
    )

# Detect in-pass outages: gaps between consecutive samples > threshold
w = Window.partitionBy("pass_id").orderBy("time")

gap_df = silver \
    .withColumn("prev_time", F.lag("time").over(w)) \
    .withColumn("gap_seconds",
        F.unix_timestamp("time") - F.unix_timestamp("prev_time")) \
    .filter(F.col("gap_seconds") > ALERT_OUTAGE_GAP_SECONDS) \
    .groupBy("pass_id") \
    .agg(
        F.count("*")                     .alias("outage_count"),
        F.round(F.max("gap_seconds"), 1) .alias("max_gap_seconds")
    )

alerts = alerts_base \
    .join(gap_df, on="pass_id", how="left") \
    .fillna({"outage_count": 0, "max_gap_seconds": 0.0}) \
    .withColumn("alert_outage", F.col("outage_count") > 0) \
    .select(
        "pass_id", "aos", "los",
        "avg_signal", "avg_packet_loss",
        "alert_high_packet_loss", "alert_low_signal", "alert_outage",
        "alert_severity", "outage_count", "max_gap_seconds",
        "computed_at"
    ) \
    .orderBy("pass_id")

write_gold(alerts, GOLD_ALERTS_PATH, GOLD_ALERTS)
display(alerts)

critical = alerts.filter(F.col("alert_severity") == "CRITICAL").count()
warning  = alerts.filter(F.col("alert_severity") == "WARNING").count()
nominal  = alerts.filter(F.col("alert_severity") == "NOMINAL").count()
print(f"\n  🔴 CRITICAL : {critical}")
print(f"  🟡 WARNING  : {warning}")
print(f"  🟢 NOMINAL  : {nominal}")

# COMMAND ----------

timeseries = silver \
    .withColumn("minute_bucket", F.date_trunc("minute", F.col("time"))) \
    .groupBy("minute_bucket", "pass_id") \
    .agg(
        F.round(F.avg("signal"), 4)         .alias("avg_signal"),
        F.round(F.avg("packet_loss"), 4)    .alias("avg_packet_loss"),
        F.round(F.avg("data_rate_mbps"), 4) .alias("avg_data_rate_mbps"),
        F.round(F.avg("elevation"), 4)      .alias("avg_elevation"),
        F.count("*")                        .alias("sample_count"),
    ) \
    .withColumn("computed_at", F.current_timestamp()) \
    .orderBy("minute_bucket")

write_gold(timeseries, GOLD_TS_PATH, GOLD_TIMESERIES)
display(timeseries.limit(10))

# COMMAND ----------

## Summary

print("✅ Silver → Gold complete")
print(f"   {GOLD_SIGNAL_KPI}     → overall KPIs (1 row)")
print(f"   {GOLD_PASS_ANALYTICS} → 1 row per pass")
print(f"   {GOLD_ALERTS}         → alert flags per pass")
print(f"   {GOLD_TIMESERIES}     → 1-min buckets for trend charts")
print()
print("Next step: connect Power BI Desktop via Databricks JDBC/ODBC connector")