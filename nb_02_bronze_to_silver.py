# Databricks notebook source
BRONZE_TABLE   = "bronze_telemetry"
SILVER_TABLE   = "silver_telemetry"
REJECTED_TABLE = "silver_rejected"

# AUTOMATION ENGINE OVERRIDE: Dynamically set catalog table targets for Free Tier vs Azure Prod
import os
is_free_tier = ('DATABRICKS_RUNTIME_VERSION' in os.environ and not os.path.exists('/dbfs/'))

if is_free_tier:
    # You: Free Tier local catalog mappings
    SRC_BRONZE_TABLE   = f"hive_metastore.default.{BRONZE_TABLE}"
    TGT_SILVER_TABLE   = f"hive_metastore.default.{SILVER_TABLE}"
    TGT_REJECTED_TABLE = f"hive_metastore.default.{REJECTED_TABLE}"
else:
    # Them: Production Azure configurations
    SRC_BRONZE_TABLE   = BRONZE_TABLE
    TGT_SILVER_TABLE   = SILVER_TABLE
    TGT_REJECTED_TABLE = REJECTED_TABLE

print(f"Reading from Bronze Table : {SRC_BRONZE_TABLE}")
print(f"Writing to Silver Table   : {TGT_SILVER_TABLE}")
print(f"Writing to Rejected Table : {TGT_REJECTED_TABLE}")

# COMMAND ----------

# COMMAND ----------
# MAGIC %md ## Cell 2 — Load Bronze

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

# Updated to use the dynamic source table name
bronze = spark.sql(f"SELECT * FROM {SRC_BRONZE_TABLE}")
print(f"Bronze rows loaded: {bronze.count()}")
bronze.printSchema()

# COMMAND ----------

typed = bronze \
    .withColumn("time",          F.to_timestamp("time")) \
    .withColumn("elevation",     F.col("elevation").cast(DoubleType())) \
    .withColumn("azimuth",       F.col("azimuth").cast(DoubleType())) \
    .withColumn("range_km",      F.col("range_km").cast(DoubleType())) \
    .withColumn("signal",        F.col("signal").cast(DoubleType())) \
    .withColumn("signal_raw",    F.col("signal_raw").cast(DoubleType())) \
    .withColumn("packet_loss",   F.col("packet_loss").cast(DoubleType())) \
    .withColumn("data_rate_mbps",F.col("data_rate_mbps").cast(DoubleType()))

print("✅ Types cast")

# COMMAND ----------

validated = typed \
    .withColumn("_v_time_not_null",
        F.col("time").isNotNull()) \
    .withColumn("_v_elevation_not_null",
        F.col("elevation").isNotNull()) \
    .withColumn("_v_range_not_null",
        F.col("range_km").isNotNull()) \
    .withColumn("_v_elevation_range",
        (F.col("elevation") >= 0) & (F.col("elevation") <= 90)) \
    .withColumn("_v_azimuth_range",
        F.col("azimuth").isNull() |
        ((F.col("azimuth") >= 0) & (F.col("azimuth") <= 360))) \
    .withColumn("_v_range_positive",
        F.col("range_km") > 0) \
    .withColumn("_v_signal_range",
        F.col("signal").isNull() |
        ((F.col("signal") >= 0) & (F.col("signal") <= 1))) \
    .withColumn("_v_packet_loss_range",
        F.col("packet_loss").isNull() |
        ((F.col("packet_loss") >= 0) & (F.col("packet_loss") <= 1))) \
    .withColumn("_v_pass_id_not_null",
        F.col("pass_id").isNotNull())

validation_cols = [c for c in validated.columns if c.startswith("_v_")]

is_valid_expr = F.lit(True)
for col in validation_cols:
    is_valid_expr = is_valid_expr & F.col(col)

validated = validated.withColumn("_is_valid", is_valid_expr)

total     = validated.count()
valid_n   = validated.filter(F.col("_is_valid")).count()
invalid_n = total - valid_n
print(f"Total rows   : {total}")
print(f"Valid rows   : {valid_n}")
print(f"Invalid rows : {invalid_n}")

# COMMAND ----------

valid_cols = [c for c in validated.columns
              if not c.startswith("_v_") and c != "_is_valid"]

silver_df = validated \
    .filter(F.col("_is_valid")) \
    .select(valid_cols)

# Build rejection reason string
reason_expr = F.lit("")
for col in validation_cols:
    label = col.replace("_v_", "")
    reason_expr = F.when(
        ~F.col(col),
        F.concat(reason_expr, F.lit(f"[FAIL:{label}] "))
    ).otherwise(reason_expr)

rejected_df = validated \
    .filter(~F.col("_is_valid")) \
    .withColumn("rejection_reason", reason_expr) \
    .withColumn("rejected_at", F.current_timestamp()) \
    .select(valid_cols + ["rejection_reason", "rejected_at"])

print(f"Routing {silver_df.count()} rows to Silver")
print(f"Routing {rejected_df.count()} rows to Rejected")

# COMMAND ----------

before = silver_df.count()
silver_df = silver_df.dropDuplicates(["time", "pass_id"])
after = silver_df.count()
print(f"Deduplication removed {before - after} duplicate rows")

# COMMAND ----------

silver_df = silver_df \
    .withColumn("validated_at", F.current_timestamp()) \
    .withColumn("silver_version", F.lit("1.0"))

silver_df.printSchema()

# COMMAND ----------

# COMMAND ----------
# MAGIC %md ## Cell 8 — Write Silver and Rejected tables

# Silver — overwrite each run (idempotent after dedup)
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(TGT_SILVER_TABLE)

# Rejected — append to keep full audit trail
if rejected_df.count() > 0:
    rejected_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TGT_REJECTED_TABLE)

silver_count = spark.sql(f"SELECT COUNT(*) as n FROM {TGT_SILVER_TABLE}").collect()[0]["n"]
print(f"✅ Silver table '{TGT_SILVER_TABLE}' has {silver_count} rows")
print(f"✅ Bronze → Silver complete")