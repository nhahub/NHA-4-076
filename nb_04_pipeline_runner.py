# Databricks notebook source
import datetime

run_id  = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
results = {}

print("=" * 55)
print("  Satellite Mission — Full Pipeline Run")
print("=" * 55)
print(f"  Run ID : {run_id}")
print(f"  Start  : {datetime.datetime.utcnow().isoformat()}Z")
print()

# COMMAND ----------

print("▶  Step 1/3 — Simulate → Bronze")
try:
    dbutils.notebook.run("nb_01_simulate_to_bronze", timeout_seconds=600)
    results["bronze"] = "✅ SUCCESS"
except Exception as e:
    results["bronze"] = f"❌ FAILED: {e}"
    print(results["bronze"])
    raise   # abort if ingestion fails
print(results["bronze"])

# COMMAND ----------

print("▶  Step 2/3 — Bronze → Silver")
try:
    dbutils.notebook.run("nb_02_bronze_to_silver", timeout_seconds=600)
    results["silver"] = "✅ SUCCESS"
except Exception as e:
    results["silver"] = f"❌ FAILED: {e}"
    print(results["silver"])
    raise
print(results["silver"])

# COMMAND ----------

print("▶  Step 3/3 — Silver → Gold")
try:
    dbutils.notebook.run("nb_03_silver_to_gold", timeout_seconds=600)
    results["gold"] = "✅ SUCCESS"
except Exception as e:
    results["gold"] = f"❌ FAILED: {e}"
    print(results["gold"])
    raise
print(results["gold"])

# COMMAND ----------

print()
print("=" * 55)
print("  Pipeline Run Summary")
print("=" * 55)
for step, status in results.items():
    print(f"  {step:<8} {status}")
print(f"\n  Completed : {datetime.datetime.utcnow().isoformat()}Z")
print("=" * 55)