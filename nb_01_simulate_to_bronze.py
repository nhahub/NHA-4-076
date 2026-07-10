# Databricks notebook source
MODE = "simulate"       # "simulate" | "csv"

# AUTOMATION ENGINE OVERRIDE: Check if we are on Free Tier to avoid DBFS blocks
import os
is_free_tier = ('DATABRICKS_RUNTIME_VERSION' in os.environ and not os.path.exists('/dbfs/'))

# Safe local path for Free tier; original path for Azure Prod
BRONZE_PATH  = "file:/tmp/delta/satellite/bronze_telemetry" if is_free_tier else "dbfs:/delta/satellite/bronze_telemetry"
BRONZE_TABLE = "bronze_telemetry"

# Simulation settings (used when MODE = "simulate")
TLE_LINE1 = "1 25544U 98067A   24104.54791667  .00006993  00000-0  12837-3 0  9991"
TLE_LINE2 = "2 25544  51.6404 117.5923 0002764 310.1360 226.8150 15.50067898447416"
SAT_NAME  = "ISS (ZARYA)"

GS_LAT    = 30.0444
GS_LON    = 31.2357
GS_ELEV_M = 75.0

SIM_START          = "2026-06-28T00:00:00Z"
SIM_DURATION_HOURS = 24
STEP_SECONDS       = 10
MIN_ELEVATION_DEG  = 5.0

SIGNAL_NOISE_STD  = 0.03
JITTER_MAX_SEC    = 2
MISSING_DATA_PROB = 0.02
PACKET_LOSS_BASE  = 0.05

CSV_PATH = "file:/tmp/telemetry.csv" if is_free_tier else "dbfs:/FileStore/telemetry.csv"

print(f"Mode         : {MODE}")
print(f"Bronze path  : {BRONZE_PATH}")

# COMMAND ----------

# MAGIC  %pip install skyfield --quiet

# COMMAND ----------

import random
import numpy as np
from datetime import datetime, timezone, timedelta
from skyfield.api import EarthSatellite, wgs84, load

MAX_RANGE_KM  = 2500.0
MAX_DATA_RATE = 100.0
EL_WEIGHT     = 0.6
RANGE_WEIGHT  = 0.4

random.seed(42)
np.random.seed(42)

# ── Orbital calculator ─────────────────────────────────────────────
def compute_orbital_data(tle1, tle2, sat_name,
                         gs_lat, gs_lon, gs_elev_m,
                         start_iso, duration_hours, step_sec, min_el):
    ts = load.timescale()
    satellite = EarthSatellite(tle1, tle2, sat_name, ts)
    ground_station = wgs84.latlon(gs_lat, gs_lon, elevation_m=gs_elev_m)

    start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    total_steps = int(duration_hours * 3600 / step_sec)
    datetimes = [start + timedelta(seconds=i * step_sec) for i in range(total_steps)]

    t = ts.from_datetimes(datetimes)
    topo = (satellite - ground_station).at(t)
    alt, az, dist = topo.altaz()

    results = []
    for i, dt in enumerate(datetimes):
        el = float(alt.degrees[i])
        results.append({
            "time": dt.replace(tzinfo=timezone.utc),
            "elevation": round(el, 4),
            "azimuth": round(float(az.degrees[i]) % 360, 4),
            "range_km": round(float(dist.km[i]), 4),
            "visible": el >= min_el,
        })
    return results

# ── Pass detector ──────────────────────────────────────────────────
def detect_passes(orbital_data):
    passes, in_pass, current = [], False, {}
    for row in orbital_data:
        if row["visible"] and not in_pass:
            in_pass = True
            current = {"aos": row["time"], "los": None,
                       "max_elevation": row["elevation"], "samples": [row]}
        elif row["visible"] and in_pass:
            current["samples"].append(row)
            if row["elevation"] > current["max_elevation"]:
                current["max_elevation"] = row["elevation"]
        elif not row["visible"] and in_pass:
            in_pass = False
            current["los"] = current["samples"][-1]["time"]
            passes.append(current)
            current = {}
    if in_pass and current.get("samples"):
        current["los"] = current["samples"][-1]["time"]
        passes.append(current)

    time_to_meta = {}
    for idx, p in enumerate(passes):
        pid = f"PASS_{idx+1:03d}"
        dur = (p["los"] - p["aos"]).total_seconds()
        for s in p["samples"]:
            time_to_meta[s["time"]] = {
                "pass_id": pid,
                "aos": p["aos"].isoformat(),
                "los": p["los"].isoformat(),
                "max_elevation_pass": round(p["max_elevation"], 4),
                "pass_duration_seconds": round(dur, 1),
            }

    annotated = []
    for row in orbital_data:
        meta = time_to_meta.get(row["time"])
        annotated.append({**row,
            "pass_id": meta["pass_id"] if meta else None,
            "aos": meta["aos"] if meta else None,
            "los": meta["los"] if meta else None,
            "max_elevation_pass": meta["max_elevation_pass"] if meta else None,
            "pass_duration_seconds": meta["pass_duration_seconds"] if meta else None,
        })
    return annotated

# ── Telemetry generator ────────────────────────────────────────────
def _signal_quality(elevation_deg, range_km):
    el_rad = np.radians(max(elevation_deg, 0.0))
    el_comp = np.sin(el_rad)
    range_comp = max(0.0, 1.0 - (range_km / MAX_RANGE_KM))
    return float(np.clip(EL_WEIGHT * el_comp + RANGE_WEIGHT * range_comp, 0.0, 1.0))

def generate_telemetry(annotated_data, noise_std, jitter_max, missing_prob, pkt_loss_base):
    records = []
    for row in annotated_data:
        if row["pass_id"] is None:
            continue
        if random.random() < missing_prob:
            continue
        signal_raw = _signal_quality(row["elevation"], row["range_km"])
        noise      = np.random.normal(0, noise_std)
        signal     = float(np.clip(signal_raw + noise, 0.0, 1.0))
        pkt_loss   = float(np.clip(pkt_loss_base + (1 - signal)**2 * (1 - pkt_loss_base), 0.0, 1.0))
        data_rate  = round(signal * MAX_DATA_RATE, 2)
        jitter     = timedelta(seconds=random.uniform(-jitter_max, jitter_max))
        ts         = (row["time"] + jitter).isoformat()
        records.append({
            "time": ts,
            "elevation": row["elevation"],
            "azimuth": row["azimuth"],
            "range_km": row["range_km"],
            "signal": round(signal, 4),
            "signal_raw": round(signal_raw, 4),
            "packet_loss": round(pkt_loss, 4),
            "data_rate_mbps": data_rate,
            "pass_id": row["pass_id"],
            "aos": row["aos"],
            "los": row["los"],
            "max_elevation_pass": row["max_elevation_pass"],
            "pass_duration_seconds": row["pass_duration_seconds"],
        })
    return records

print("✅ Simulation helpers loaded")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd

BRONZE_SCHEMA = StructType([
    StructField("time",                  StringType(),  True),
    StructField("elevation",             DoubleType(),  True),
    StructField("azimuth",               DoubleType(),  True),
    StructField("range_km",              DoubleType(),  True),
    StructField("signal",                DoubleType(),  True),
    StructField("signal_raw",            DoubleType(),  True),
    StructField("packet_loss",           DoubleType(),  True),
    StructField("data_rate_mbps",        DoubleType(),  True),
    StructField("pass_id",               StringType(),  True),
    StructField("aos",                   StringType(),  True),
    StructField("los",                   StringType(),  True),
    StructField("max_elevation_pass",    DoubleType(),  True),
    StructField("pass_duration_seconds", DoubleType(),  True),
])

if MODE == "simulate":
    print("🛰️  Running Skyfield simulation...")
    orbital   = compute_orbital_data(TLE_LINE1, TLE_LINE2, SAT_NAME,
                                     GS_LAT, GS_LON, GS_ELEV_M,
                                     SIM_START, SIM_DURATION_HOURS,
                                     STEP_SECONDS, MIN_ELEVATION_DEG)
    annotated = detect_passes(orbital)
    records   = generate_telemetry(annotated, SIGNAL_NOISE_STD,
                                   JITTER_MAX_SEC, MISSING_DATA_PROB,
                                   PACKET_LOSS_BASE)
    pdf = pd.DataFrame(records)
    print(f"✅ Simulation produced {len(pdf)} records")

elif MODE == "csv":
    print(f"📂 Loading CSV from {CSV_PATH} ...")
    pdf = spark.read \
              .option("header", True) \
              .schema(BRONZE_SCHEMA) \
              .csv(CSV_PATH) \
              .toPandas()
    print(f"✅ Loaded {len(pdf)} records from CSV")

else:
    raise ValueError(f"Unknown MODE '{MODE}'. Use 'simulate' or 'csv'.")

display(pdf.head(5))

# COMMAND ----------

# COMMAND ----------
# MAGIC %md ## Cell 5 — Write to Bronze Delta table

df_spark = spark.createDataFrame(pdf, schema=BRONZE_SCHEMA) \
                .withColumn("ingested_at", F.current_timestamp()) \
                .withColumn("source_mode", F.lit(MODE))

# AUTOMATION ENGINE OVERRIDE: Bypass DBFS storage path entirely by writing straight to the default catalog
import os
is_free_tier = ('DATABRICKS_RUNTIME_VERSION' in os.environ and not os.path.exists('/dbfs/'))
TARGET_TABLE = f"hive_metastore.default.{BRONZE_TABLE}" if is_free_tier else BRONZE_TABLE

# Save directly as a managed table (Databricks automatically handles storage safely)
df_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(TARGET_TABLE)

count = spark.sql(f"SELECT COUNT(*) as n FROM {TARGET_TABLE}").collect()[0]["n"]
print(f"✅ Bronze table '{TARGET_TABLE}' now has {count} total rows")