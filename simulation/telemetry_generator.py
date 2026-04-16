import pandas as pd
import numpy as np
from processing.merge_engine import build_dataset
from analytics.pass_detector import detect_passes


# =========================
# CONFIG
# =========================
NOISE_STD = 0.015
FADE_PROB = 0.08        # atmospheric fading chance
DROP_PROB = 0.04        # missing packets
MAX_RATE = 100


# =========================
# PHYSICAL-LIKE SIGNAL MODEL
# =========================
def compute_signal(elevation, range_km):

    if elevation <= 0:
        return 0.0

    # base geometric loss
    base = (elevation / 90) * (1 / (range_km + 1))

    # atmospheric attenuation (worse near horizon)
    atmosphere = np.exp(-3 * (1 - elevation / 90))

    return base * atmosphere


# =========================
# REALISTIC NOISE
# =========================
def add_noise(signal):
    noise = np.random.normal(0, NOISE_STD)

    # occasional deep fade
    if np.random.rand() < FADE_PROB:
        signal *= np.random.uniform(0.2, 0.7)

    return max(signal + noise, 0)


# =========================
# PACKET LOSS MODEL (REALISTIC)
# =========================
def packet_loss(signal):

    if signal < 0.002:
        return np.random.uniform(0.6, 1.0)  # near blackout

    if signal < 0.01:
        return np.random.uniform(0.1, 0.6)

    # normal operation
    return min(0.05 / (signal + 1e-6), 0.2)


# =========================
# DATA RATE MODEL
# =========================
def data_rate(signal, loss):
    effective = signal * (1 - loss)
    return min(effective * MAX_RATE * 120, MAX_RATE)


# =========================
# TELEMETRY PER PASS
# =========================
def generate_pass_telemetry(pass_obj):

    df = pass_obj.data.copy()

    # ---- TIME JITTER (real ground latency)
    df["time"] = pd.to_datetime(df["time"]) + pd.to_timedelta(
        np.random.normal(0, 2.5, len(df)), unit="s"
    )

    # ---- SIGNAL
    df["signal_raw"] = df.apply(
        lambda r: compute_signal(r["elevation"], r["range"]),
        axis=1
    )

    df["signal"] = df["signal_raw"].apply(add_noise)

    # ---- PACKET LOSS
    df["packet_loss"] = df["signal"].apply(packet_loss)

    # ---- DATA RATE
    df["data_rate"] = df.apply(
        lambda r: data_rate(r["signal"], r["packet_loss"]),
        axis=1
    )

    # ---- RANDOM DROPS (true missing telemetry)
    mask = np.random.rand(len(df)) > DROP_PROB
    df = df[mask].reset_index(drop=True)

    # ---- METADATA
    df["aos"] = pass_obj.aos
    df["los"] = pass_obj.los
    df["max_elevation"] = pass_obj.max_elevation

    return df


# =========================
# FULL PIPELINE
# =========================
def generate_telemetry(passes):

    all_data = []

    for i, p in enumerate(passes):
        df = generate_pass_telemetry(p)
        df["pass_id"] = i + 1
        all_data.append(df)

    return pd.concat(all_data).reset_index(drop=True) if all_data else pd.DataFrame()


# =========================
# RUN
# =========================
if __name__ == "__main__":

    df = build_dataset()
    passes = detect_passes(df)

    telemetry = generate_telemetry(passes)

    telemetry.to_csv("data/processed/telemetry.csv", index=False)

    print("\nREALISTIC TELEMETRY SAMPLE:")
    print(telemetry.head())

    print("\nDONE → saved telemetry.csv")