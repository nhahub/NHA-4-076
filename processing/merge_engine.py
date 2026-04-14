import pandas as pd
from stk_loader import load_access, load_aer, build_access_windows


# =========================
# SIGNAL MODEL (BASE)
# =========================
def compute_signal(elevation, range_km):
    if elevation <= 0:
        return 0.0
    return (elevation / 90) * (1 / (range_km + 1))


# =========================
# BUILD DATASET
# =========================
def build_dataset(threshold=5.0):
    access_df = load_access()
    aer_df = load_aer()

    windows = build_access_windows(access_df)

    def in_window(t):
        return any(start <= t <= end for start, end in windows)

    def compute_visibility(row):
        return int(in_window(row["time"]) and row["elevation"] >= threshold)

    aer_df["visibility"] = aer_df.apply(compute_visibility, axis=1)

    aer_df["signal"] = aer_df.apply(
        lambda r: compute_signal(r["elevation"], r["range"]),
        axis=1
    )

    return aer_df


# =========================
# MAIN TEST
# =========================
if __name__ == "__main__":
    df = build_dataset()

    df.to_csv("data/processed/merged_data.csv", index=False)

    print("\nMERGED DATA SAMPLE:")
    print(df.head(10))

    print("\nVisibility counts:")
    print(df["visibility"].value_counts())