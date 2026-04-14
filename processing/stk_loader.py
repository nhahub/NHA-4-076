import pandas as pd
import os

# =========================
# BASE PATH SETUP
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")

ACCESS_FILE = os.path.join(DATA_DIR, "stk_access.csv")
AER_FILE = os.path.join(DATA_DIR, "stk_aer.csv")


# =========================
# 1. LOAD ACCESS DATA
# =========================
def load_access(file_path=ACCESS_FILE):
    df = pd.read_csv(file_path)

    df.columns = [c.strip() for c in df.columns]

    df["start"] = pd.to_datetime(df["Start Time (UTCG)"])
    df["end"] = pd.to_datetime(df["Stop Time (UTCG)"])

    return df[["start", "end", "Duration (sec)"]]


# =========================
# 2. LOAD AER DATA
# =========================
def load_aer(file_path=AER_FILE):
    df = pd.read_csv(file_path)

    df.columns = [c.strip() for c in df.columns]

    df["time"] = pd.to_datetime(df["Time (UTCG)"])

    df = df.rename(columns={
    "Elevation (deg)": "elevation",
    "Azimuth (deg)": "azimuth",
    "Range (km)": "range"
    })

    keep_cols = ["time", "elevation", "azimuth", "range"]
    df = df[[c for c in keep_cols if c in df.columns]]

    return df


# =========================
# 3. CONVERT ACCESS → WINDOWS
# =========================
def build_access_windows(access_df):
    return list(zip(access_df["start"], access_df["end"]))


# =========================
# 4. VISIBILITY CHECK
# =========================
def is_visible(timestamp, windows):
    return any(start <= timestamp <= end for start, end in windows)


# =========================
# 5. TEST RUN
# =========================
if __name__ == "__main__":
    access_df = load_access()
    aer_df = load_aer()

    windows = build_access_windows(access_df)

    print(f"Access windows loaded: {len(windows)}")

    print("\nACCESS SAMPLE:")
    print(access_df.head())

    print("\nAER SAMPLE:")
    print(aer_df.head())

