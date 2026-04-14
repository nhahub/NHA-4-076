import pandas as pd


class Pass:
    def __init__(self, aos, los, tca, max_el, df_segment):
        self.aos = aos
        self.los = los
        self.tca = tca
        self.max_elevation = max_el
        self.duration = (los - aos).total_seconds()
        self.data = df_segment


# =========================
# PASS DETECTION ENGINE
# =========================
def detect_passes(df):
    df = df.copy().sort_values("time")

    passes = []
    current = []

    for _, row in df.iterrows():

        if row["visibility"] == 1:
            current.append(row)
        else:
            if current:
                passes.append(_build_pass(pd.DataFrame(current)))
                current = []

    if current:
        passes.append(_build_pass(pd.DataFrame(current)))

    return passes


def _build_pass(df_pass):
    aos = df_pass.iloc[0]["time"]
    los = df_pass.iloc[-1]["time"]

    max_row = df_pass.loc[df_pass["elevation"].idxmax()]
    tca = max_row["time"]
    max_el = df_pass["elevation"].max()

    return Pass(aos, los, tca, max_el, df_pass)