from __future__ import annotations
import numpy as np
import pandas as pd


def add_flow_metrics(df):
    df = df.copy()
    df["Obs_Flow"] = pd.to_numeric(df["Obs_Flow"], errors="coerce")
    df["Modelled_Flow"] = pd.to_numeric(df["Modelled_Flow"], errors="coerce")
    df["Diff"] = df["Modelled_Flow"] - df["Obs_Flow"]
    df["%Diff"] = np.where((df["Obs_Flow"].notna()) & (df["Obs_Flow"] != 0), 100.0 * df["Diff"] / df["Obs_Flow"], np.nan)
    denom = df["Modelled_Flow"] + df["Obs_Flow"]
    df["GEH"] = np.where((df["Obs_Flow"].notna()) & (df["Modelled_Flow"].notna()) & (denom > 0),
                         np.sqrt(2.0 * (df["Diff"] ** 2) / denom), np.nan)
    return df
