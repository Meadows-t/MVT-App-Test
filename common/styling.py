from __future__ import annotations
import pandas as pd


def geh_row_style(row):
    try:
        val = row.get("GEH")
        if pd.notna(val) and float(val) > 5:
            return ["background-color: #f8d7da"] * len(row)
    except Exception:
        pass
    return [""] * len(row)
