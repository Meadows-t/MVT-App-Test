from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from common.time_utils import TIME_RE_15, add_clock
from common.metrics import add_flow_metrics
from att_tools import read_mov_att


def build_15min_bins(eval_start: str, eval_dur_min: int):
    if not TIME_RE_15.match(str(eval_start).strip()):
        raise ValueError("Flow EvalStart must be 15-min aligned (HH:00/15/30/45).")
    if int(eval_dur_min) % 15 != 0:
        raise ValueError("Flow EvalDuration must be multiple of 15.")
    sdt = datetime.strptime(str(eval_start).strip(), "%H:%M")
    n = int(eval_dur_min) // 15
    return [(sdt + timedelta(minutes=15 * i)).strftime("%H:%M") for i in range(n)]


def _parse_movement_str(mov: str):
    s = str(mov).strip()
    if not s:
        return None, None, None
    parts = s.split("-")
    if len(parts) < 3:
        return None, None, None
    node = parts[0].strip()
    fr = parts[1].split("@")[0].strip()
    to = parts[2].split("@")[0].strip()
    return node, fr, to


def load_flow_definition(xlsx: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx, sheet_name="Flow_Definition", engine="openpyxl")
    req = ["SiteName", "FlowType", "Node", "FromLink", "ToLink", "FromArm", "ToArm", "FlowName"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        raise KeyError("Flow_Definition missing columns: {}".format(miss))

    df = df.copy()
    df["SiteName"] = df["SiteName"].astype(str).str.strip()
    df["FlowType"] = df["FlowType"].astype(str).str.strip().str.upper()
    df["Node"] = df["Node"].astype(str).str.strip()
    df["FromLink"] = df["FromLink"].astype(str).str.strip()
    df["ToLink"] = df["ToLink"].astype(str).str.strip()
    df["FromArm"] = df["FromArm"].fillna("").astype(str).str.strip()
    df["ToArm"] = df["ToArm"].fillna("").astype(str).str.strip()
    df["FlowName"] = df["FlowName"].fillna("").astype(str).str.strip()

    if not df["FlowType"].isin(["TURN", "ENTRY", "EXIT"]).all():
        bad = df.loc[~df["FlowType"].isin(["TURN", "ENTRY", "EXIT"]), "FlowType"].unique().tolist()
        raise ValueError("Invalid FlowType values: {}".format(bad))

    bad_key = df[(df["Node"] == "") | (df["FromLink"] == "") | (df["ToLink"] == "")]
    if len(bad_key):
        raise ValueError("Flow_Definition requires Node+FromLink+ToLink for all rows (bad rows: {})".format(len(bad_key)))

    df["DefID"] = range(1, len(df) + 1)
    return df


def load_flow_observed_wide(xlsx: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx, sheet_name="Flow_Observed", engine="openpyxl")
    req = ["SiteName", "FromArm", "ToArm", "FlowType", "Node", "FromLink", "ToLink"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        raise KeyError("Flow_Observed missing columns: {}".format(miss))

    df = df.copy()
    df["SiteName"] = df["SiteName"].astype(str).str.strip()
    df["FlowType"] = df["FlowType"].astype(str).str.strip().str.upper()
    df["Node"] = df["Node"].astype(str).str.strip()
    df["FromLink"] = df["FromLink"].astype(str).str.strip()
    df["ToLink"] = df["ToLink"].astype(str).str.strip()
    for c in ["FromArm", "ToArm", "FlowName"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()
        else:
            df[c] = ""
    return df


def _obs_col(scenario: str, class_id: str) -> str:
    return "{}__Obs_{}".format(str(scenario).strip(), str(class_id).strip())


def _normalise_movement_df(df: pd.DataFrame) -> pd.DataFrame:
    cols_u = {str(c).strip().upper(): c for c in df.columns}
    run_col = cols_u.get("SIMRUN") or cols_u.get("RUN")
    int_col = cols_u.get("TIMEINT") or cols_u.get("TIMEINTERVAL")
    mov_col = cols_u.get("MOVEMENT")

    miss = [n for n, c in [("SIMRUN", run_col), ("TIMEINT", int_col), ("MOVEMENT", mov_col)] if c is None]
    if miss:
        raise KeyError("Movement ATT missing columns {}. Available: {}".format(miss, list(df.columns)))

    out = df.copy()
    out.rename(columns={run_col: "SimRun", int_col: "TimeInterval", mov_col: "MOVEMENT"}, inplace=True)

    out["SimRun"] = out["SimRun"].astype(str).str.strip()
    out = out[out["SimRun"].str.match(r"^\d+$")].copy()
    out["SimRun"] = out["SimRun"].astype(int)

    out["TimeInterval"] = out["TimeInterval"].astype(str).str.strip().str.replace("–", "-").str.replace("—", "-")
    out["MOVEMENT"] = out["MOVEMENT"].astype(str).str.strip()

    parsed = out["MOVEMENT"].map(_parse_movement_str)
    out["Node"] = parsed.map(lambda x: x[0])
    out["FromLink"] = parsed.map(lambda x: x[1])
    out["ToLink"] = parsed.map(lambda x: x[2])

    out["Node"] = out["Node"].fillna("").astype(str).str.strip()
    out["FromLink"] = out["FromLink"].fillna("").astype(str).str.strip()
    out["ToLink"] = out["ToLink"].fillna("").astype(str).str.strip()

    return out


def pick_model_flow_col(df: pd.DataFrame, class_id: str):
    target = "VEHS({})".format(str(class_id).strip())
    for c in df.columns:
        if str(c).strip().upper() == target:
            return c
    return None


def compute_flow_for_class(xlsx: Path, scenario: str, mov_att: Path, model_start: str,
                           eval_start: str, eval_dur: int, class_id: str,
                           run_mode: str, selected_runs: list) -> dict:

    fdef = load_flow_definition(xlsx)
    fobs = load_flow_observed_wide(xlsx)

    obs_col = _obs_col(scenario, class_id)
    if obs_col not in fobs.columns:
        raise KeyError("Missing observed column '{}' in Flow_Observed".format(obs_col))

    raw = read_mov_att(str(mov_att))
    node = _normalise_movement_df(raw)

    model_col = pick_model_flow_col(node, class_id)
    if not model_col:
        raise KeyError("No model column found for class {} (expected VEHS({}))".format(class_id, class_id))

    node["Flow"] = pd.to_numeric(node[model_col], errors="coerce").fillna(0.0)
    node = add_clock(node, model_start, "TimeInterval")
    bins = build_15min_bins(eval_start, int(eval_dur))
    node = node[node["ClockTime"].isin(bins)].copy()

    if run_mode == "Select runs" and selected_runs:
        node = node[node["SimRun"].isin(selected_runs)].copy()

    mov_run = node.groupby(["SimRun", "Node", "FromLink", "ToLink"], as_index=False)["Flow"].sum().rename(columns={"Flow": "Model_RunTotal"})

    model_tbl = fdef.merge(mov_run, on=["Node", "FromLink", "ToLink"], how="left")
    model_tbl = model_tbl.groupby(["DefID", "SiteName", "FlowType", "Node", "FromLink", "ToLink", "FromArm", "ToArm", "FlowName"], as_index=False)        .agg(Modelled_Flow=("Model_RunTotal", "mean"))

    key = ["SiteName", "FlowType", "Node", "FromLink", "ToLink"]
    obs_map = fobs[key + ["FromArm", "ToArm", "FlowName", obs_col]].copy()
    obs_map.rename(columns={obs_col: "Obs_Flow"}, inplace=True)

    out = model_tbl.merge(obs_map, on=key, how="left")
    out = add_flow_metrics(out)

    return {
        "TURN": out[out["FlowType"] == "TURN"].copy(),
        "ENTRY": out[out["FlowType"] == "ENTRY"].copy(),
        "EXIT": out[out["FlowType"] == "EXIT"].copy(),
        "meta": {"class_id": str(class_id), "model_col": model_col, "bins": bins},
    }


def compute_flow_total_T1(xlsx: Path, scenario: str, mov_att: Path, model_start: str,
                          eval_start: str, eval_dur: int, class_ids: list,
                          run_mode: str, selected_runs: list) -> dict:

    fdef = load_flow_definition(xlsx)
    fobs = load_flow_observed_wide(xlsx)

    obs_cols = [_obs_col(scenario, cid) for cid in class_ids]
    missing = [c for c in obs_cols if c not in fobs.columns]
    if missing:
        raise KeyError("Missing observed columns in Flow_Observed: {}".format(missing))

    fobs = fobs.copy()
    fobs["Obs_Flow"] = 0.0
    for c in obs_cols:
        fobs["Obs_Flow"] += pd.to_numeric(fobs[c], errors="coerce").fillna(0.0)

    raw = read_mov_att(str(mov_att))
    node = _normalise_movement_df(raw)

    model_cols = []
    for cid in class_ids:
        col = pick_model_flow_col(node, cid)
        if not col:
            raise KeyError("No model column found for class {} (expected VEHS({}))".format(cid, cid))
        model_cols.append(col)

    node["Flow"] = 0.0
    for c in model_cols:
        node["Flow"] += pd.to_numeric(node[c], errors="coerce").fillna(0.0)

    node = add_clock(node, model_start, "TimeInterval")
    bins = build_15min_bins(eval_start, int(eval_dur))
    node = node[node["ClockTime"].isin(bins)].copy()

    if run_mode == "Select runs" and selected_runs:
        node = node[node["SimRun"].isin(selected_runs)].copy()

    mov_run = node.groupby(["SimRun", "Node", "FromLink", "ToLink"], as_index=False)["Flow"].sum().rename(columns={"Flow": "Model_RunTotal"})

    model_tbl = fdef.merge(mov_run, on=["Node", "FromLink", "ToLink"], how="left")
    model_tbl = model_tbl.groupby(["DefID", "SiteName", "FlowType", "Node", "FromLink", "ToLink", "FromArm", "ToArm", "FlowName"], as_index=False)        .agg(Modelled_Flow=("Model_RunTotal", "mean"))

    key = ["SiteName", "FlowType", "Node", "FromLink", "ToLink"]
    obs_map = fobs[key + ["FromArm", "ToArm", "FlowName", "Obs_Flow"]].copy()

    out = model_tbl.merge(obs_map, on=key, how="left")
    out = add_flow_metrics(out)

    return {
        "TURN": out[out["FlowType"] == "TURN"].copy(),
        "ENTRY": out[out["FlowType"] == "ENTRY"].copy(),
        "EXIT": out[out["FlowType"] == "EXIT"].copy(),
        "meta": {"class_id": "TOTAL", "model_cols": model_cols, "bins": bins},
    }
