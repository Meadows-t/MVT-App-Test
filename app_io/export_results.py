# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from datetime import datetime
import pandas as pd

from common.path_utils import ensure_dir
from common.time_utils import add_clock, build_bins
from att_tools import read_queue_att, normalise_queue_att, read_jt_att, normalise_jt_att_base, read_mov_att
from unified_inpx_tools import parse_inpx


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def default_export_path(model_root: Path, scenario: str) -> Path:
    out_dir = Path(model_root) / "_OutData"
    ensure_dir(out_dir)
    scen = str(scenario).strip().replace("/", "_").replace("\\", "_")
    return out_dir / ("Compiled_Output_" + scen + "_" + _ts() + ".xlsx")


def _get_inpx_maps(model_root: Path) -> tuple[dict, dict]:
    """Return (queuecounter_id_to_name, vttm_id_to_name) from first INPX in model_root."""
    model_root = Path(model_root)
    inpx = list(model_root.glob("*.inpx"))
    if not inpx:
        return {}, {}
    info = parse_inpx(str(inpx[0]))
    qc = {str(x.get("no")).strip(): str(x.get("name")).strip() for x in info.get("queue_counters", []) if x.get("no") and x.get("name")}
    vttm = {str(x.get("no")).strip(): str(x.get("name")).strip() for x in info.get("jt_measurements", []) if x.get("no") and x.get("name")}
    return qc, vttm


def export_queue_wide_fullatt(queue_att: Path, model_start: str, preferred: str, peak: str, qc_id_to_name: dict) -> pd.DataFrame:
    """WIDE, full ATT, all simruns. Columns: Preferred Name, Peak, Clock time, QueueCounterName, Run_#"""
    raw = read_queue_att(str(queue_att))
    q = add_clock(normalise_queue_att(raw), model_start, "TimeInterval")

    q["Preferred Name"] = str(preferred)
    q["Peak"] = str(peak)
    q["Clock time"] = q["ClockTime"].astype(str)

    q["QueueCounterName"] = q["QueueCounter"].map(lambda x: qc_id_to_name.get(str(x).strip(), str(x).strip()))

    # pivot to wide
    wide = q.pivot_table(
        index=["Preferred Name", "Peak", "Clock time", "QueueCounterName"],
        columns="SimRun",
        values="QLenMax",
        aggfunc="mean",
    )

    wide = wide.reset_index()

    # sort simrun columns and rename to Run_#
    run_cols = [c for c in wide.columns if isinstance(c, int)]
    run_cols_sorted = sorted(run_cols)
    rename = {c: "Run_" + str(c) for c in run_cols_sorted}
    wide.rename(columns=rename, inplace=True)

    # ensure final column order
    ordered = ["Preferred Name", "Peak", "Clock time", "QueueCounterName"] + [rename[c] for c in run_cols_sorted]
    wide = wide[ordered]

    return wide


def export_jt_wide_fullatt(jt_att: Path, model_start: str, preferred: str, peak: str, jt_class_id: str, vttm_id_to_name: dict) -> pd.DataFrame:
    """WIDE, full ATT, all simruns. Columns: Preferred Name, Peak, Clock time, RouteName, Run_#_JT, Run_#_Veh"""
    base = add_clock(normalise_jt_att_base(read_jt_att(str(jt_att))), model_start, "TimeInterval")

    jt_class_id = str(jt_class_id).strip()

    trav_col = None
    veh_col = None
    for c in base.columns:
        if str(c).strip().upper() == ("TRAVTM(" + jt_class_id + ")"):
            trav_col = c
        if str(c).strip().upper() == ("VEHS(" + jt_class_id + ")"):
            veh_col = c

    if trav_col is None or veh_col is None:
        raise KeyError("JT ATT missing TRAVTM({}) or VEHS({}) columns".format(jt_class_id, jt_class_id))

    base["Preferred Name"] = str(preferred)
    base["Peak"] = str(peak)
    base["Clock time"] = base["ClockTime"].astype(str)

    base["RouteName"] = base["VTTM_ID"].map(lambda x: vttm_id_to_name.get(str(x).strip(), str(x).strip()))

    tmp = base[["Preferred Name", "Peak", "Clock time", "RouteName", "SimRun", trav_col, veh_col]].copy()
    tmp.rename(columns={trav_col: "JT", veh_col: "Veh"}, inplace=True)
    tmp["JT"] = pd.to_numeric(tmp["JT"], errors="coerce")
    tmp["Veh"] = pd.to_numeric(tmp["Veh"], errors="coerce")

    # pivot JT and Veh separately
    jt_w = tmp.pivot_table(index=["Preferred Name", "Peak", "Clock time", "RouteName"], columns="SimRun", values="JT", aggfunc="mean")
    vh_w = tmp.pivot_table(index=["Preferred Name", "Peak", "Clock time", "RouteName"], columns="SimRun", values="Veh", aggfunc="mean")

    jt_w = jt_w.reset_index()
    vh_w = vh_w.reset_index()

    # identify simrun cols
    jt_runs = sorted([c for c in jt_w.columns if isinstance(c, int)])

    # build final with side-by-side columns
    out = jt_w[["Preferred Name", "Peak", "Clock time", "RouteName"]].copy()
    for r in jt_runs:
        out["Run_" + str(r) + "_JT"] = jt_w[r]
        out["Run_" + str(r) + "_Veh"] = vh_w[r] if r in vh_w.columns else pd.NA

    return out


def _parse_movement_str(mov: str):
    s = str(mov).strip()
    if not s:
        return "", "", ""
    parts = s.split("-")
    if len(parts) < 3:
        return "", "", ""
    node = parts[0].strip()
    fr = parts[1].split("@")[0].strip()
    to = parts[2].split("@")[0].strip()
    return node, fr, to


def export_flow_avg_option1(workbook: Path, scenario: str, mov_att: Path, model_start: str, eval_start: str, eval_dur: int, class_ids: list[str]) -> dict:
    """Flow Option 1: evaluation totals only, model AVG across all simruns, no observed."""
    fdef = pd.read_excel(workbook, sheet_name="Flow_Definition", engine="openpyxl")

    # normalize key fields
    fdef = fdef.copy()
    for c in ["SiteName", "FlowType", "Node", "FromLink", "ToLink", "FromArm", "ToArm", "FlowName"]:
        if c in fdef.columns:
            fdef[c] = fdef[c].fillna("").astype(str).str.strip()
    fdef["FlowType"] = fdef["FlowType"].astype(str).str.strip().str.upper()

    raw = read_mov_att(str(mov_att))

    # find required cols
    cols_u = {str(c).strip().upper(): c for c in raw.columns}
    run_col = cols_u.get("SIMRUN") or cols_u.get("RUN")
    int_col = cols_u.get("TIMEINT") or cols_u.get("TIMEINTERVAL")
    mov_col = cols_u.get("MOVEMENT")

    if run_col is None or int_col is None or mov_col is None:
        raise KeyError("MOVEMENTEVALUATION ATT missing SIMRUN/TIMEINT/MOVEMENT")

    mv = raw.rename(columns={run_col: "SimRun", int_col: "TimeInterval", mov_col: "MOVEMENT"}).copy()
    mv["SimRun"] = mv["SimRun"].astype(str).str.strip()
    mv = mv[mv["SimRun"].str.match(r"^\d+$")].copy()
    mv["SimRun"] = mv["SimRun"].astype(int)

    parsed = mv["MOVEMENT"].map(_parse_movement_str)
    mv["Node"] = parsed.map(lambda x: x[0])
    mv["FromLink"] = parsed.map(lambda x: x[1])
    mv["ToLink"] = parsed.map(lambda x: x[2])

    mv = add_clock(mv, model_start, "TimeInterval")

    # evaluation bins (15-min)
    bins = build_bins(eval_start, int(eval_dur), 15)
    mv = mv[mv["ClockTime"].astype(str).isin([str(x) for x in bins])].copy()

    def pick_col(cid: str):
        tgt = ("VEHS(" + str(cid).strip() + ")").upper()
        for c in mv.columns:
            if str(c).strip().upper() == tgt:
                return c
        return None

    out = {}

    # per class
    for cid in class_ids:
        col = pick_col(cid)
        if col is None:
            continue

        mv2 = mv[["SimRun", "Node", "FromLink", "ToLink", col]].copy()
        mv2["Flow"] = pd.to_numeric(mv2[col], errors="coerce").fillna(0.0)

        per_run = mv2.groupby(["SimRun", "Node", "FromLink", "ToLink"], as_index=False)["Flow"].sum()
        avg = per_run.groupby(["Node", "FromLink", "ToLink"], as_index=False).agg(Modelled_Flow_AVG=("Flow", "mean"))

        merged = fdef.merge(avg, on=["Node", "FromLink", "ToLink"], how="left")
        merged["Modelled_Flow_AVG"] = pd.to_numeric(merged["Modelled_Flow_AVG"], errors="coerce")

        out[str(cid)] = {
            "TURN": merged[merged["FlowType"] == "TURN"].copy(),
            "ENTRY": merged[merged["FlowType"] == "ENTRY"].copy(),
            "EXIT": merged[merged["FlowType"] == "EXIT"].copy(),
        }

    # TOTAL
    cols = [pick_col(cid) for cid in class_ids]
    cols = [c for c in cols if c]
    if cols:
        mv3 = mv[["SimRun", "Node", "FromLink", "ToLink"] + cols].copy()
        mv3["Flow"] = 0.0
        for c in cols:
            mv3["Flow"] += pd.to_numeric(mv3[c], errors="coerce").fillna(0.0)

        per_run = mv3.groupby(["SimRun", "Node", "FromLink", "ToLink"], as_index=False)["Flow"].sum()
        avg = per_run.groupby(["Node", "FromLink", "ToLink"], as_index=False).agg(Modelled_Flow_AVG=("Flow", "mean"))

        merged = fdef.merge(avg, on=["Node", "FromLink", "ToLink"], how="left")
        merged["Modelled_Flow_AVG"] = pd.to_numeric(merged["Modelled_Flow_AVG"], errors="coerce")

        out["TOTAL"] = {
            "TURN": merged[merged["FlowType"] == "TURN"].copy(),
            "ENTRY": merged[merged["FlowType"] == "ENTRY"].copy(),
            "EXIT": merged[merged["FlowType"] == "EXIT"].copy(),
        }

    return out


def export_all_to_excel(
    export_path: Path,
    model_root: Path,
    workbook: Path,
    scenario: str,
    preferred: str,
    peak: str,
    model_start: str,
    eval_start: str,
    eval_dur: int,
    queue_att: Path | None,
    jt_att: Path | None,
    mov_att: Path | None,
    jt_class_id: str | None,
    flow_class_ids: list[str] | None,
) -> Path:

    model_root = Path(model_root)
    workbook = Path(workbook)
    export_path = Path(export_path)
    ensure_dir(export_path.parent)

    qc_id_to_name, vttm_id_to_name = _get_inpx_maps(model_root)

    meta = [
        {"Key": "Scenario", "Value": str(scenario)},
        {"Key": "Preferred Name", "Value": str(preferred)},
        {"Key": "Peak", "Value": str(peak)},
        {"Key": "ModelStartHHMM", "Value": str(model_start)},
        {"Key": "EvalStartHHMM", "Value": str(eval_start)},
        {"Key": "EvalDurationMin", "Value": str(eval_dur)},
        {"Key": "Queue ATT", "Value": "" if not queue_att else str(queue_att)},
        {"Key": "JT ATT", "Value": "" if not jt_att else str(jt_att)},
        {"Key": "Movement ATT", "Value": "" if not mov_att else str(mov_att)},
        {"Key": "ExportedAt", "Value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
    ]

    with pd.ExcelWriter(export_path, engine="openpyxl") as xl:
        pd.DataFrame(meta).to_excel(xl, "Summary", index=False)

        # Queue ALL
        if queue_att:
            qdf = export_queue_wide_fullatt(queue_att, model_start, preferred, peak, qc_id_to_name)
            qdf.to_excel(xl, "Queue_FullATT_Wide", index=False)

        # JT ALL
        if jt_att and jt_class_id:
            jdf = export_jt_wide_fullatt(jt_att, model_start, preferred, peak, jt_class_id, vttm_id_to_name)
            jdf.to_excel(xl, "JT_FullATT_Wide", index=False)

        # Flow AVG Option 1
        if mov_att and flow_class_ids:
            flow = export_flow_avg_option1(workbook, scenario, mov_att, model_start, eval_start, eval_dur, list(flow_class_ids))
            for cid, part in flow.items():
                for ft in ["TURN", "ENTRY", "EXIT"]:
                    df = part.get(ft)
                    if df is None:
                        continue
                    # keep key columns + AVG only
                    keep = ["SiteName", "FlowType", "Node", "FromLink", "ToLink", "FromArm", "ToArm", "FlowName", "Modelled_Flow_AVG"]
                    keep = [c for c in keep if c in df.columns]
                    df2 = df[keep].copy()
                    sheet = ("Flow_" + str(cid) + "_" + ft)[:31]
                    df2.to_excel(xl, sheet, index=False)

    return export_path
