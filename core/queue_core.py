from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from common.time_utils import build_bins, add_clock
from app_io.workbooks import load_workbook_from_path
from att_tools import read_queue_att, normalise_queue_att


def hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = (hex_color or "#808080").lstrip("#")
    if len(h) != 6:
        h = "808080"
    r = int(h[0:2], 16); g = int(h[2:4], 16); b = int(h[4:6], 16)
    return "rgba({},{},{},{})".format(r, g, b, alpha)


def run_queue(xlsx: Path, scenario: str, queue_att: Path, simruns_token: str, run_mode: str, selected_runs: list,
              q_eval_start: str, q_eval_dur: int, q_counters: list, q_show_runs: bool,
              q_col_obs: str, q_col_mean: str, q_col_runs: str, q_col_band: str,
              qc_id_to_name: dict, qc_name_to_id: dict) -> dict:

    xlsx, cfg_df, _ = load_workbook_from_path(xlsx)
    row = cfg_df[cfg_df["ScenarioName"] == str(scenario)].iloc[0]
    preferred = str(row["PreferredName"])
    peak = str(row["Peak"])
    model_start = str(row["ModelStartHHMM"])

    obs_q = pd.read_excel(xlsx, sheet_name="Queue", engine="openpyxl")
    obs_q = obs_q[(obs_q["PreferredName"].astype(str) == preferred) & (obs_q["Peak"].astype(str) == peak)].copy()
    queue_cols = [c for c in obs_q.columns if c not in ("PreferredName", "Peak", "ClockTime")]

    bins = build_bins(q_eval_start, int(q_eval_dur), 5)
    try:
        sdt = datetime.strptime(str(q_eval_start), "%H:%M")
        end_tick = (sdt + timedelta(minutes=int(q_eval_dur))).strftime("%H:%M")
        bins_inc = list(bins) + [end_tick]
    except Exception:
        bins_inc = list(bins)

    obs_slice = obs_q[obs_q["ClockTime"].astype(str).isin(bins_inc)].copy()
    obs_slice["ClockTime"] = obs_slice["ClockTime"].astype(str)

    q_att = add_clock(normalise_queue_att(read_queue_att(str(queue_att))), model_start, "TimeInterval")
    q_att = q_att[q_att["ClockTime"].isin(bins)].copy()

    try:
        N = int(simruns_token) if str(simruns_token).strip().isdigit() else 0
    except Exception:
        N = 0

    if run_mode == "Select runs" and N > 0:
        q_att = q_att[q_att["SimRun"].isin(selected_runs)].copy()

    sel = q_counters or queue_cols[:1]
    nplots = len(sel)
    ncols = 2 if nplots > 1 else 1
    nrows = (nplots + ncols - 1) // ncols
    fig = make_subplots(rows=nrows, cols=ncols, subplot_titles=[str(x) for x in sel])

    for idx, qc_name in enumerate(sel, start=1):
        rr = (idx - 1) // ncols + 1
        cc = (idx - 1) % ncols + 1

        if qc_name not in obs_slice.columns:
            continue

        obs_series = obs_slice[["ClockTime", qc_name]].rename(columns={qc_name: "Observed"})
        qc_id = qc_name_to_id.get(str(qc_name), None) or str(qc_name).strip()

        q_sub = q_att[q_att["QueueCounter"].astype(str).str.strip() == str(qc_id).strip()].copy()
        if q_sub.empty:
            continue

        wide = q_sub.pivot_table(index="ClockTime", columns="SimRun", values="QLenMax", aggfunc="mean").reset_index()
        wide = wide.set_index("ClockTime").reindex(bins_inc).reset_index()

        run_cols = [c for c in wide.columns if c != "ClockTime"]
        if not run_cols:
            continue

        wide["Low"] = wide[run_cols].min(axis=1)
        wide["High"] = wide[run_cols].max(axis=1)
        wide["Mean"] = wide[run_cols].mean(axis=1)
        plot_df = pd.merge(wide, obs_series, on="ClockTime", how="left")

        fig.add_trace(go.Scatter(x=plot_df["ClockTime"], y=plot_df["High"], mode="lines", line=dict(width=0),
                                 showlegend=False, hoverinfo="skip"), row=rr, col=cc)
        fig.add_trace(go.Scatter(x=plot_df["ClockTime"], y=plot_df["Low"], mode="lines", line=dict(width=0),
                                 fill="tonexty", fillcolor=hex_to_rgba(q_col_band, 0.25),
                                 name="Band", showlegend=(idx == 1), hoverinfo="skip"), row=rr, col=cc)

        if q_show_runs:
            for rc in sorted(run_cols):
                fig.add_trace(go.Scatter(x=plot_df["ClockTime"], y=plot_df[rc], mode="lines",
                                         line=dict(color=hex_to_rgba(q_col_runs, 0.55), width=1, dash="dot"),
                                         showlegend=False), row=rr, col=cc)

        # Model mean BLACK
        fig.add_trace(go.Scatter(x=plot_df["ClockTime"], y=plot_df["Mean"], mode="lines+markers",
                                 line=dict(color=q_col_mean, width=3),
                                 name="Model mean", showlegend=(idx == 1)), row=rr, col=cc)
        # Observed RED
        fig.add_trace(go.Scatter(x=plot_df["ClockTime"], y=plot_df["Observed"], mode="lines+markers",
                                 line=dict(color=q_col_obs, width=4),
                                 name="Observed", showlegend=(idx == 1)), row=rr, col=cc)

    fig.update_xaxes(categoryorder="array", categoryarray=bins_inc)
    fig.update_layout(title="Queue | {} – {}".format(preferred, peak), height=320 * nrows,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0))

    obs_long = obs_slice.melt(id_vars=["ClockTime"], value_vars=queue_cols, var_name="QueueCounterName", value_name="Obs")
    obs_long["Obs"] = pd.to_numeric(obs_long["Obs"], errors="coerce")

    q_att_tmp = q_att.copy()
    q_att_tmp["QueueCounterName"] = q_att_tmp["QueueCounter"].map(lambda x: qc_id_to_name.get(str(x).strip(), str(x).strip()))
    mmq_long = q_att_tmp.groupby(["ClockTime", "QueueCounterName"], as_index=False).agg(MMQ=("QLenMax", "mean"))
    join = pd.merge(obs_long, mmq_long, on=["ClockTime", "QueueCounterName"], how="outer")

    mean_tbl = join.groupby("QueueCounterName", as_index=False).agg(Observed_MeanMaxQ=("Obs", "mean"), Modelled_MeanMaxQ=("MMQ", "mean"))
    max_tbl = join.groupby("QueueCounterName", as_index=False).agg(Observed_MaxMaxQ=("Obs", "max"), Modelled_MaxMaxQ=("MMQ", "max"))

    return {"fig": fig, "mean_tbl": mean_tbl, "max_tbl": max_tbl}
