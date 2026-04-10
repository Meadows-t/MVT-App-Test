from __future__ import annotations

from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from common.time_utils import TIME_RE, build_bins, add_clock
from app_io.workbooks import load_workbook_from_path
from att_tools import read_jt_att, normalise_jt_att_base


def parse_stacked_jt_sheet(df_raw: pd.DataFrame) -> dict:
    blocks = {}
    r = 0
    nrows = df_raw.shape[0]

    while r < nrows:
        title = str(df_raw.iat[r, 0]) if pd.notna(df_raw.iat[r, 0]) else ""
        title = title.strip()
        if not title or title.lower().startswith("nan"):
            r += 1
            continue
        if r + 1 >= nrows:
            break

        header = df_raw.iloc[r + 1].tolist()
        time_cols = [str(h).strip() for h in header if TIME_RE.match(str(h).strip())]

        rows = []
        rr = r + 2
        while rr < nrows:
            if df_raw.iloc[rr].isna().all():
                break
            first = df_raw.iat[rr, 0]
            if pd.notna(first) and str(first).strip() and df_raw.iloc[rr].notna().sum() <= 1:
                break
            rows.append(df_raw.iloc[rr].tolist())
            rr += 1

        if rows:
            tmp = pd.DataFrame(rows, columns=header)
            for bc in ["VehicleClassID", "VehicleClassName", "RouteName"]:
                if bc not in tmp.columns:
                    for c in tmp.columns:
                        if str(c).strip().lower() == bc.lower():
                            tmp.rename(columns={c: bc}, inplace=True)

            long = tmp.melt(id_vars=["VehicleClassID", "VehicleClassName", "RouteName"],
                            value_vars=time_cols, var_name="ClockTime", value_name="ObsJT")
            long["VehicleClassID"] = long["VehicleClassID"].astype(str).str.strip()
            long["RouteName"] = long["RouteName"].astype(str).str.strip()
            long["ClockTime"] = long["ClockTime"].astype(str).str.strip()
            long["ObsJT"] = pd.to_numeric(long["ObsJT"], errors="coerce")
            blocks[title] = long[["VehicleClassID", "RouteName", "ClockTime", "ObsJT"]]

        r = rr + 1

    return blocks


def run_jt(xlsx: Path, scenario: str, jt_att: Path, simruns_token: str, run_mode: str, selected_runs: list,
           jt_eval_start: str, jt_eval_dur: int, jt_classes_sel: list, jt_groups_sel: list,
           vttm_id_to_name: dict) -> dict:

    xlsx, cfg_df, _ = load_workbook_from_path(xlsx)
    row = cfg_df[cfg_df["ScenarioName"] == str(scenario)].iloc[0]
    model_start = str(row["ModelStartHHMM"])

    jt_raw = pd.read_excel(xlsx, sheet_name="JT", header=None, engine="openpyxl")
    blocks = parse_stacked_jt_sheet(jt_raw)

    title_key = (str(row["PreferredName"]) + " – " + str(row["Peak"])).strip(" –")
    if title_key not in blocks:
        raise KeyError("Observed JT block '{}' not found".format(title_key))

    bins = build_bins(jt_eval_start, int(jt_eval_dur), 15)

    obs_long = blocks[title_key].copy()
    obs_long = obs_long[obs_long["ClockTime"].isin(bins)].copy()
    obs_win = obs_long.groupby(["RouteName", "VehicleClassID"], as_index=False).agg(Observed_JT=("ObsJT", "mean"))

    grp = pd.read_excel(xlsx, sheet_name="JT_Grouping", engine="openpyxl")
    grp["Group_ID"] = grp.get("Group_ID", "").astype(str).str.strip()
    grp = grp[grp["Group_ID"] != ""].copy()
    grp["Segment_Order"] = pd.to_numeric(grp.get("Segment_Order"), errors="coerce")
    grp["Model_Segment_ID"] = grp.get("Model_Segment_ID", "").astype(str).str.strip()
    grp["Model_Segment_Name"] = grp.get("Model_Segment_Name", "").astype(str).str.strip()
    grp["Model_Segment_Name"] = grp.apply(lambda r: r["Model_Segment_Name"] if r["Model_Segment_Name"] else vttm_id_to_name.get(r["Model_Segment_ID"], ""), axis=1)
    grp = grp[grp["Model_Segment_Name"] != ""].copy()

    if jt_groups_sel:
        grp = grp[grp["Group_ID"].isin(jt_groups_sel)].copy()

    jt_base = add_clock(normalise_jt_att_base(read_jt_att(str(jt_att))), model_start, "TimeInterval")
    jt_base = jt_base[jt_base["ClockTime"].isin(bins)].copy()

    try:
        N = int(simruns_token) if str(simruns_token).strip().isdigit() else 0
    except Exception:
        N = 0
    if run_mode == "Select runs" and N > 0:
        jt_base = jt_base[jt_base["SimRun"].isin(selected_runs)].copy()

    # distance column
    dist_col = None
    for c in jt_base.columns:
        if str(c).strip().upper() in ["VEHICLETRAVELTIMEMEASUREMENT\DIST", "VEHICLETRAVELTIMEMEASUREMENT\\DIST"]:
            dist_col = c
            break
    if dist_col is None:
        cand = [c for c in jt_base.columns if "DIST" in str(c).upper() and "(" not in str(c)]
        dist_col = cand[0] if cand else None
    if dist_col is None:
        raise KeyError("Distance column not found in JT ATT")

    jt_base[dist_col] = pd.to_numeric(jt_base[dist_col], errors="coerce")
    jt_base["RouteName"] = jt_base["VTTM_ID"].map(lambda x: vttm_id_to_name.get(str(x).strip(), str(x).strip()))

    out_tabs = []
    for vc in jt_classes_sel:
        vc = str(vc)
        vehs_col = None
        trav_col = None
        for c in jt_base.columns:
            if str(c).strip().upper() == "VEHS({})".format(vc):
                vehs_col = c
            if str(c).strip().upper() == "TRAVTM({})".format(vc):
                trav_col = c
        if vehs_col is None or trav_col is None:
            continue

        tmp = jt_base[["SimRun", "ClockTime", "RouteName", vehs_col, trav_col, dist_col]].copy()
        tmp.rename(columns={vehs_col: "Vehs", trav_col: "TravTm", dist_col: "SegDist"}, inplace=True)
        tmp["Vehs"] = pd.to_numeric(tmp["Vehs"], errors="coerce")
        tmp["TravTm"] = pd.to_numeric(tmp["TravTm"], errors="coerce")
        tmp["SegDist"] = pd.to_numeric(tmp["SegDist"], errors="coerce")

        def wavg(g):
            denom = g["Vehs"].sum()
            return (g["Vehs"] * g["TravTm"]).sum() / denom if denom and denom > 0 else g["TravTm"].mean()

        win_run = tmp.groupby(["SimRun", "RouteName"], as_index=False).apply(
            lambda g: pd.Series({
                "Modelled JT": wavg(g),
                "Vehicles": g["Vehs"].sum(),
                "SegDist": g["SegDist"].dropna().iloc[0] if g["SegDist"].notna().any() else None,
            })
        ).reset_index(drop=True)

        agg = win_run.groupby("RouteName", as_index=False).agg(
            **{
                "Modelled JT": ("Modelled JT", "mean"),
                "ModelJT_min": ("Modelled JT", "min"),
                "ModelJT_max": ("Modelled JT", "max"),
                "Vehicles": ("Vehicles", "mean"),
                "SegDist": ("SegDist", "first"),
            }
        )

        obs_vc = obs_win[obs_win["VehicleClassID"] == vc].copy()

        seg_tbl = pd.merge(grp[["Group_ID", "Segment_Order", "Model_Segment_Name"]],
                           agg.rename(columns={"RouteName": "Model_Segment_Name"}),
                           on="Model_Segment_Name", how="left")
        seg_tbl = pd.merge(seg_tbl,
                           obs_vc.rename(columns={"RouteName": "Model_Segment_Name"}),
                           on="Model_Segment_Name", how="left")

        seg_tbl.rename(columns={"Model_Segment_Name": "Segment name"}, inplace=True)
        seg_tbl["Observed JT"] = pd.to_numeric(seg_tbl["Observed_JT"], errors="coerce")
        seg_tbl["Modelled JT"] = pd.to_numeric(seg_tbl["Modelled JT"], errors="coerce")
        seg_tbl["Vehicles"] = pd.to_numeric(seg_tbl["Vehicles"], errors="coerce")

        seg_tbl["Diff"] = seg_tbl["Modelled JT"] - seg_tbl["Observed JT"]
        seg_tbl["% diff"] = 100.0 * seg_tbl["Diff"] / seg_tbl["Observed JT"]
        seg_tbl.loc[seg_tbl["Observed JT"].isna() | (seg_tbl["Observed JT"] == 0), "% diff"] = pd.NA
        seg_tbl["Pass"] = seg_tbl["% diff"].abs().le(15) & seg_tbl["Diff"].abs().le(60)

        seg_tbl = seg_tbl[["Group_ID", "Segment name", "Observed JT", "Modelled JT", "Vehicles", "Diff", "% diff", "Pass", "Segment_Order", "SegDist", "ModelJT_min", "ModelJT_max"]].copy()

        grp_tot = seg_tbl.groupby("Group_ID", as_index=False).agg(Observed_JT=("Observed JT", "sum"), Modelled_JT=("Modelled JT", "sum"), Vehicles=("Vehicles", "sum"))
        grp_tot.rename(columns={"Observed_JT": "Observed JT", "Modelled_JT": "Modelled JT"}, inplace=True)
        grp_tot["Diff"] = grp_tot["Modelled JT"] - grp_tot["Observed JT"]
        grp_tot["% diff"] = 100.0 * grp_tot["Diff"] / grp_tot["Observed JT"]
        grp_tot.loc[grp_tot["Observed JT"].isna() | (grp_tot["Observed JT"] == 0), "% diff"] = pd.NA
        grp_tot["Pass"] = grp_tot["% diff"].abs().le(15) & grp_tot["Diff"].abs().le(60)

        groups_plot = jt_groups_sel or sorted(seg_tbl["Group_ID"].dropna().unique().tolist())
        nplots = len(groups_plot)
        ncols = 2 if nplots > 1 else 1
        nrows = (nplots + ncols - 1) // ncols
        fig = make_subplots(rows=nrows, cols=ncols, subplot_titles=[str(g) for g in groups_plot])

        for idx2, gid in enumerate(groups_plot, start=1):
            rr = (idx2 - 1) // ncols + 1
            cc = (idx2 - 1) % ncols + 1
            gdf = seg_tbl[seg_tbl["Group_ID"] == gid].sort_values("Segment_Order").copy()
            if gdf.empty:
                continue

            dist = pd.to_numeric(gdf["SegDist"], errors="coerce").fillna(0).tolist()
            cumD = [0]
            for d in dist:
                cumD.append(cumD[-1] + float(d or 0))

            def cum(arr):
                out = [0]
                s = 0
                for x in arr:
                    s += float(x or 0)
                    out.append(s)
                return out

            cumObs = cum(pd.to_numeric(gdf["Observed JT"], errors="coerce").fillna(0).tolist())
            cumMean = cum(pd.to_numeric(gdf["Modelled JT"], errors="coerce").fillna(0).tolist())
            cumLow = cum(pd.to_numeric(gdf["ModelJT_min"], errors="coerce").fillna(0).tolist())
            cumHigh = cum(pd.to_numeric(gdf["ModelJT_max"], errors="coerce").fillna(0).tolist())
            cumUpper = [1.15 * x for x in cumObs]
            cumLower = [0.85 * x for x in cumObs]

            fig.add_trace(go.Scatter(x=cumD, y=cumHigh, mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip"), row=rr, col=cc)
            fig.add_trace(go.Scatter(x=cumD, y=cumLow, mode="lines", line=dict(width=0), fill="tonexty",
                                     fillcolor="rgba(160,160,160,0.25)", name="Model band", showlegend=(idx2 == 1), hoverinfo="skip"), row=rr, col=cc)
            fig.add_trace(go.Scatter(x=cumD, y=cumMean, mode="lines+markers", line=dict(color="black", width=3),
                                     name="Model mean", showlegend=(idx2 == 1)), row=rr, col=cc)
            fig.add_trace(go.Scatter(x=cumD, y=cumObs, mode="lines+markers", line=dict(color="#E31A1C", width=4),
                                     name="Observed", showlegend=(idx2 == 1)), row=rr, col=cc)
            fig.add_trace(go.Scatter(x=cumD, y=cumUpper, mode="lines", line=dict(color="black", width=2, dash="dash"),
                                     name="+15% obs", showlegend=(idx2 == 1)), row=rr, col=cc)
            fig.add_trace(go.Scatter(x=cumD, y=cumLower, mode="lines", line=dict(color="black", width=2, dash="dash"),
                                     name="-15% obs", showlegend=(idx2 == 1)), row=rr, col=cc)

        fig.update_layout(title="Time–distance (Class {})".format(vc), height=360 * nrows,
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0))

        seg_out = seg_tbl[["Group_ID", "Segment name", "Observed JT", "Modelled JT", "Vehicles", "Diff", "% diff", "Pass"]].copy()
        grp_out = grp_tot[["Group_ID", "Observed JT", "Modelled JT", "Vehicles", "Diff", "% diff", "Pass"]].copy()

        out_tabs.append({"class": vc, "seg_tbl": seg_out, "grp_tbl": grp_out, "fig": fig})

    return {"tabs": out_tabs}
