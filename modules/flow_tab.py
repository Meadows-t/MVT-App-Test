from __future__ import annotations

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from common.styling import geh_row_style


def _to_num(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _combine(res: dict) -> pd.DataFrame:
    frames = []
    for ft in ["TURN", "ENTRY", "EXIT"]:
        if ft in res and isinstance(res[ft], pd.DataFrame):
            frames.append(res[ft].copy())
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _movement_label(row: pd.Series) -> str:
    site = str(row.get("SiteName", "")).strip()
    ft = str(row.get("FlowType", "")).strip().upper()
    fa = str(row.get("FromArm", "")).strip()
    ta = str(row.get("ToArm", "")).strip()
    fn = str(row.get("FlowName", "")).strip()
    node = str(row.get("Node", "")).strip()
    fr = str(row.get("FromLink", "")).strip()
    to = str(row.get("ToLink", "")).strip()

    if ft == "TURN":
        arm_txt = (fa + "→" + ta) if (fa and ta) else ""
    elif ft == "ENTRY":
        arm_txt = fa if fa else ""
    else:
        arm_txt = ta if ta else ""

    key_txt = (node + ":" + fr + "->" + to) if (node and fr and to) else ""
    bits = [b for b in [site, arm_txt, fn, key_txt] if b]
    return " | ".join(bits) if bits else key_txt


def _diagnostics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"n": 0, "matched_model": 0, "missing_model": 0, "missing_obs": 0, "both_missing": 0, "geh_gt5": 0, "geh_pass_rate": None}

    d = _to_num(df, ["Obs_Flow", "Modelled_Flow", "GEH"])
    miss_model = d["Modelled_Flow"].isna()
    miss_obs = d["Obs_Flow"].isna()
    geh_valid = d["GEH"].notna()

    geh_gt5 = int((geh_valid & (d["GEH"] > 5)).sum())
    geh_pass_rate = None
    if int(geh_valid.sum()) > 0:
        geh_pass_rate = float((geh_valid & (d["GEH"] <= 5)).sum()) / float(geh_valid.sum())

    return {
        "n": int(len(d)),
        "matched_model": int((~miss_model).sum()),
        "missing_model": int(miss_model.sum()),
        "missing_obs": int(miss_obs.sum()),
        "both_missing": int((miss_model & miss_obs).sum()),
        "geh_gt5": geh_gt5,
        "geh_pass_rate": geh_pass_rate,
    }


def _styled_table(df: pd.DataFrame, flowtype: str):
    if df is None or df.empty:
        st.info("No rows.")
        return

    if flowtype == "TURN":
        cols = ["SiteName", "FromArm", "ToArm", "FlowName", "Obs_Flow", "Modelled_Flow", "Diff", "%Diff", "GEH"]
    elif flowtype == "ENTRY":
        cols = ["SiteName", "FromArm", "FlowName", "Obs_Flow", "Modelled_Flow", "Diff", "%Diff", "GEH"]
    else:
        cols = ["SiteName", "ToArm", "FlowName", "Obs_Flow", "Modelled_Flow", "Diff", "%Diff", "GEH"]

    cols = [c for c in cols if c in df.columns]
    out = _to_num(df, ["Obs_Flow", "Modelled_Flow", "Diff", "%Diff", "GEH"]).copy()

    for c, dp in [("Obs_Flow", 1), ("Modelled_Flow", 1), ("Diff", 1), ("%Diff", 1), ("GEH", 2)]:
        if c in out.columns:
            out[c] = out[c].round(dp)

    st.dataframe(out[cols].style.apply(geh_row_style, axis=1), use_container_width=True)


def _plots(df: pd.DataFrame):
    if df.empty:
        st.info("No data to plot.")
        return

    d = _to_num(df, ["Obs_Flow", "Modelled_Flow", "GEH"]).copy()
    d["FlowType"] = d.get("FlowType", "").astype(str).str.upper()
    d["Label"] = d.apply(_movement_label, axis=1)

    st.markdown("#### Observed vs Modelled")
    scat = d.dropna(subset=["Obs_Flow", "Modelled_Flow"]).copy()
    if scat.empty:
        st.info("No Obs/Model pairs available for scatter.")
    else:
        mn = float(min(scat["Obs_Flow"].min(), scat["Modelled_Flow"].min()))
        mx = float(max(scat["Obs_Flow"].max(), scat["Modelled_Flow"].max()))
        fig = px.scatter(scat, x="Obs_Flow", y="Modelled_Flow", color="FlowType", hover_name="Label")
        fig.add_trace(go.Scatter(x=[mn, mx], y=[mn, mx], mode="lines", line=dict(color="black", dash="dash"), name="y=x"))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### GEH distribution")
    geh = d.dropna(subset=["GEH"]).copy()
    if geh.empty:
        st.info("No GEH values available.")
    else:
        fig2 = px.histogram(geh, x="GEH", nbins=20)
        fig2.add_vline(x=5, line_dash="dash", line_color="black")
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("#### Top worst GEH")
    worst = d.dropna(subset=["GEH"]).sort_values("GEH", ascending=False).head(15).copy()
    if worst.empty:
        st.info("No GEH values available.")
    else:
        fig3 = px.bar(worst[::-1], x="GEH", y="Label", orientation="h")
        st.plotly_chart(fig3, use_container_width=True)


def render():
    st.subheader("Flow outputs (evaluation totals only)")

    per_class = st.session_state.get("flow_per_class")
    total_res = st.session_state.get("flow_total")

    if not per_class and not total_res:
        st.info("No Flow results yet. Run Flow in Controls.")
        return

    class_ids = list(per_class.keys()) if per_class else []
    labels = ["Class " + str(cid) for cid in class_ids]
    if total_res is not None:
        labels.append("TOTAL")

    tabs = st.tabs(labels) if labels else []

    for i, cid in enumerate(class_ids):
        with tabs[i]:
            res = per_class[cid]
            df_all = _combine(res)

            st.caption("Model column used: " + str(res.get("meta", {}).get("model_col", "")))
            st.caption("Eval bins used: " + str(res.get("meta", {}).get("bins", "")))

            diag = _diagnostics(df_all)
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Definitions", diag["n"])
            c2.metric("Matched model", diag["matched_model"])
            c3.metric("Missing model", diag["missing_model"])
            c4.metric("Missing observed", diag["missing_obs"])
            c5.metric("Both missing", diag["both_missing"])
            c6.metric("GEH>5", diag["geh_gt5"])
            if diag["geh_pass_rate"] is not None:
                st.caption("GEH pass rate (<=5): {:.1f}%".format(diag["geh_pass_rate"] * 100.0))

            with st.expander("Diagnostics details", expanded=False):
                d = _to_num(df_all, ["Obs_Flow", "Modelled_Flow", "GEH"]).copy()
                d["Label"] = d.apply(_movement_label, axis=1)
                miss_m = d[d["Modelled_Flow"].isna()].copy()
                miss_o = d[d["Obs_Flow"].isna()].copy()
                worst20 = d.dropna(subset=["GEH"]).sort_values("GEH", ascending=False).head(20)
                st.markdown("**Missing model (top 50)**")
                st.dataframe(miss_m.head(50), use_container_width=True) if not miss_m.empty else st.info("None")
                st.markdown("**Missing observed (top 50)**")
                st.dataframe(miss_o.head(50), use_container_width=True) if not miss_o.empty else st.info("None")
                st.markdown("**Worst GEH (top 20)**")
                st.dataframe(worst20, use_container_width=True) if not worst20.empty else st.info("None")

            st.divider()
            st.markdown("### Plots")
            _plots(df_all)

            st.divider()
            t1, t2, t3 = st.tabs(["TURN", "ENTRY", "EXIT"])
            with t1:
                _styled_table(res.get("TURN", pd.DataFrame()), "TURN")
            with t2:
                _styled_table(res.get("ENTRY", pd.DataFrame()), "ENTRY")
            with t3:
                _styled_table(res.get("EXIT", pd.DataFrame()), "EXIT")

    if total_res is not None:
        with tabs[-1]:
            df_all = _combine(total_res)
            diag = _diagnostics(df_all)
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Definitions", diag["n"])
            c2.metric("Matched model", diag["matched_model"])
            c3.metric("Missing model", diag["missing_model"])
            c4.metric("Missing observed", diag["missing_obs"])
            c5.metric("Both missing", diag["both_missing"])
            c6.metric("GEH>5", diag["geh_gt5"])
            if diag["geh_pass_rate"] is not None:
                st.caption("GEH pass rate (<=5): {:.1f}%".format(diag["geh_pass_rate"] * 100.0))
            st.divider()
            st.markdown("### Plots")
            _plots(df_all)
            st.divider()
            t1, t2, t3 = st.tabs(["TURN", "ENTRY", "EXIT"])
            with t1:
                _styled_table(total_res.get("TURN", pd.DataFrame()), "TURN")
            with t2:
                _styled_table(total_res.get("ENTRY", pd.DataFrame()), "ENTRY")
            with t3:
                _styled_table(total_res.get("EXIT", pd.DataFrame()), "EXIT")
