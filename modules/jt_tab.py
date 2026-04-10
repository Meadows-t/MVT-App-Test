from __future__ import annotations
import streamlit as st
import pandas as pd


def _fail_style(row: pd.Series):
    try:
        if ("Pass" in row) and (row["Pass"] is False):
            return ["background-color: #f8d7da"] * len(row)
    except Exception:
        pass
    return [""] * len(row)


def render():
    st.subheader("JT outputs")
    res = st.session_state.get("jt_result")
    if not res:
        st.info("No JT results yet. Run JT in Controls.")
        return

    tabs = res.get("tabs", [])
    if not tabs:
        st.warning("No JT outputs produced (check selections).")
        return

    jt_tabs = st.tabs(["Class " + str(t.get("class")) for t in tabs])
    for tb, t in zip(jt_tabs, tabs):
        with tb:
            st.markdown("### Segment comparison")
            st.dataframe(t["seg_tbl"].style.apply(_fail_style, axis=1), use_container_width=True)

            st.markdown("### Group comparison")
            st.dataframe(t["grp_tbl"].style.apply(_fail_style, axis=1), use_container_width=True)

            st.markdown("### Time–distance plots")
            st.plotly_chart(t["fig"], use_container_width=True)
