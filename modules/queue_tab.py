from __future__ import annotations
import streamlit as st


def render():
    st.subheader("Queue outputs")
    res = st.session_state.get("queue_result")
    if not res:
        st.info("No queue results yet. Run Queue in Controls.")
        return
    st.plotly_chart(res["fig"], use_container_width=True)
    a, b = st.columns(2)
    with a:
        st.markdown("### Mean Max Queue")
        st.dataframe(res["mean_tbl"], use_container_width=True)
    with b:
        st.markdown("### Max Max Queue")
        st.dataframe(res["max_tbl"], use_container_width=True)
