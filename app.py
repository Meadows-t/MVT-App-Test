# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import streamlit as st

from common.theme import write_theme_config
from modules import controls_tab, queue_tab, jt_tab, flow_tab

st.set_page_config(page_title="VISSIM BaseYear Dashboard", layout="wide")

_logo = Path(__file__).parent / "assets" / "logo.png"
if _logo.exists():
    st.sidebar.image(str(_logo), use_container_width=True)

st.sidebar.subheader("🎨 Theme")
theme_choice = st.sidebar.selectbox("Theme preset", ["Light (default)", "Dark", "Custom (colors below)"])

app_root = Path(__file__).resolve().parent

if theme_choice.startswith("Light"):
    preset = dict(primaryColor="#145A96", backgroundColor="#FFFFFF", secondaryBackgroundColor="#F3F6FA", textColor="#111111", font="sans serif")
elif theme_choice.startswith("Dark"):
    preset = dict(primaryColor="#4EA1FF", backgroundColor="#0E1117", secondaryBackgroundColor="#161B22", textColor="#FAFAFA", font="sans serif")
else:
    preset = {
        "primaryColor": st.sidebar.color_picker("primaryColor", "#145A96"),
        "backgroundColor": st.sidebar.color_picker("backgroundColor", "#FFFFFF"),
        "secondaryBackgroundColor": st.sidebar.color_picker("secondaryBackgroundColor", "#F3F6FA"),
        "textColor": st.sidebar.color_picker("textColor", "#111111"),
        "font": st.sidebar.selectbox("font", ["sans serif", "serif", "monospace"]),
    }

if st.sidebar.button("Apply theme (writes config.toml)"):
    cfgp = write_theme_config(app_root, preset)
    st.sidebar.success("Wrote {}. Restart Streamlit to apply.".format(cfgp))

st.sidebar.caption("Theme changes require Streamlit restart.")

st.title("🚦 VISSIM BaseYear Dashboard")

for k in ["queue_result", "jt_result", "flow_per_class", "flow_total", "active_workbook"]:
    st.session_state.setdefault(k, None)

_tab_controls, _tab_queue, _tab_jt, _tab_flow = st.tabs([
    "⚙️ Controls", "📊 Queue outputs", "⏱️ JT outputs", "🧮 Flow outputs"
])

with _tab_controls:
    controls_tab.render()
with _tab_queue:
    queue_tab.render()
with _tab_jt:
    jt_tab.render()
with _tab_flow:
    flow_tab.render()
