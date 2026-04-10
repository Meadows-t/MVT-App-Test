from __future__ import annotations

from pathlib import Path
import streamlit as st
import pandas as pd

from unified_inpx_tools import parse_inpx

from common.path_utils import model_root
from common.time_utils import TIME_RE, TIME_RE_15, parse_run_spec

from app_io.config_yaml import load_cfg, save_cfg
from app_io.workbooks import (
    find_existing_workbooks,
    timestamped_inputs_path,
    load_workbook_from_path,
    generate_inputs_workbook,
)
from app_io.discovery import choose_att_dropdown

from core.queue_core import run_queue
from core.jt_core import run_jt
from core.flow_core import compute_flow_for_class, compute_flow_total_T1

# ✅ Export (NEW)
from app_io.export_results import default_export_path, export_all_to_excel


def _safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default


def render():
    st.subheader("Common inputs")

    model_path = st.text_input(
        "Model path (folder recommended)",
        value=st.session_state.get("model_path", ""),
        key="model_path",
    )
    root = model_root(model_path)
    if not root:
        st.info("Paste model folder path.")
        st.stop()

    cfg_yaml = load_cfg(root)
    proj_default = (cfg_yaml.get("project", {}) or {}).get("project_name", "")
    active_wb_cfg = (cfg_yaml.get("workbook", {}) or {}).get("active", "")

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        project_name = st.text_input(
            "Project Name",
            value=st.session_state.get("project_name") or proj_default,
            key="project_name",
        )
    with c2:
        simruns_token = st.text_input(
            "SIMRUNS token (optional)",
            value=st.session_state.get("simruns_token", "010"),
            key="simruns_token",
        )
    with c3:
        debug = st.checkbox(
            "Debug file search",
            value=bool(st.session_state.get("debug", False)),
            key="debug",
        )

    outputs_dir = root / "Outputs"

    st.divider()
    st.subheader("Inputs workbook (Mode B: timestamped, local time)")

    existing = find_existing_workbooks(root)
    latest = existing[0] if existing else None

    if active_wb_cfg and Path(active_wb_cfg).exists():
        default_choice = Path(active_wb_cfg)
    elif latest:
        default_choice = latest
    else:
        default_choice = None

    wb_mode = st.radio(
        "Workbook source",
        ["Auto-select latest", "Pick existing", "Generate new (timestamped)"],
        horizontal=True,
    )

    if wb_mode == "Auto-select latest":
        chosen_wb = default_choice
    elif wb_mode == "Pick existing":
        opts = [str(p) for p in existing]
        pick = st.selectbox("Existing workbooks in _InData", options=opts, index=0) if opts else ""
        chosen_wb = Path(pick) if pick else None
    else:
        chosen_wb = None

    if chosen_wb is None or not chosen_wb.exists():
        st.warning("No inputs workbook selected yet. Generate one below.")

    with st.expander(
        "Generate Inputs workbook (timestamped)",
        expanded=(wb_mode == "Generate new (timestamped)"),
    ):
        inpx_hits = list(root.glob("*.inpx"))
        if not inpx_hits:
            st.error("No INPX found in model folder.")
            st.stop()

        info = parse_inpx(str(inpx_hits[0]))
        vc_ids = [str(x.get("no")).strip() for x in info.get("vehicle_classes", []) if x.get("no")]
        vc_map = {str(x.get("no")).strip(): str(x.get("name")).strip()
                  for x in info.get("vehicle_classes", []) if x.get("no")}

        def fmt(cid):
            return f"{cid} – {vc_map.get(cid, '')}" if vc_map.get(cid, "") else cid

        jt_sel = st.selectbox("JT class (exactly 1)", options=vc_ids, index=0 if vc_ids else None, format_func=fmt)
        flow_sel = st.multiselect("Flow classes (max 5)", options=vc_ids, default=vc_ids[:5], format_func=fmt)

        setup_df = st.data_editor(
            pd.DataFrame({
                "Scenario Name": [""],
                "Preferred Name": [""],
                "Peak": [""],
                "Model start time": ["07:00"],
                "Model duration (min)": [60],
                "Eval start time": ["07:00"],
                "Eval duration (min)": [60],
            }),
            num_rows="dynamic",
            use_container_width=True,
        )

        st.caption("Eval start must be 15-min aligned (HH:00/15/30/45) and duration multiple of 15 (JT/Flow).")

        if st.button("Generate Inputs Workbook Now", type="primary"):
            if not project_name.strip():
                st.error("Project Name is required")
                st.stop()
            if not jt_sel:
                st.error("Select exactly 1 JT class")
                st.stop()
            if len(flow_sel) == 0 or len(flow_sel) > 5:
                st.error("Flow classes must be 1..5")
                st.stop()

            rows = []
            for _, r in setup_df.iterrows():
                scen = str(r.get("Scenario Name", "") or "").strip()
                pref = str(r.get("Preferred Name", "") or "").strip()
                peak = str(r.get("Peak", "") or "").strip()
                ms = str(r.get("Model start time", "") or "").strip()
                md = _safe_int(r.get("Model duration (min)"), None)
                es = str(r.get("Eval start time", "") or "").strip()
                ed = _safe_int(r.get("Eval duration (min)"), None)

                if not scen:
                    continue
                if not TIME_RE.match(ms):
                    st.error(f"Invalid model start time: {ms}")
                    st.stop()
                if md is None or md <= 0:
                    st.error("Invalid model duration")
                    st.stop()

                if not TIME_RE.match(es) or not TIME_RE_15.match(es):
                    st.error(f"Eval start must be 15-min aligned (HH:00/15/30/45). Got: {es}")
                    st.stop()
                if ed is None or ed <= 0 or ed % 15 != 0:
                    st.error(f"Eval duration must be positive and multiple of 15. Got: {ed}")
                    st.stop()

                rows.append({
                    "scenario_name": scen,
                    "preferred_name": pref,
                    "peak": peak,
                    "model_start_hhmm": ms,
                    "model_duration_min": md,
                    "eval_start_hhmm": es,
                    "eval_duration_min": ed,
                })

            if not rows:
                st.error("Add at least one scenario row")
                st.stop()

            out_path = timestamped_inputs_path(root)
            generate_inputs_workbook(root, project_name.strip(), rows, [str(jt_sel)], list(flow_sel), out_path)
            save_cfg(root, project_name.strip(), [str(jt_sel)], list(flow_sel), rows, active_workbook=str(out_path))
            st.session_state["active_workbook"] = str(out_path)
            st.success(f"Workbook generated: {out_path.name}")
            st.rerun()

    if chosen_wb is None:
        awb = st.session_state.get("active_workbook")
        chosen_wb = Path(awb) if awb and Path(awb).exists() else default_choice

    if not chosen_wb or not chosen_wb.exists():
        st.info("Generate/pick workbook to enable scenario selection and outputs.")
        st.stop()

    xlsx, cfg_df, classes_df = load_workbook_from_path(chosen_wb)
    scen_list = cfg_df["ScenarioName"].tolist()
    scenario = st.selectbox("Scenario", scen_list, index=0 if scen_list else None, key="scenario")
    row = cfg_df[cfg_df["ScenarioName"] == str(scenario)].iloc[0]

    model_start = str(row.get("ModelStartHHMM", "") or "").strip()
    eval_start = str(row.get("EvalStartHHMM", model_start) or model_start).strip()
    eval_dur = _safe_int(row.get("EvalDurationMin", row.get("ModelDurationMin", 60)), 60)

    # derive classes from Classes sheet
    jt_class_ids = []
    flow_class_ids = []
    if not classes_df.empty and "Role" in classes_df.columns and "ClassID" in classes_df.columns:
        jt_class_ids = classes_df.loc[classes_df["Role"].astype(str).str.upper() == "JT", "ClassID"].astype(str).tolist()
        flow_class_ids = classes_df.loc[classes_df["Role"].astype(str).str.upper() == "FLOW", "ClassID"].astype(str).tolist()[:5]

    jt_class_id = jt_class_ids[0] if jt_class_ids else None

    st.caption(f"Active workbook: {chosen_wb.name}")

    with st.expander("📌 Scenario summary (from workbook)", expanded=True):
        st.write(f"**ScenarioName**: {scenario}")
        st.write(f"**Model Start**: {model_start}")
        st.write(f"**Evaluation Start**: {eval_start}")
        st.write(f"**Evaluation Duration (min)**: {eval_dur}")
        st.write(f"**JT Class**: {jt_class_id if jt_class_id else 'NOT SET'}")
        st.write(f"**Flow Classes**: {flow_class_ids if flow_class_ids else 'NOT SET'}")

    st.divider()
    st.subheader("ATT selection")

    q_att = choose_att_dropdown(
        "Queue",
        outputs_dir,
        ["Queue Results"],
        str(scenario),
        str(project_name),
        str(simruns_token),
        "q_manual_att",
        "q_pick_att",
        debug,
    )
    jt_att = choose_att_dropdown(
        "JT",
        outputs_dir,
        ["Vehicle Travel Time Results", "Travel Time Results"],
        str(scenario),
        str(project_name),
        str(simruns_token),
        "jt_manual_att",
        "jt_pick_att",
        debug,
    )
    mov_att = choose_att_dropdown(
        "Movement Evaluation",
        outputs_dir,
        ["Movement", "Movement Evaluation", "Node Results"],
        str(scenario),
        str(project_name),
        str(simruns_token),
        "mov_manual_att",
        "mov_pick_att",
        debug,
    )

    # ✅ EXPORT BUTTON PLACED HERE (after ATT selection)
    st.divider()
    st.subheader("Export Results (Excel)")
    st.caption("Queue & JT export = FULL modelled duration (full ATT). Flow export = Option 1 AVG only.")

    if st.button("Export Queue + JT + Flow (Excel)"):
        try:
            out_path = default_export_path(root, str(scenario))
            export_all_to_excel(
                export_path=out_path,
                model_root=root,
                workbook=chosen_wb,
                scenario=str(scenario),
                preferred=str(row.get("PreferredName", "")),
                peak=str(row.get("Peak", "")),
                model_start=str(row.get("ModelStartHHMM", "")),
                eval_start=str(row.get("EvalStartHHMM", model_start) or model_start),
                eval_dur=int(eval_dur),
                queue_att=q_att,
                jt_att=jt_att,
                mov_att=mov_att,
                jt_class_id=jt_class_id,
                flow_class_ids=flow_class_ids,
            )
            st.success("Exported: {}".format(out_path))
        except Exception as e:
            st.error("Export failed: {}".format(e))

    st.divider()
    st.subheader("Runs")

    run_mode = st.radio("Runs mode", ["All runs", "Select runs"], horizontal=True, key="run_mode")
    run_spec = st.text_input("Run selection spec", value=st.session_state.get("run_spec", "1-10"), key="run_spec")

    selected_runs = []
    try:
        N = int(str(simruns_token).strip()) if str(simruns_token).strip() else 0
    except Exception:
        N = 0

    if run_mode == "Select runs" and N > 0:
        selected_runs = st.multiselect(
            "Select run numbers",
            options=list(range(1, N + 1)),
            default=parse_run_spec(run_spec, N),
            key="selected_runs",
        )

    # INPX mappings
    inpx_hits = list(root.glob("*.inpx"))
    info = parse_inpx(str(inpx_hits[0])) if inpx_hits else {}
    qc_id_to_name = {str(x.get("no")).strip(): str(x.get("name")).strip()
                     for x in info.get("queue_counters", []) if x.get("no") and x.get("name")}
    qc_name_to_id = {v: k for k, v in qc_id_to_name.items()}
    vttm_id_to_name = {str(x.get("no")).strip(): str(x.get("name")).strip()
                       for x in info.get("jt_measurements", []) if x.get("no") and x.get("name")}

    st.divider()
    st.subheader("Queue controls")
    obs_q = pd.read_excel(chosen_wb, sheet_name="Queue", engine="openpyxl")
    queue_cols = [c for c in obs_q.columns if c not in ("PreferredName", "Peak", "ClockTime")]
    q_counters = st.multiselect("Queue counters", options=queue_cols, default=queue_cols[:1], key="q_counters")

    st.divider()
    st.subheader("JT controls")
    grp_df = pd.read_excel(chosen_wb, sheet_name="JT_Grouping", engine="openpyxl")
    grp_df["Group_ID"] = grp_df.get("Group_ID", "").astype(str).str.strip()
    grp_df = grp_df[grp_df["Group_ID"] != ""]
    groups_list = sorted(grp_df["Group_ID"].unique().tolist())
    jt_groups_sel = st.multiselect(
        "Group_ID(s) to plot",
        options=groups_list,
        default=groups_list[:1] if groups_list else [],
        key="jt_groups",
    )

    st.divider()
    st.subheader("Flow controls")
    st.caption("Flow uses EvalStart/EvalDuration and classes from workbook. Observed flows are wide: <ScenarioName>__Obs_<ClassID>.")

    st.divider()
    b1, b2, b3, b4 = st.columns([1, 1, 1, 1])
    with b1:
        run_queue_btn = st.button("Run Queue", type="primary")
    with b2:
        run_jt_btn = st.button("Run JT", type="primary")
    with b3:
        run_flow_btn = st.button("Run Flow (per class)", type="primary")
    with b4:
        run_flow_total_btn = st.button("Run Flow (TOTAL)", type="primary")

    if run_queue_btn:
        if not q_att:
            st.error("Queue ATT not selected")
            st.stop()
        res = run_queue(
            chosen_wb,
            str(scenario),
            q_att,
            str(simruns_token),
            run_mode,
            selected_runs,
            str(eval_start),
            int(eval_dur),
            q_counters,
            False,
            "#E31A1C",
            "#000000",
            "#808080",
            "#A0A0A0",
            qc_id_to_name,
            qc_name_to_id,
        )
        st.session_state["queue_result"] = res
        st.success("Queue updated. Open Queue outputs tab.")

    if run_jt_btn:
        if not jt_att:
            st.error("JT ATT not selected")
            st.stop()
        if not jt_class_id:
            st.error("JT Class not found in workbook Classes sheet (Role=JT).")
            st.stop()
        try:
            res = run_jt(
                chosen_wb,
                str(scenario),
                jt_att,
                str(simruns_token),
                run_mode,
                selected_runs,
                str(eval_start),
                int(eval_dur),
                [str(jt_class_id)],
                jt_groups_sel,
                vttm_id_to_name,
            )
            st.session_state["jt_result"] = res
            st.success("JT updated. Open JT outputs tab.")
        except Exception as e:
            st.error(f"JT failed: {e}")

    if run_flow_btn:
        if not mov_att:
            st.error("Movement Evaluation ATT not selected")
            st.stop()
        if not flow_class_ids:
            st.error("Flow classes not found in workbook Classes sheet (Role=Flow).")
            st.stop()

        per = {}
        try:
            for cid in flow_class_ids:
                per[str(cid)] = compute_flow_for_class(
                    chosen_wb,
                    str(scenario),
                    mov_att,
                    model_start,
                    str(eval_start),
                    int(eval_dur),
                    str(cid),
                    run_mode,
                    selected_runs,
                )
            st.session_state["flow_per_class"] = per
            st.success("Flow per-class updated. Open Flow outputs tab.")
        except Exception as e:
            st.error(f"Flow failed: {e}")

    if run_flow_total_btn:
        if not mov_att:
            st.error("Movement Evaluation ATT not selected")
            st.stop()
        if not flow_class_ids:
            st.error("Flow classes not found in workbook Classes sheet (Role=Flow).")
            st.stop()

        try:
            total = compute_flow_total_T1(
                chosen_wb,
                str(scenario),
                mov_att,
                model_start,
                str(eval_start),
                int(eval_dur),
                list(map(str, flow_class_ids)),
                run_mode,
                selected_runs,
            )
            st.session_state["flow_total"] = total
            st.success("Flow TOTAL updated. Open Flow outputs tab.")
        except Exception as e:
            st.error(f"Flow TOTAL failed: {e}")