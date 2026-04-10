# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import streamlit as st


def list_all_att(outputs_dir: Path):
    """Recursive search for .att in nested Outputs folders."""
    if not outputs_dir.exists():
        return []
    files = list(outputs_dir.rglob("*.att"))
    # Newest first (useful for fallback cases)
    files.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return files


def _norm(s: str) -> str:
    return (s or "").strip()


def _cf(s: str) -> str:
    return _norm(s).casefold()


def _expected_names(project: str, scenario: str, keyword: str, simrun: str):
    """
    Return ordered list of exact filenames (case-insensitive match will be used).
    Highest priority first.
    """
    project = _norm(project)
    scenario = _norm(scenario)
    keyword = _norm(keyword)
    simrun = _norm(simrun)

    names = []

    # With simrun token (strict first)
    if simrun:
        if project:
            names.append(f"{project} - {scenario}_{keyword}_{simrun}.att")
        names.append(f"{scenario}_{keyword}_{simrun}.att")

    # Without simrun (fallback)
    if project:
        names.append(f"{project} - {scenario}_{keyword}.att")
    names.append(f"{scenario}_{keyword}.att")

    return names


def find_att_candidates(outputs_dir: Path, keyword: str, scenario: str, project: str, simruns_token: str):
    """
    Candidate list sorted such that:
      - if SIMRUN token provided: exact token file(s) first (nested search)
      - if token blank: exact Scenario_Keyword.att first
    """
    all_files = list_all_att(outputs_dir)
    if not all_files:
        return []

    keyword = _norm(keyword)
    scenario = _norm(scenario)
    project = _norm(project)
    token = _norm(simruns_token)

    expected = _expected_names(project, scenario, keyword, token)
    expected_cf = [_cf(x) for x in expected]

    # Build lookup for exact matches by name only (we still search nested folders)
    exact_hits = []
    for p in all_files:
        if _cf(p.name) in expected_cf:
            exact_hits.append(p)

    # Sort exact hits in the same priority order as expected list
    if exact_hits:
        order = {name_cf: i for i, name_cf in enumerate(expected_cf)}
        exact_hits.sort(key=lambda p: order.get(_cf(p.name), 999))
        return exact_hits

    # If exact not found, fallback to loose search
    # Loose = must include keyword and scenario in filename; token increases rank if present.
    scen_cf = _cf(scenario)
    key_cf = _cf(keyword)
    token_cf = _cf(token)

    def rank(ncf: str) -> int:
        score = 0
        # if token exists and filename contains it, boost
        if token_cf and (("_" + token_cf + ".att") in ncf or ("-" + token_cf + ".att") in ncf or token_cf in ncf):
            score += 50
        # prefer project hint
        if project and _cf(project) in ncf:
            score += 10
        # keyword/scenario already filtered; give small base
        score += 5
        return score

    matches = []
    for p in all_files:
        ncf = _cf(p.name)

        if key_cf and key_cf not in ncf:
            continue
        if scen_cf and scen_cf not in ncf:
            continue

        matches.append((rank(ncf), p.stat().st_mtime if p.exists() else 0.0, p))

    matches.sort(key=lambda x: (x[0], x[1]), reverse=True)

    uniq = {}
    for _, __, p in matches:
        uniq[str(p.resolve())] = p
    return list(uniq.values())


def choose_att_dropdown(
    label: str,
    outputs_dir: Path,
    keywords: list[str],
    scenario: str,
    project: str,
    simruns_token: str,
    manual_key: str,
    pick_key: str,
    debug: bool,
):
    """
    UI helper:
      - optional manual override
      - finds candidates recursively
      - if SIMRUN token is provided: exact token file is preferred
      - if token blank: exact Scenario_Keyword.att is preferred
    """
    manual = st.text_input(
        "Manual {} ATT path (optional)".format(label),
        value=st.session_state.get(manual_key, "") or "",
        key=manual_key,
    )

    if manual.strip():
        mp = Path(manual.strip())
        if mp.exists():
            return mp
        st.warning("Manual path does not exist. Ignoring manual override.")

    # Build candidate list, keyword-by-keyword
    cands = []
    for kw in keywords:
        cands.extend(find_att_candidates(outputs_dir, kw, scenario, project, simruns_token))

    # De-dup while preserving order
    seen = set()
    ordered = []
    for p in cands:
        rp = str(p.resolve())
        if rp in seen:
            continue
        seen.add(rp)
        ordered.append(p)

    if debug:
        with st.expander("🔎 Debug: {} ATT search".format(label), expanded=False):
            st.write("Outputs dir:", str(outputs_dir))
            st.write("Scenario:", scenario)
            st.write("Project:", project)
            st.write("SIMRUN token:", simruns_token)
            st.write("Keywords:", keywords)
            st.write("Candidates found:", len(ordered))
            st.write([p.name for p in ordered[:20]])

    if not ordered:
        st.warning("No {} ATT candidates found. Use manual path.".format(label))
        return None

    chosen = st.selectbox(
        "{} ATT file".format(label),
        options=[str(p) for p in ordered],
        index=0,
        key=pick_key,
    )
    return Path(chosen)