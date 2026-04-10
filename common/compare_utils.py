from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd


def _to_hhmm(s: str) -> str:
    return (str(s or '').strip())


def _end_time(start_hhmm: str, dur_min: int) -> str:
    hh, mm = [int(x) for x in str(start_hhmm).split(':')]
    total = hh * 60 + mm + int(dur_min)
    end_hh = (total // 60) % 24
    end_mm = total % 60
    return f"{end_hh:02d}:{end_mm:02d}"


def validate_comparison_window(cfg_df: pd.DataFrame, scenarios: list[str]):
    """Validate EvalStartHHMM + EvalDurationMin are identical across scenarios.

    Returns dict:
      {
        ok: bool,
        error: str|None,
        warn_model_start: bool,
        summary_df: DataFrame,
        common: {eval_start, eval_dur, eval_end}|None
      }
    """
    if cfg_df is None or cfg_df.empty:
        return {"ok": False, "error": "Config sheet is empty", "warn_model_start": False, "summary_df": pd.DataFrame(), "common": None}

    sub = cfg_df[cfg_df['ScenarioName'].astype(str).isin([str(s) for s in scenarios])].copy()
    if sub.empty:
        return {"ok": False, "error": "No matching scenarios found in Config", "warn_model_start": False, "summary_df": pd.DataFrame(), "common": None}

    # normalize
    if 'EvalStartHHMM' not in sub.columns:
        sub['EvalStartHHMM'] = ''
    if 'EvalDurationMin' not in sub.columns:
        sub['EvalDurationMin'] = None
    if 'ModelStartHHMM' not in sub.columns:
        sub['ModelStartHHMM'] = ''

    sub['EvalStartHHMM'] = sub['EvalStartHHMM'].astype(str).str.strip()
    sub['ModelStartHHMM'] = sub['ModelStartHHMM'].astype(str).str.strip()
    sub['EvalDurationMin'] = pd.to_numeric(sub['EvalDurationMin'], errors='coerce')

    summary = sub[['ScenarioName','EvalStartHHMM','EvalDurationMin','ModelStartHHMM']].copy()

    eval_starts = [x for x in summary['EvalStartHHMM'].dropna().unique().tolist() if str(x).strip()]
    eval_durs = [int(x) for x in summary['EvalDurationMin'].dropna().unique().tolist()]

    if (len(eval_starts) != 1) or (len(eval_durs) != 1):
        return {
            "ok": False,
            "error": "Scenario Comparison requires identical EvalStartHHMM and EvalDurationMin across selected scenarios.",
            "warn_model_start": False,
            "summary_df": summary,
            "common": None,
        }

    common_start = str(eval_starts[0]).strip()
    common_dur = int(eval_durs[0])
    common_end = _end_time(common_start, common_dur)

    model_starts = [x for x in summary['ModelStartHHMM'].dropna().unique().tolist() if str(x).strip()]
    warn_model = len(model_starts) > 1

    return {
        "ok": True,
        "error": None,
        "warn_model_start": warn_model,
        "summary_df": summary,
        "common": {"eval_start": common_start, "eval_dur": common_dur, "eval_end": common_end},
    }
