from __future__ import annotations

import re
from datetime import datetime, timedelta
import pandas as pd

TIME_RE = re.compile(r"^(?:[01]?\d|2[0-3]):[0-5]\d$")
TIME_RE_5 = re.compile(r"^(?:[01]?\d|2[0-3]):(?:00|05|10|15|20|25|30|35|40|45|50|55)$")
TIME_RE_15 = re.compile(r"^(?:[01]?\d|2[0-3]):(?:00|15|30|45)$")


def parse_run_spec(spec, max_n):
    spec = (spec or "").replace(" ", "")
    if not spec:
        return []
    out = set()
    for part in spec.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            if a.isdigit() and b.isdigit():
                a = int(a); b = int(b)
                lo, hi = min(a, b), max(a, b)
                for x in range(lo, hi + 1):
                    if 1 <= x <= max_n:
                        out.add(x)
        else:
            if part.isdigit():
                x = int(part)
                if 1 <= x <= max_n:
                    out.add(x)
    return sorted(out)


def parse_interval_start_secs(interval):
    s = str(interval).replace("–", "-").replace("—", "-")
    return int(s.split("-")[0].strip())


def add_clock(df, model_start_hhmm, interval_col):
    hh, mm = [int(x) for x in str(model_start_hhmm).split(":")]
    base = hh * 3600 + mm * 60
    out = df.copy()

    def to_clock(iv):
        t = (base + parse_interval_start_secs(iv)) % (24 * 3600)
        return "{:02d}:{:02d}".format(t // 3600, (t % 3600) // 60)

    out["ClockTime"] = out[interval_col].map(to_clock)
    return out


def build_bins(start_hhmm, duration_min, step_min):
    sdt = datetime.strptime(str(start_hhmm).strip(), "%H:%M")
    n = int(duration_min) // int(step_min)
    return [(sdt + timedelta(minutes=i * step_min)).strftime("%H:%M") for i in range(n)]
