from __future__ import annotations
from pathlib import Path


def ensure_dir(p):
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def model_root(path_str):
    if not path_str:
        return None
    p = Path(path_str)
    if not p.exists():
        return None
    return p if p.is_dir() else p.parent
