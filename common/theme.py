# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from common.path_utils import ensure_dir


def write_theme_config(app_root: Path, theme_dict: dict) -> Path:
    """
    Writes Streamlit theme config at <app_root>/.streamlit/config.toml
    """
    app_root = Path(app_root)
    st_dir = app_root / ".streamlit"
    ensure_dir(st_dir)

    cfg_path = st_dir / "config.toml"

    lines = ["[theme]"]
    for k, v in (theme_dict or {}).items():
        if v is None:
            continue
        # Always write values as strings to keep TOML simple
        lines.append('{} = "{}"'.format(k, v))

    # IMPORTANT: keep "\n" on the same line (paste corruption breaks this)
    cfg_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return cfg_path