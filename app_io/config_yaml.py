from __future__ import annotations
from pathlib import Path
from datetime import datetime
import yaml

from common.path_utils import ensure_dir


def cfg_file(root):
    root = Path(root)
    return root / "_InData" / "_config" / "BaseYear_Config.yaml"


def load_cfg(root):
    f = cfg_file(root)
    if f.exists():
        try:
            return yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        except Exception:
            return {}
    return {}


def save_cfg(root, project_name, jt_ids, flow_ids, setup_rows, active_workbook=None):
    f = cfg_file(root)
    ensure_dir(f.parent)
    data = {
        "project": {"project_name": project_name},
        "model": {"model_root": str(Path(root))},
        "classes": {"jt": jt_ids, "flow": flow_ids},
        "setup_rows": setup_rows,
        "workbook": {"active": active_workbook} if active_workbook else {},
        "meta": {"app_version": "v5-split", "saved_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
    }
    f.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
