# -*- coding: utf-8 -*-
"""Best-effort VISSIM INPX/XML parser for:
- vehicle classes
- queue counters
- vehicle travel time measurements
"""

from __future__ import annotations
import io
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET


def _norm(s):
    return (s or "").strip()


def _low(d):
    return {str(k).lower(): v for k, v in (d or {}).items()}


def _is_zip(path):
    try:
        with Path(path).open("rb") as f:
            return f.read(2) == b"PK"
    except Exception:
        return False


def _scan_xml_bytes(data, out):
    try:
        for _, el in ET.iterparse(io.BytesIO(data), events=("start",)):
            tag = str(el.tag).lower()
            at = _low(el.attrib)
            name = _norm(at.get("name") or at.get("desc") or at.get("description"))
            no = _norm(at.get("no") or at.get("id") or at.get("index"))

            if "vehicleclass" in tag:
                out.setdefault("vehicle_classes", []).append({"no": no, "name": name})
            if "queuecounter" in tag:
                out.setdefault("queue_counters", []).append({"no": no, "name": name})
            if "vehicletraveltimemeasurement" in tag:
                out.setdefault("jt_measurements", []).append({"no": no, "name": name})
    except ET.ParseError:
        return


def parse_inpx(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    out = {}
    if p.suffix.lower() in (".inpx", ".zip") and _is_zip(p):
        with zipfile.ZipFile(p, "r") as z:
            for info in z.infolist():
                if info.filename.lower().endswith(".xml"):
                    with z.open(info, "r") as f:
                        _scan_xml_bytes(f.read(), out)
    else:
        _scan_xml_bytes(p.read_bytes(), out)

    for k in ("vehicle_classes", "queue_counters", "jt_measurements"):
        seen = set()
        ded = []
        for it in out.get(k, []):
            key = (it.get("no", ""), it.get("name", ""))
            if key in seen:
                continue
            seen.add(key)
            ded.append(it)
        out[k] = ded

    return out
