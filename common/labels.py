from __future__ import annotations


def build_qc_label_maps(qc_id_to_name_raw: dict[str, str]):
    """Return (qc_id_to_label, qc_label_to_id, qc_labels).

    Label format:
      - '<ID> - <Name>' if name exists
      - '<ID> - (Unnamed)' if missing

    Also allow selecting plain ID in UI.
    """
    qc_id_to_label: dict[str, str] = {}
    qc_label_to_id: dict[str, str] = {}

    for qc_id, nm in (qc_id_to_name_raw or {}).items():
        qc_id = str(qc_id).strip()
        nm2 = (nm or '').strip()
        label = f"{qc_id} - {nm2}" if nm2 else f"{qc_id} - (Unnamed)"
        qc_id_to_label[qc_id] = label
        qc_label_to_id[label] = qc_id

    # also allow selecting plain ID
    for qc_id in qc_id_to_label.keys():
        qc_label_to_id[qc_id] = qc_id

    qc_labels = sorted(qc_label_to_id.keys())
    return qc_id_to_label, qc_label_to_id, qc_labels
