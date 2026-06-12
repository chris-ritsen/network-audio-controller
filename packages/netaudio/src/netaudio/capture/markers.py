from __future__ import annotations

import re


STANDARD_MARKER_TYPES = (
    "action", "observation", "state_change", "system", "hypothesis",
    "evidence", "analysis", "bug", "bug:fix", "code_change",
    "code_change:fix", "code_change:feat", "code_change:refactor",
)
MARKER_TYPE_ALIASES = {
    "action": "action",
    "observation": "observation",
    "observe": "observation",
    "state": "state_change",
    "state_change": "state_change",
    "state-change": "state_change",
    "system": "system",
    "hypothesis": "hypothesis",
    "inference": "hypothesis",
    "evidence": "evidence",
    "analysis": "analysis",
    "analyze": "analysis",
    "note": "observation",
    "start": "action",
    "end": "state_change",
    "capture": "system",
    "session": "system",
    "bug": "bug",
    "bug:fix": "bug:fix",
    "bugfix": "bug:fix",
    "code_change": "code_change",
    "code-change": "code_change",
    "code_change:fix": "code_change:fix",
    "code_change:feat": "code_change:feat",
    "code_change:refactor": "code_change:refactor",
}
MARKER_LABEL_SANITIZE = re.compile(r"[^a-z0-9]+")


def normalize_marker_type(marker_type: str | None, *, strict: bool = True) -> str:
    token = (marker_type or "").strip().lower().replace(" ", "_")
    normalized = MARKER_TYPE_ALIASES.get(token)
    if normalized:
        return normalized
    if strict:
        allowed = ", ".join(STANDARD_MARKER_TYPES)
        raise ValueError(f"Invalid --type {marker_type!r}. Use one of: {allowed}.")
    return "observation"


def normalize_marker_label(label: str) -> str:
    token = (label or "").strip().lower()
    token = MARKER_LABEL_SANITIZE.sub("_", token).strip("_")
    if not token:
        raise ValueError("Marker label is empty after normalization.")
    return token
