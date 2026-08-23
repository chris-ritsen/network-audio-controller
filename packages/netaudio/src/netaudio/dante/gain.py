from __future__ import annotations

from collections.abc import Sequence


INPUT_GAIN_LEVEL_LABELS = {
    1: "+24 dBu",
    2: "+4 dBu",
    3: "0 dBu",
    4: "0 dBV",
    5: "-10 dBV",
}

OUTPUT_GAIN_LEVEL_LABELS = {
    1: "+18 dBu",
    2: "+4 dBu",
    3: "0 dBu",
    4: "0 dBV",
    5: "-10 dBV",
}

SUPPORTED_GAIN_LEVELS = (1, 2, 3, 4, 5)


def gain_level_labels(device_type: str) -> dict[int, str] | None:
    if device_type == "input":
        return INPUT_GAIN_LEVEL_LABELS
    if device_type == "output":
        return OUTPUT_GAIN_LEVEL_LABELS
    return None


def gain_level_label(device_type: str, gain_level: int) -> str:
    labels = gain_level_labels(device_type)
    if labels is None:
        return f"Unknown ({gain_level})"
    return labels.get(gain_level, f"Unknown ({gain_level})")


def gain_level_choices(device_type: str, supported_gain_levels: Sequence[int] | None) -> list[dict] | None:
    if supported_gain_levels is None:
        return None
    return [
        {"value": gain_level, "label": gain_level_label(device_type, gain_level)}
        for gain_level in supported_gain_levels
    ]


def gain_channel_type(device_type: str) -> str | None:
    if device_type == "input":
        return "tx"
    if device_type == "output":
        return "rx"
    return None
