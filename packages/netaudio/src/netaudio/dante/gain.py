from __future__ import annotations

from collections.abc import Sequence
import time


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


def gain_adapter_from_codec_status(device, codec_status: dict) -> dict | None:
    recognized = {(1, 2): "input", (2, 1): "output"}
    parameters = codec_status.get("parameters") or []
    matches = [p for p in parameters if (p.get("parameter_type"), p.get("mode")) in recognized]
    # Conflicting modes for an analog parameter make the projection ambiguous.
    if len(matches) != 1 or any(
        p.get("parameter_type") in (1, 2) and (p.get("parameter_type"), p.get("mode")) not in recognized
        for p in parameters
    ):
        return None
    parameter = matches[0]
    values = parameter.get("values")
    if (
        not isinstance(values, list)
        or not 1 <= len(values) <= 2
        or any(isinstance(v, bool) or not isinstance(v, int) or v not in SUPPORTED_GAIN_LEVELS for v in values)
    ):
        return None
    return {
        "parameter_type": parameter["parameter_type"],
        "mode": parameter["mode"],
        "device_type": recognized[(parameter["parameter_type"], parameter["mode"])],
        "channel_levels": list(values),
        "supported_levels": list(SUPPORTED_GAIN_LEVELS),
    }


def codec_status_fields(device, codec_status: dict) -> dict:
    adapter = gain_adapter_from_codec_status(device, codec_status)
    fields = {
        "codec_status": codec_status,
        "codec_observed_at": time.time(),
        "codec_parameters": codec_status.get("parameters") or [],
        "gain_adapter": adapter,
        "gain_device_type": None,
        "gain_levels": None,
        "supported_gain_levels": None,
    }
    if adapter is not None:
        fields.update(
            gain_device_type=adapter["device_type"],
            gain_levels=adapter["channel_levels"],
            supported_gain_levels=adapter["supported_levels"],
        )
    return fields


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
