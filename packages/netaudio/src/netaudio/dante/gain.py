from __future__ import annotations

from collections.abc import Sequence
from functools import lru_cache
import time

from netaudio import core


@lru_cache(maxsize=1)
def _gain_metadata():
    return core.gain_metadata()


def codec_status_fields(codec_status: dict) -> dict:
    adapter = codec_status.get("gain_adapter")
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


def gain_level_label(device_type: str, gain_level: int) -> str:
    metadata = _gain_metadata().get(device_type, {})

    return next((entry["label"] for entry in metadata.get("levels", []) if entry["value"] == gain_level), "Unknown")


def gain_level_choices(device_type: str, supported_gain_levels: Sequence[int] | None) -> list[dict] | None:
    if supported_gain_levels is None:
        return None
    return [
        {"value": gain_level, "label": gain_level_label(device_type, gain_level)}
        for gain_level in supported_gain_levels
    ]


def gain_channel_type(device_type: str) -> str | None:
    return _gain_metadata().get(device_type, {}).get("channel_type")
