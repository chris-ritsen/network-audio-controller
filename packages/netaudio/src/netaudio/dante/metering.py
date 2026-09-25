from __future__ import annotations

from functools import lru_cache

from netaudio import core
from netaudio.core import _types


@lru_cache(maxsize=1)
def metering_scale() -> _types.MeteringScales:
    return core.metering_scale()


def _metering_value(value: int, source: str | None) -> _types.MeteringValue:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 255:
        raise ValueError("metering value must fit in one byte")

    scales = metering_scale()
    if source == "detailed":
        return scales["detailed"][value]

    if source == "signal_presence":
        return scales["signal_presence"][value]

    return {"dbfs": None, "state": "unknown"}


def metering_value_dbfs(value: int, source: str | None) -> float | None:
    return _metering_value(value, source)["dbfs"]


def classify_signal_presence(value: int, source: str | None) -> str:
    return _metering_value(value, source)["state"]


def parse_metering_levels(data: bytes) -> dict:
    parsed = core.parse_response("metering", data)
    return {
        "tx": {index: level for index, level in enumerate(parsed["tx_levels"], start=1)},
        "rx": {index: level for index, level in enumerate(parsed["rx_levels"], start=1)},
    }
