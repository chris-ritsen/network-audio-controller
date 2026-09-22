from __future__ import annotations

from functools import lru_cache

from netaudio import core


@lru_cache(maxsize=1)
def metering_scale() -> list[dict]:
    return core.metering_scale()


def _metering_value(value: int) -> dict:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < len(metering_scale()):
        raise ValueError("metering value must fit in one byte")

    return metering_scale()[value]


def metering_value_dbfs(value: int) -> float | None:
    return _metering_value(value)["dbfs"]


def classify_signal_presence(value: int) -> str:
    return _metering_value(value)["state"]


def parse_metering_levels(data: bytes) -> dict:
    parsed = core.parse_response("metering", data)
    return {
        "tx": {index: level for index, level in enumerate(parsed["tx_levels"], start=1)},
        "rx": {index: level for index, level in enumerate(parsed["rx_levels"], start=1)},
    }
