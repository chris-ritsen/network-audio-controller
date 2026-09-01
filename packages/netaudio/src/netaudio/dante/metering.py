from __future__ import annotations

import ipaddress
import logging
import socket


logger = logging.getLogger("netaudio")


def metering_value_dbfs(value: int) -> float | None:
    if not 0 <= value <= 0xFF:
        raise ValueError("metering value must fit in one byte")
    if value == 0x01:
        return 0.0
    if 0x02 <= value <= 0xFD:
        return -((value - 1) / 2)
    return None


def classify_signal_presence(value: int) -> str:
    if not 0 <= value <= 0xFF:
        raise ValueError("metering value must fit in one byte")
    if value == 0x00:
        return "clipping"
    if value <= 0x7B:
        return "signal_present"
    if value <= 0xFD:
        return "below_threshold"
    if value == 0xFE:
        return "muted"
    return "unknown"


def parse_metering_levels(data: bytes) -> dict:
    from netaudio import core

    parsed = core.parse_response("metering", data)
    return {
        "tx": {index: level for index, level in enumerate(parsed["tx_levels"], start=1)},
        "rx": {index: level for index, level in enumerate(parsed["rx_levels"], start=1)},
    }


def _get_local_ip() -> ipaddress.IPv4Address:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("224.0.0.231", 1))
        local_ip = sock.getsockname()[0]
    finally:
        sock.close()
    return ipaddress.IPv4Address(local_ip)
