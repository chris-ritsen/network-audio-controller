from __future__ import annotations

import json
from functools import cache
from pathlib import Path


MODERN_ARC_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "modern_arc_capture.json"


@cache
def modern_arc_fixture() -> dict:
    return json.loads(MODERN_ARC_FIXTURE_PATH.read_text())


def modern_arc_packets(*path: str) -> list[dict]:
    value = modern_arc_fixture()
    for component in path:
        value = value[component]
    return value


def modern_arc_payloads(*path: str, source_port: int | None = None) -> list[bytes]:
    packets = modern_arc_packets(*path)
    if source_port is not None:
        packets = [packet for packet in packets if packet["source_port"] == source_port]
    return [bytes.fromhex(packet["payload"]) for packet in packets]
