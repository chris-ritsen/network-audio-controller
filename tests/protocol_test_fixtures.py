import json
from functools import cache
from pathlib import Path


PROTOCOL_PACKETS_PATH = Path(__file__).parent / "fixtures" / "protocol_packets.json"


@cache
def _protocol_packets() -> dict[str, str]:
    return json.loads(PROTOCOL_PACKETS_PATH.read_text())


def load_protocol_packet(group: str, filename: str) -> bytes:
    return bytes.fromhex(_protocol_packets()[f"{group}/{filename}"])
