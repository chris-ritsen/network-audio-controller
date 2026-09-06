import hashlib
import json
from pathlib import Path

import pytest

from netaudio import core

EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "subscription_2809.json").read_text())


@pytest.mark.parametrize("exchange", EVIDENCE["exchanges"])
def test_audio_subscription_page_matches_controller_capture(exchange):
    for direction in ("request", "response"):
        record = exchange[direction]
        assert hashlib.sha256(bytes.fromhex(record["hexadecimal"])).hexdigest() == record["sha256"]
    packet = core.build_command({
        "command": "modern_arc_subscription_page",
        "protocol_id": 0x2809,
        "page_capacity": exchange["page_capacity"],
        "media_type_code": 3,
        "records": exchange["records"],
        "transaction_id": exchange["transaction_id"],
    })
    assert packet.hex() == exchange["request"]["hexadecimal"]
    assert core.parse_response("result_code", bytes.fromhex(exchange["response"]["hexadecimal"])) == 1


@pytest.mark.parametrize("protocol,media_type", [(0x2809, 4), (0x2801, 3), (0xFFFF, 3)])
def test_audio_subscription_page_rejects_unverified_protocol_media_combinations(protocol, media_type):
    with pytest.raises(core.NetaudioCoreError):
        core.build_command({
            "command": "modern_arc_subscription_page", "protocol_id": protocol,
            "page_capacity": 1, "media_type_code": media_type,
            "records": [{"action": "clear", "rx_channel": 1}],
        })


def test_audio_subscription_page_rejects_unverified_same_device_shorthand():
    with pytest.raises(core.NetaudioCoreError):
        core.build_command({
            "command": "modern_arc_subscription_page", "protocol_id": 0x2809,
            "page_capacity": 1, "media_type_code": 3,
            "records": [{"action": "set", "rx_channel": 1, "tx_channel": "01", "tx_device": "."}],
        })
