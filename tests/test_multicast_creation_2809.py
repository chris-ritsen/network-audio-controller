import hashlib
import json
from pathlib import Path

import pytest

from netaudio import core


EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "multicast_creation_2809.json").read_text())
AES3_EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "multicast_creation_2809_aes3.json").read_text())
ALLOCATION_EVIDENCE = json.loads(
    (Path(__file__).parent / "fixtures" / "multicast_allocation_2809_aes3.json").read_text()
)


@pytest.mark.parametrize(
    "exchange", EVIDENCE["exchanges"] + AES3_EVIDENCE["exchanges"] + ALLOCATION_EVIDENCE["exchanges"]
)
def test_multicast_creation_matches_digest_bound_exchange(exchange):
    for direction in ("request", "response"):
        record = exchange[direction]
        assert hashlib.sha256(bytes.fromhex(record["hexadecimal"])).hexdigest() == record["sha256"]

    request = core.build_command(
        {
            "command": "create_multicast_flow_2809",
            "channels": exchange["channels"],
            "media_local_flow_id": 2,
            "transport": "native",
            "request_options_word": exchange["request_options_word"],
            "transaction_id": exchange["transaction_id"],
        }
    )
    assert request.hex() == exchange["request"]["hexadecimal"]
    result = core.parse_response("multicast_flow_creation_2809", bytes.fromhex(exchange["response"]["hexadecimal"]))
    assert result == {
        "global_flow_id": 2,
        "media_type_code": 3,
        "media_local_flow_id": 2,
        "channels": exchange["channels"],
    }


@pytest.mark.parametrize("offset,value", [(0, 0x27), (6, 0x36), (9, 0), (18, 1), (35, 0), (41, 0), (47, 0)])
def test_multicast_creation_rejects_unverified_response_variants(offset, value):
    response = bytearray.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])
    response[offset] = value
    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("multicast_flow_creation_2809", bytes(response))


def test_multicast_creation_rejects_every_truncation():
    response = bytes.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])
    for length in range(len(response)):
        with pytest.raises(core.NetaudioCoreError):
            core.parse_response("multicast_flow_creation_2809", response[:length])


def test_multicast_creation_accepts_an_explicit_nonzero_media_local_identity():
    response = bytearray.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])
    response[40:42] = (7).to_bytes(2, "big")

    result = core.parse_response("multicast_flow_creation_2809", bytes(response))

    assert result["global_flow_id"] == 2
    assert result["media_local_flow_id"] == 7


def test_multicast_creation_requires_explicit_identity_transport_and_options():
    with pytest.raises(core.NetaudioCoreError):
        core.build_command({"command": "create_multicast_flow_2809", "channels": [1]})


def test_multicast_creation_rejects_ipv6_destination():
    with pytest.raises(core.NetaudioCoreError):
        core.build_command(
            {
                "command": "create_multicast_flow_2809",
                "channels": [1],
                "destinations": [{"address": "ff15::1", "port": 5004}],
                "media_local_flow_id": 1,
                "request_options_word": 0,
                "transport": "rtp_aes67",
            }
        )
