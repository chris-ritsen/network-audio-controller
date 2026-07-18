import json
from pathlib import Path

import pytest

from netaudio import core
from tests.core_golden import response_input_bytes

if not core.available():
    pytest.skip("netaudio-core library not available", allow_module_level=True)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN = json.loads((FIXTURES_DIR / "core_responses_golden.json").read_text())


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_parse_response_matches_golden(name):
    entry = GOLDEN[name]
    parsed = core.parse_response(entry["kind"], response_input_bytes(FIXTURES_DIR, name, entry))
    assert parsed == entry["parsed"]


def _channel_count_response(tx_count=260, rx_count=520):
    response = bytearray(16)
    response[0:2] = (0x27FF).to_bytes(2, "big")
    response[2:4] = len(response).to_bytes(2, "big")
    response[6:8] = (0x1000).to_bytes(2, "big")
    response[8:10] = (1).to_bytes(2, "big")
    response[12:14] = tx_count.to_bytes(2, "big")
    response[14:16] = rx_count.to_bytes(2, "big")
    return response


def test_channel_count_preserves_u16_counts():
    assert core.parse_response("channel_count", bytes(_channel_count_response())) == {
        "tx_count": 260,
        "rx_count": 520,
        "locked": None,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (slice(0, 2), 0x1234),
        (slice(2, 4), 15),
        (slice(6, 8), 0x1002),
        (slice(8, 10), 0x8001),
    ],
)
def test_channel_count_rejects_invalid_response_envelope(field, value):
    response = _channel_count_response()
    response[field] = value.to_bytes(2, "big")

    with pytest.raises(core.NetaudioCoreError) as exc_info:
        core.parse_response("channel_count", bytes(response))

    assert exc_info.value.status == 10


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_typed_response_parsers_reject_every_truncated_prefix(name):
    entry = GOLDEN[name]
    data = response_input_bytes(FIXTURES_DIR, name, entry)
    for length in range(len(data)):
        with pytest.raises(core.NetaudioCoreError) as exc_info:
            core.parse_response(entry["kind"], data[:length])
        assert exc_info.value.status == 10


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_typed_response_parsers_reject_wrong_protocol_or_declared_length(name):
    entry = GOLDEN[name]
    original = response_input_bytes(FIXTURES_DIR, name, entry)
    for field, value in ((slice(0, 2), 0x1234), (slice(2, 4), len(original) - 1)):
        data = bytearray(original)
        data[field] = value.to_bytes(2, "big")
        with pytest.raises(core.NetaudioCoreError) as exc_info:
            core.parse_response(entry["kind"], bytes(data))
        assert exc_info.value.status == 10


RX_PAGE = (FIXTURES_DIR / "20250517_200646_499097_avio-usb-2_get_receivers_response.bin").read_bytes()
TX_INFO_PAGE = bytes.fromhex(
    "27ff0048aaaa20000001000000010000002c0030"
    "00020000002c003600030000002c003c00040000"
    "002c00420000bb8063682d30310063682d303200"
    "63682d30330063682d303400"
)
TX_FRIENDLY_PAGE = bytes.fromhex(
    "27ff005ebbbb20100001000000000001002400000002003100000003004100000004"
    "00526d69632d6d69782d68696768006c696e75782d6d61696e3a6c656674006c69"
    "6e75782d6d61696e3a7269676874006d69632d6d69782d6c6f7700"
)


@pytest.mark.parametrize(
    ("kind", "data"),
    [("rx", RX_PAGE), ("tx_info", TX_INFO_PAGE), ("tx_friendly", TX_FRIENDLY_PAGE)],
)
def test_page_parsers_reject_every_truncated_prefix(kind, data):
    for length in range(len(data)):
        with pytest.raises(core.NetaudioCoreError) as exc_info:
            core.parse_page(kind, data[:length], 1)
        assert exc_info.value.status == 10


def test_rx_page_gap_and_bad_pointer_do_not_return_partial_records():
    for offset, value in ((32, 3), (20, 1)):
        data = bytearray(RX_PAGE)
        data[offset : offset + 2] = value.to_bytes(2, "big")
        with pytest.raises(core.NetaudioCoreError) as exc_info:
            core.parse_page("rx", bytes(data), 1)
        assert exc_info.value.status == 10


def test_tx_page_gap_and_group_change_do_not_return_partial_records():
    gap = bytearray(TX_INFO_PAGE)
    gap[20:22] = (3).to_bytes(2, "big")
    with pytest.raises(core.NetaudioCoreError, match="malformed response"):
        core.parse_page("tx_info", bytes(gap), 1)

    group_change = bytearray(TX_INFO_PAGE)
    group_change[24:26] = (0x1234).to_bytes(2, "big")
    with pytest.raises(core.NetaudioCoreError, match="malformed response"):
        core.parse_page("tx_info", bytes(group_change), 1)
