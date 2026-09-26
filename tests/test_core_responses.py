import json
from pathlib import Path

import pytest

from netaudio import core
from tests.core_golden import response_input_bytes


@pytest.mark.parametrize("result,accepted", [(1, True), (0x8112, True), (7, False), (0, False)])
def test_native_command_acknowledgement_classifies_recognized_replies(result, accepted):
    response = bytes.fromhex("2729000a00012201") + result.to_bytes(2, "big")
    assert core.command_acknowledgement(response) == {
        "received": True,
        "parseable": True,
        "result_code": result,
        "accepted": accepted,
        "raw_response_hexadecimal": response.hex(),
    }


@pytest.mark.parametrize("response", [b"", b"truncated", bytes.fromhex("2810000a000122010001")])
def test_unrecognized_acknowledgement_is_not_reported_as_rejection(response):
    assert core.command_acknowledgement(response) == {
        "received": True,
        "parseable": False,
        "raw_response_hexadecimal": response.hex(),
    }


def test_missing_acknowledgement_remains_unavailable():
    assert core.command_acknowledgement(None) is None


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
    result = core.parse_response("channel_count", bytes(_channel_count_response()))
    assert result.pop("receiver_telemetry_capacity")["raw_response"] == list(_channel_count_response())
    assert result == {
        "transmit_flow_authoring_capability_word": 0,
        "uses_modern_transmit_flow_authoring": False,
        "tx_count": 260,
        "rx_count": 520,
        "locked": None,
    }


@pytest.mark.parametrize(
    "word,protocol,inventory",
    [
        (0, 0x2729, "legacy"),
        (0x0030, 0x2729, "legacy"),
        (0x1000, 0x2809, "modern"),
        (0x1030, 0x2809, "modern"),
    ],
)
def test_authoring_capabilities_are_consistent_from_wire_to_device(word, protocol, inventory):
    from netaudio.dante.device import DanteDevice

    response = _channel_count_response()
    response[10:12] = word.to_bytes(2, "big")
    parsed = core.parse_response("channel_count", bytes(response))
    expected = {
        "transmit_flow_authoring": {
            "protocol_id": protocol,
            "identity_field": "media_local_flow_id" if inventory == "modern" else "global_flow_id",
            "identifier_max": 65535 if inventory == "modern" else 32,
            "media_modes": ["native_dante", "rtp_aes67"] if inventory == "modern" else ["native_dante"],
            "supports_flow_options": inventory == "modern",
        },
        "receiver_flow_inventory_family": inventory,
    }
    assert core.flow_authoring_capabilities(word) == expected
    assert parsed["uses_modern_transmit_flow_authoring"] is (protocol == 0x2809)
    device = DanteDevice("receiver.local.")
    controls = device.controls_data_from_core(
        {
            "name": "Receiver",
            "counts": {
                "tx_count": 260,
                "rx_count": 520,
                "locked": None,
                "transmit_flow_authoring_capability_word": word,
                "receiver_telemetry_capacity": None,
            },
            "rx": [],
            "tx": [],
            "channels_included": False,
        }
    )
    device.apply_controls(controls)

    assert device.tx_count == 260 and device.rx_count == 520
    assert {name: getattr(device, name) for name in expected} == expected


@pytest.mark.parametrize("word", [None, True, -1, 65536])
def test_flow_capabilities_never_guess_from_missing_or_truncated_words(word):
    with pytest.raises(ValueError):
        core.flow_authoring_capabilities(word)


@pytest.mark.parametrize(
    ("payload_hexadecimal", "expected_discriminators", "expected_speeds"),
    [
        (
            "ffff008c000b00000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c0000000000000000000000000000000000000001000003e80000000000000000000000000000000001000001000003e8000000000000000000000000000000000101000000000000",
            [1, 0x01000001, 0x01010000],
            [1000, 1000, 0],
        ),
        (
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c000000000000000000000000000000000000000100000064000000000000000000000000000000000100000100000064000000000000000000000000000000000101000000000000",
            [1, 0x01000001, 0x01010000],
            [100, 100, 0],
        ),
        (
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c0000000000000000000000000000000000000001000003e80000000000000000000000000000000001000000000000000000000000000000000000000000000001010001000003e8",
            [1, 0x01000000, 0x01010001],
            [1000, 0, 1000],
        ),
        (
            "ffff008c001000000200000000010000417564696e6174650724004000000000000100240010000000140000000000010000000000000000000000070003002c0044005c000000000000000000000000000000000000000100000064000000000000000000000000000000000100000000000000000000000000000000000000000000000101000100000064",
            [1, 0x01000000, 0x01010001],
            [100, 0, 100],
        ),
    ],
)
def test_interface_statistics_preserves_raw_discriminators_and_speeds(
    payload_hexadecimal,
    expected_discriminators,
    expected_speeds,
):
    parsed = core.parse_response("interface_statistics_status", bytes.fromhex(payload_hexadecimal))
    records = parsed["interface_groups"][0]["raw_records"]

    assert [record["discriminator_status_word"] for record in records] == expected_discriminators
    assert [record["speed_megabits_per_second"] for record in records] == expected_speeds
    assert parsed["interface_groups"][0]["selected_stats"] == records[0]


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
def test_typed_response_parsers_reject_truncation_with_original_declared_length(name):
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
    ids=["rx", "tx-info", "tx-friendly"],
)
def test_page_parsers_reject_truncation_with_original_declared_length(kind, data):
    for length in range(len(data)):
        with pytest.raises(core.NetaudioCoreError) as exc_info:
            core.parse_page(kind, data[:length], 1)
        assert exc_info.value.status == 10


@pytest.mark.parametrize(
    ("kind", "data"),
    [("rx", RX_PAGE), ("tx_info", TX_INFO_PAGE), ("tx_friendly", TX_FRIENDLY_PAGE)],
    ids=["rx", "tx-info", "tx-friendly"],
)
@pytest.mark.parametrize("cut", [10, 11, 13, 20, 33, -8])
def test_page_parsers_reject_incomplete_records_with_consistent_declared_length(kind, data, cut):
    # These cuts remove required table or string bytes. Some other prefixes
    # are valid empty pages or merely omit unreferenced trailing data.
    shortened = bytearray(data[:cut])
    shortened[2:4] = len(shortened).to_bytes(2, "big")
    with pytest.raises(core.NetaudioCoreError) as exc_info:
        core.parse_page(kind, bytes(shortened), 1)
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
    with pytest.raises(core.NetaudioCoreError, match="malformed binary response"):
        core.parse_page("tx_info", bytes(gap), 1)

    group_change = bytearray(TX_INFO_PAGE)
    group_change[24:26] = (0x1234).to_bytes(2, "big")
    with pytest.raises(core.NetaudioCoreError, match="malformed binary response"):
        core.parse_page("tx_info", bytes(group_change), 1)
