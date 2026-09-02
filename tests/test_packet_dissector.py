import struct

import pytest
from netaudio.dante.dissection.dissector import dissect


def _arc_packet(protocol_identifier: int, opcode: int, status: int, body: bytes = b"") -> bytes:
    packet_length = 10 + len(body)
    return struct.pack(">HHHHH", protocol_identifier, packet_length, 0x1234, opcode, status) + body


def test_sample_rate_fields_are_rendered_as_frequency():
    payload = bytearray(40)
    payload[36:40] = (44_100).to_bytes(4, "big")
    facts = [
        {
            "category": "conmon_message",
            "key": "0x0081",
            "name": "sample_rate_control",
            "fields": [
                {
                    "name": "target_sample_rate",
                    "offset": 36,
                    "length": 4,
                    "dtype": "uint32_be",
                }
            ],
        }
    ]

    result = dissect(bytes(payload), facts=facts)

    target_sample_rate = next(span for span in result.spans if span.name == "target_sample_rate")
    assert target_sample_rate.value == "44,100 (44.1 kHz)"


def test_direction_scoped_fact_fields_are_dissected_only_at_matching_boundaries():
    facts = [
        {
            "category": "arc_opcode",
            "key": "0x2202",
            "name": "delete_tx_flow",
            "fields": [
                {
                    "name": "delete_flow_slot",
                    "offset": 14,
                    "length": 2,
                    "dtype": "uint16_be",
                    "direction": "request",
                },
                {
                    "name": "delete_result",
                    "offset": 8,
                    "length": 2,
                    "dtype": "uint16_be",
                    "direction": "response",
                },
            ],
        }
    ]
    request = bytes.fromhex("2729001016c122020000000100000020")
    response = bytes.fromhex("2729000a16c122020001")

    request_result = dissect(request, facts=facts, direction="request")
    response_result = dissect(response, facts=facts, direction="response")
    unspecified_result = dissect(request, facts=facts)

    assert {span.name for span in request_result.spans} >= {"delete_flow_slot"}
    assert "delete_result" not in {span.name for span in request_result.spans}
    assert {span.name for span in response_result.spans} >= {"delete_result"}
    assert "delete_flow_slot" not in {span.name for span in response_result.spans}
    assert "delete_flow_slot" not in {span.name for span in unspecified_result.spans}
    assert "delete_result" not in {span.name for span in unspecified_result.spans}


def _rx_channels_packet(status_code: int) -> bytes:
    strings_offset = 10 + 2 + 20
    record = struct.pack(
        ">HHHHHHHHI",
        1,
        0,
        0,
        strings_offset,
        strings_offset + 4,
        strings_offset + 8,
        0,
        status_code,
        0,
    )
    body = bytes([1, 1]) + record + b"tx1\x00dev\x00rx1\x00"
    return _arc_packet(0x27FF, 0x3000, 0x0001, body)


def test_rx_channels_response_renders_core_records():
    result = dissect(_rx_channels_packet(0x0004), facts=[])

    assert result.core_kind == "rx"
    assert result.core_fields == {
        "records": [
            {
                "number": 1,
                "rx_channel_name": "rx1",
                "rx_status_code": 0,
                "subscription_status_code": 4,
                "tx_channel_name": "tx1",
                "tx_device_name": "dev",
            }
        ]
    }


def test_encoding_status_dissects_current_and_supported_encodings():
    payload = bytes.fromhex(
        "ffff003c21020000001dc11073320000417564696e6174650724008200000000"
        "00180003000000180000000000020000000000180000001000000020"
    )

    result = dissect(payload, facts=[])

    assert result.core_kind == "encoding_status"
    assert result.core_fields == {"current_encoding": 24, "supported_encodings": [24, 16, 32]}


def test_encoding_control_request_keeps_fact_spans_without_a_core_parser():
    payload = bytes.fromhex("ffff0028985a00003e42274cff240000417564696e617465073a0083000000640000000000000000")

    facts = [
        {
            "category": "conmon_message",
            "key": "0x0083",
            "name": "encoding_control",
            "protocol_id": 0xFFFF,
            "match_offset": 26,
            "match_size": 2,
            "fields": [
                {
                    "name": "message_type",
                    "offset": 26,
                    "length": 2,
                    "dtype": "uint16_be",
                    "value": "0x0083",
                },
                {
                    "name": "operation_mode",
                    "offset": 32,
                    "length": 4,
                    "dtype": "uint32_be",
                },
                {
                    "name": "encoding_operand",
                    "offset": 36,
                    "length": 4,
                    "dtype": "uint32_be",
                },
            ],
        }
    ]

    result = dissect(payload, facts=facts)
    spans_by_name = {span.name: span for span in result.spans if span.name}

    assert result.core_kind is None
    assert spans_by_name["message_type"].detail == "encoding_control"
    assert spans_by_name["operation_mode"].value == "0x00000000"
    assert spans_by_name["encoding_operand"].value == "0x00000000"


def test_interface_status_packet_12362182_dissects_link_speed_from_protocol_fact():
    payload = bytes.fromhex(
        "ffff0060dbdb0000001dc1fffe507b8d417564696e6174650727001100000000"
        "00010000000000640003001dc1507b8dc0a80123ffffff0008080808c0a80101"
        "0018003000000000000000000000000000000000000000000000000000000000"
    )

    facts = [
        {
            "category": "conmon_message",
            "key": "0x0011",
            "name": "interface_status_announcement",
            "protocol_id": 0xFFFF,
            "match_offset": 26,
            "match_size": 2,
            "fields": [
                {
                    "name": "message_type",
                    "offset": 26,
                    "length": 2,
                    "dtype": "uint16_be",
                    "value": "0x0011",
                },
                {
                    "name": "interface_count",
                    "offset": 32,
                    "length": 2,
                    "dtype": "uint16_be",
                },
                {
                    "name": "link_speed_mbps",
                    "offset": 36,
                    "length": 4,
                    "dtype": "uint32_be",
                },
            ],
        }
    ]

    result = dissect(payload, facts=facts)
    spans_by_name = {span.name: span for span in result.spans if span.name}

    assert spans_by_name["message_type"].detail == "interface_status_announcement"
    assert spans_by_name["interface_count"].value == "1"
    assert spans_by_name["link_speed_mbps"].value == "100"
    assert spans_by_name["link_speed_mbps"].detail == "100 Mbps"


def test_device_settings_resolves_latency_values_through_absolute_pointers():
    properties = [
        (0x8301, 32),
        (0x8204, 36),
        (0x8302, 40),
        (0x8205, 44),
        (0x8306, 48),
    ]
    values = [1_000_000, 2_000_000, 42_666_667, 256_000, 250_000]
    body = bytes([0x24, len(properties)])
    body += b"".join(struct.pack(">HH", *property_record) for property_record in properties)
    body += b"".join(struct.pack(">I", property_value) for property_value in values)

    result = dissect(_arc_packet(0x2729, 0x1100, 0x0001, body), facts=[])

    assert result.core_kind == "device_settings"
    assert result.core_fields["default_latency_ns"] == 2_000_000
    assert result.core_fields["configured_latency_ns"] == 256_000
    assert result.core_fields["active_latency_ns"] == 1_000_000
    assert result.core_fields["max_latency_ns"] == 42_666_667
    assert result.core_fields["min_latency_ns"] == 250_000
    assert [entry["pointer"] for entry in result.core_fields["referenced_values"]] == [32, 36, 40, 44, 48]


@pytest.mark.parametrize(
    ("value_hexadecimal", "payload_hexadecimal"),
    [
        (
            "00000000000000000000000000000000",
            "27ff00a80000110000011b18802000788021007c0022b000002300000024000100f00000020100018204008c82050090020a0000020b0000021000000211000002120030021300000214000083010094830600988302009c03100010031100020303000483f000a0060100000000000000000000000000000000000000000000000000000000000000000000000f4240000f4240000000000003d090000000000000000500000000",
        ),
        (
            "00000001000000000000000000000000",
            "27ff00a80000110000011b18802000788021007c0022b000002300000024000100f00000020100018204008c82050090020a0000020b0000021000000211000002120030021300000214000083010094830600988302009c03100010031100020303000483f000a0060100000000000000000000000000000000000000000001000000000000000000000000000f4240000f4240000000000003d090000000000000000500000000",
        ),
    ],
)
def test_device_settings_preserves_variable_width_property_values(value_hexadecimal, payload_hexadecimal):
    result = dissect(bytes.fromhex(payload_hexadecimal), facts=[])
    referenced_by_property = {entry["info_code"]: entry for entry in result.core_fields["referenced_values"]}

    assert result.core_kind == "device_settings"
    assert referenced_by_property[0x8021]["pointer"] == 0x007C
    assert referenced_by_property[0x8021]["value_hexadecimal"] == value_hexadecimal
    assert referenced_by_property[0x83F0]["value_hexadecimal"] == "0000000500000000"


def test_device_settings_preserves_unsupported_property_placeholders():
    properties = [(0x8204, 24), (0x0000, 0x8218), (0x8301, 28)]
    body = bytes([0x03, len(properties)])
    body += b"".join(struct.pack(">HH", *property_record) for property_record in properties)
    body += struct.pack(">II", 1_000_000, 2_000_000)

    result = dissect(_arc_packet(0x2809, 0x1100, 0x0001, body), facts=[])

    assert result.core_kind == "device_settings"
    assert result.core_fields["unavailable_property_ids"] == [0x8218]
    assert result.core_fields["default_latency_ns"] == 1_000_000
    assert result.core_fields["active_latency_ns"] == 2_000_000


def test_device_settings_does_not_parse_error_payload_as_property_table():
    body = bytes([0x03, 1]) + struct.pack(">HH", 0x8204, 16) + struct.pack(">I", 1_000_000)

    result = dissect(_arc_packet(0x2809, 0x1100, 0x0022, body), facts=[])

    assert result.core_kind is None
    assert result.core_fields is None


def test_device_settings_request_has_no_core_parser():
    body = bytes([0x00, 3]) + struct.pack(">HHH", 0x8204, 0x8301, 0x8306)

    result = dissect(_arc_packet(0x27FF, 0x1100, 0x0000, body), facts=[])

    assert result.core_kind is None
    assert result.header_summary == "protocol=ARC  18B"


@pytest.mark.parametrize("protocol_identifier", [0x2729, 0x27FF, 0x2801, 0x2809])
def test_property_directory_dissects_captured_arc_protocol_variants(protocol_identifier):
    body = struct.pack(">HHHHH", 2, 0x8204, 0x0003, 0x8301, 0x0001)

    result = dissect(_arc_packet(protocol_identifier, 0x1102, 0x0001, body), facts=[])

    assert result.core_kind == "property_directory"
    assert result.core_fields == {
        "aes67_supported": False,
        "properties": [{"flags": 3, "property_id": 0x8204}, {"flags": 1, "property_id": 0x8301}],
    }


def test_disproved_facts_never_reach_runtime_labeling(tmp_path, monkeypatch):
    from netaudio.capture import packets as capture_packets
    from netaudio.dante import fact_store
    from netaudio.dante.dissection.dissector import _load_facts_for_packet
    from netaudio.dante.fact_store import FactRecord, add_fact

    facts_path = tmp_path / "facts.json"
    add_fact(
        facts_path,
        "conmon_message",
        "0x03D7",
        FactRecord("invalid_interpretation", confidence="disproved", protocol_id=0xFFFF, match_offset=26),
    )
    add_fact(
        facts_path, "conmon_message", "0x03D8", FactRecord("active_interpretation", protocol_id=0xFFFF, match_offset=26)
    )
    payload = bytearray(28)
    payload[0:2] = (0xFFFF).to_bytes(2, "big")
    payload[26:28] = (0x03D7).to_bytes(2, "big")

    assert _load_facts_for_packet(bytes(payload), facts_path) == []

    monkeypatch.setattr(fact_store, "DEFAULT_FACTS_PATH", facts_path)
    capture_packets._FACT_LABEL_CACHE = None
    try:
        labels = capture_packets._load_fact_labels()
        assert labels["conmon:0x03D8"] == "active_interpretation"
        assert "conmon:0x03D7" not in labels
    finally:
        capture_packets._FACT_LABEL_CACHE = None
