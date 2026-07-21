import struct

import pytest

from netaudio.dante.fact_store import DEFAULT_FACTS_PATH, get_fact
from netaudio.dante.packet_dissector import dissect


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


def test_rx_subscription_status_uses_capture_backed_labels():
    payload = bytearray(32)
    payload[0:2] = (0x27FF).to_bytes(2, "big")
    payload[2:4] = len(payload).to_bytes(2, "big")
    payload[6:8] = (0x3000).to_bytes(2, "big")
    payload[8:10] = (0x0001).to_bytes(2, "big")
    payload[10] = 1
    payload[11] = 1
    payload[12:14] = (1).to_bytes(2, "big")
    payload[26:28] = (0x0004).to_bytes(2, "big")

    result = dissect(bytes(payload), facts=[])

    subscription_status = next(span for span in result.spans if span.name == "subscription_status")
    assert subscription_status.detail == "Subscription connected (self)"


def test_encoding_status_dissects_current_and_supported_encodings():
    payload = bytes.fromhex(
        "ffff003c21020000001dc11073320000417564696e6174650724008200000000"
        "00180003000000180000000000020000000000180000001000000020"
    )

    result = dissect(payload, facts=[])
    spans_by_name = {span.name: span for span in result.spans if span.name}

    assert spans_by_name["supported_encoding_count"].value == "3"
    assert spans_by_name["current_encoding"].detail == "PCM24"
    assert [spans_by_name[f"supported_encoding_{index}"].detail for index in range(1, 4)] == [
        "PCM24",
        "PCM16",
        "PCM32",
    ]


def test_encoding_control_distinguishes_read_operand_from_set_target():
    read_payload = bytes.fromhex("ffff0028985a00003e42274cff240000417564696e617465073a0083000000640000000000000000")
    set_payload = bytes.fromhex("ffff00284b4e00003e42274cff240000417564696e617465073a0083000000640000000100000018")

    read_result = dissect(read_payload, facts=[])
    set_result = dissect(set_payload, facts=[])
    read_spans = {span.name: span for span in read_result.spans if span.name}
    set_spans = {span.name: span for span in set_result.spans if span.name}

    assert read_spans["operation_mode"].detail == "read current and supported encodings"
    assert read_spans["ignored_encoding_operand"].detail == "ignored in read mode"
    assert set_spans["operation_mode"].detail == "set encoding"
    assert set_spans["target_encoding"].detail == "PCM24"


def test_encoding_control_dynamic_fields_replace_generic_fact_spans():
    payload = bytes.fromhex("ffff0028985a00003e42274cff240000417564696e617465073a0083000000640000000000000000")

    result = dissect(payload)

    assert len([span for span in result.spans if span.offset == 28 and span.length == 4]) == 1
    assert len([span for span in result.spans if span.offset == 32 and span.length == 4]) == 1
    assert len([span for span in result.spans if span.offset == 36 and span.length == 4]) == 1
    assert any(span.name == "ignored_encoding_operand" for span in result.spans)
    assert all(span.name != "encoding_operand" for span in result.spans)


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
    spans_by_name = {span.name: span for span in result.spans if span.name}

    assert spans_by_name["default_latency"].value == "2,000,000 ns (2 ms)"
    assert spans_by_name["configured_latency"].value == "256,000 ns (256 us)"
    assert spans_by_name["active_latency"].value == "1,000,000 ns (1 ms)"
    assert spans_by_name["max_latency"].value == "42,666,667 ns (42.67 ms)"
    assert spans_by_name["min_latency"].value == "250,000 ns (250 us)"


def test_device_settings_preserves_unsupported_property_placeholders():
    properties = [(0x8204, 24), (0x0000, 0x8218), (0x8301, 28)]
    body = bytes([0x03, len(properties)])
    body += b"".join(struct.pack(">HH", *property_record) for property_record in properties)
    body += struct.pack(">II", 1_000_000, 2_000_000)

    result = dissect(_arc_packet(0x2809, 0x1100, 0x0001, body), facts=[])
    unsupported_property = next(span for span in result.spans if span.name == "unsupported_property_id")
    spans_by_name = {span.name: span for span in result.spans if span.name}

    assert unsupported_property.value == "0x8218"
    assert spans_by_name["default_latency"].value == "1,000,000 ns (1 ms)"
    assert spans_by_name["active_latency"].value == "2,000,000 ns (2 ms)"


def test_device_settings_does_not_parse_error_payload_as_property_table():
    body = bytes([0x03, 1]) + struct.pack(">HH", 0x8204, 16) + struct.pack(">I", 1_000_000)

    result = dissect(_arc_packet(0x2809, 0x1100, 0x0022, body), facts=[])

    assert all(span.name != "property_count" for span in result.spans)
    assert all(span.name != "default_latency" for span in result.spans)


def test_selective_device_settings_query_dissects_requested_properties():
    body = bytes([0x00, 3]) + struct.pack(">HHH", 0x8204, 0x8301, 0x8306)

    result = dissect(_arc_packet(0x27FF, 0x1100, 0x0000, body), facts=[])
    requested_properties = [span for span in result.spans if span.name == "requested_property_id"]

    assert [span.detail for span in requested_properties] == [
        "default_latency",
        "active_latency",
        "min_latency",
    ]


@pytest.mark.parametrize("protocol_identifier", [0x2729, 0x27FF, 0x2801, 0x2809])
def test_property_directory_dissects_captured_arc_protocol_variants(protocol_identifier):
    body = struct.pack(">HHHHH", 2, 0x8204, 0x0003, 0x8301, 0x0001)

    result = dissect(_arc_packet(protocol_identifier, 0x1102, 0x0001, body), facts=[])
    property_identifiers = [span for span in result.spans if span.name == "property_id"]
    property_flags = [span for span in result.spans if span.name == "property_flags"]

    assert [span.detail for span in property_identifiers] == ["default_latency", "active_latency"]
    assert [span.value for span in property_flags] == ["0x0003", "0x0001"]


def test_protocol_facts_use_capability_and_property_table_meanings():
    encoding_status = get_fact(DEFAULT_FACTS_PATH, "conmon_message", "0x0082")
    encoding_control = get_fact(DEFAULT_FACTS_PATH, "conmon_message", "0x0083")
    device_settings = get_fact(DEFAULT_FACTS_PATH, "arc_opcode", "0x1100")
    property_directory = get_fact(DEFAULT_FACTS_PATH, "arc_opcode", "0x1102")

    assert encoding_status["name"] == "encoding_status"
    assert encoding_control["name"] == "encoding_control"
    assert device_settings["name"] == "query_device_settings"
    assert property_directory["name"] == "query_property_directory"
    assert all(field["name"] != "aes67_mode" for field in device_settings["fields"])
    assert get_fact(DEFAULT_FACTS_PATH, "conmon_message", "0x03D7") is None
    assert get_fact(DEFAULT_FACTS_PATH, "conmon_opcode", "0x0083") is None
    assert get_fact(DEFAULT_FACTS_PATH, "ddp_notification", "0x0082") is None
