import json

import pytest
from netaudio.cli_support.output import drop_empty_columns
from netaudio.commands.device import display as device_display
from netaudio.commands.device.display import (
    device_list_headers,
    device_list_row,
    format_clock_frequency_offset_parts_per_billion,
    format_encoding,
    format_encodings,
    format_latency_choices_milliseconds,
    format_latency_milliseconds,
    format_latency_nanoseconds,
    format_latency_range_milliseconds,
    format_link_speed_megabits_per_second,
    format_on_off,
    format_sample_rate_hertz,
    format_sample_rates_hertz,
)
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DEVICE_JSON_FIELD_NAMES, DanteDeviceSerializer
from netaudio.dante.latency import latency_controls_from_settings, latency_state_from_settings


def make_device() -> DanteDevice:
    device = DanteDevice(server_name="AVIOAES3-53ef37.local.")
    device.name = "avio-aes3-1"
    device.ipv4 = "192.168.1.18"
    device.mac_address = "001dc1fffe53ef37"
    device.manufacturer = "Audinate"
    device.dante_model = "AVIO-AES3"
    device.model_id = "DIOAES3"
    device.sample_rate = 48_000
    device.supported_sample_rates = [44_100, 48_000, 88_200, 96_000]
    device.encoding = 24
    device.supported_encodings = [16, 24, 32]
    device.latency = 2.0
    device.active_latency = 2.0
    device.configured_latency = 2.0
    device.default_latency = 1.0
    device.min_latency = 1.0
    device.max_latency = 20.3125
    device.receiver_flow_latency_nanoseconds = 2_000_000
    device.tx_count = 2
    device.rx_count = 2
    return device


def test_device_from_json_accepts_unit_suffixed_and_legacy_field_names():
    unit_suffixed = DanteDeviceSerializer.device_from_json(
        {"server_name": "a.local.", "sample_rate_hz": 96_000, "latency_ms": 0.25, "min_latency_ms": 0.15}
    )
    legacy = DanteDeviceSerializer.device_from_json(
        {"server_name": "a.local.", "sample_rate": 96_000, "latency": 0.25, "min_latency": 0.15}
    )

    for device in (unit_suffixed, legacy):
        assert device.sample_rate == 96_000
        assert device.latency == 0.25
        assert device.min_latency == 0.15


def test_device_json_field_names_state_their_units():
    as_json = json.loads(json.dumps(DanteDeviceSerializer.to_json(make_device()), default=str))

    assert as_json["sample_rate_hz"] == 48_000
    assert as_json["supported_sample_rates_hz"] == [44_100, 48_000, 88_200, 96_000]
    assert as_json["latency_ms"] == 2.0
    assert as_json["active_latency_ms"] == 2.0
    assert as_json["configured_latency_ms"] == 2.0
    assert as_json["default_latency_ms"] == 1.0
    assert as_json["min_latency_ms"] == 1.0
    assert as_json["max_latency_ms"] == 20.3125
    assert as_json["standard_latency_choices_ms"] == [1.0, 2.0, 5.0]
    assert as_json["receiver_flow_latency_ns"] == 2_000_000
    assert list(as_json) == sorted(as_json)
    for attribute_name in DEVICE_JSON_FIELD_NAMES:
        assert attribute_name not in as_json


def test_device_list_row_uses_shared_formatters_and_kind():
    device = make_device()
    device.bit_depth = 24
    device.encoding = None

    row = dict(zip(device_list_headers(verbose=True), device_list_row(device.server_name, device, verbose=True)))

    assert row["Kind"] == "hardware"
    assert row["Sample Rate"] == "48 kHz"
    assert row["Supported Sample Rates"] == "44.1, 48, 88.2, 96 kHz"
    assert row["Encoding"] == "PCM24"
    assert row["Supported Encodings"] == "PCM16, PCM24, PCM32"
    assert row["Latency"] == "2 ms"
    assert row["Configured Latency"] == "2 ms"
    assert row["Latency Range"] == "1-20.3125 ms"
    assert row["Latency Options"] == "1, 2, 5 ms"
    assert row["Sample Rate Pull-Up"] == ""
    assert row["Bluetooth"] == ""
    assert "Bit Depth" not in row


def test_drop_empty_columns_removes_columns_that_are_blank_in_every_row():
    headers = ["Name", "Sample Rate Pull-Up", "Bluetooth", "Lock"]
    rows = [["a", "", "", "unlocked"], ["b", "", "", ""]]

    kept_headers, kept_rows = drop_empty_columns(headers, rows)

    assert kept_headers == ["Name", "Lock"]
    assert kept_rows == [["a", "unlocked"], ["b", ""]]


def test_format_aes67_reads_as_on_off_with_prefix_in_parentheses():
    device = make_device()
    device.aes67_current = False
    device.aes67_configured = False
    device.aes67_multicast_prefix = "239.69.0.0"
    assert device_display._format_aes67(device) == "off (multicast prefix 239.69.0.0)"

    device.aes67_configured = True
    assert device_display._format_aes67(device) == "off (configured on, reboot required; multicast prefix 239.69.0.0)"

    device.aes67_current = None
    device.aes67_configured = None
    device.aes67_multicast_prefix = None
    assert device_display._format_aes67(device) == ""

    device.failed_queries.add("aes67")
    assert device_display._format_aes67(device) == "unknown"

    device.aes67_supported = False
    assert device_display._format_aes67(device) == "unsupported"


def test_format_latency_range_milliseconds():
    assert format_latency_range_milliseconds(1.0, 20.3125) == "1-20.3125 ms"
    assert format_latency_range_milliseconds(0.15, 21.333334) == "0.15-21.3333 ms"
    assert format_latency_range_milliseconds(None, 5.0) == ""


@pytest.mark.parametrize(
    ("formatter", "value", "expected"),
    [
        (format_clock_frequency_offset_parts_per_billion, -21_200, "-21.2 ppm"),
        (format_clock_frequency_offset_parts_per_billion, None, ""),
        (format_encoding, 24, "PCM24"),
        (format_encoding, None, ""),
        (format_encodings, [16, 24, 32], "PCM16, PCM24, PCM32"),
        (format_encodings, [], "none advertised"),
        (format_encodings, None, ""),
        (format_latency_choices_milliseconds, [1.0, 2.0, 5.0], "1, 2, 5 ms"),
        (format_latency_choices_milliseconds, None, ""),
        (format_latency_milliseconds, 1.0, "1 ms"),
        (format_latency_milliseconds, 2.5, "2.5 ms"),
        (format_latency_milliseconds, 0.15, "0.15 ms"),
        (format_latency_milliseconds, None, ""),
        (format_latency_nanoseconds, 1_000_000, "1 ms"),
        (format_latency_nanoseconds, 2_500_000, "2.5 ms"),
        (format_latency_nanoseconds, 291_667, "0.291667 ms"),
        (format_latency_nanoseconds, None, ""),
        (format_link_speed_megabits_per_second, 100, "100 Mbps"),
        (format_link_speed_megabits_per_second, 1_000, "1 Gbps"),
        (format_link_speed_megabits_per_second, None, ""),
        (format_on_off, True, "on"),
        (format_on_off, False, "off"),
        (format_on_off, None, ""),
        (format_sample_rate_hertz, 48_000, "48 kHz"),
        (format_sample_rate_hertz, 44_100, "44.1 kHz"),
        (format_sample_rate_hertz, None, ""),
        (format_sample_rates_hertz, [44_100, 48_000, 96_000], "44.1, 48, 96 kHz"),
        (format_sample_rates_hertz, None, ""),
    ],
)
def test_formatters_render_units_and_blank_for_missing_values(formatter, value, expected):
    assert formatter(value) == expected


def test_latency_controls_and_state_derive_milliseconds_identically():
    settings = {
        "active_latency_ns": 750_000,
        "configured_latency_ns": 250_000,
        "default_latency_ns": 1_000_000,
        "max_latency_ns": 5_000_000,
        "min_latency_ns": 150_000,
    }

    controls = latency_controls_from_settings(settings)
    state = latency_state_from_settings(settings)

    for field_name in ("active", "configured", "default", "max", "min"):
        assert controls[f"{field_name}_latency"] == state[f"{field_name}_latency_ms"]
    assert controls["latency"] == state["active_latency_ms"]


def test_show_rows_read_preferred_leader_and_lock_as_words():
    device = make_device()
    device.preferred_leader = False
    device.is_locked = False

    rows = dict(device_display._device_show_rows(device))

    assert rows["Kind"] == "hardware"
    assert rows["Preferred Leader"] == "off"
    assert rows["Lock"] == "unlocked"
    assert rows["Sample Rate"] == "48 kHz"
    assert rows["Supported Sample Rates"] == "44.1, 48, 88.2, 96 kHz"
    assert rows["Latency Range"] == "1-20.3125 ms"
    assert rows["Latency Options"] == "1, 2, 5 ms"

    device.preferred_leader = None
    device.is_locked = None
    rows = dict(device_display._device_show_rows(device))
    assert rows["Preferred Leader"] == ""
    assert rows["Lock"] == ""

    device.failed_queries.update({"clock_status", "is_locked"})
    rows = dict(device_display._device_show_rows(device))
    assert rows["Preferred Leader"] == "unknown"
    assert rows["Lock"] == "unknown"


def test_transmitter_flow_summary_uses_shared_formatters():
    summary = device_display._format_transmitter_flow(
        {
            "channel_count": 2,
            "destination_internet_protocol_version_four_address": "192.168.1.108",
            "destination_user_datagram_port": 14341,
            "encoding": 24,
            "flow_number": 1,
            "flow_type": "unicast",
            "sample_rate": 48_000,
            "subscriber_device_name": "lx-dante",
            "subscriber_flow_name": "4",
        }
    )

    assert summary == "unicast 2ch 48 kHz PCM24 -> 192.168.1.108:14341 lx-dante/4"
