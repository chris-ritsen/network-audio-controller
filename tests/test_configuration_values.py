import pytest

from netaudio import core
from netaudio.presets.schema import normalize_device_config


@pytest.mark.parametrize(
    "values",
    [
        {"sample_rate": 0},
        {"encoding": True},
        {"sample_rate_pullup": 2**32},
        {"clock_source_code": -1},
        {"receive_flow_default_slots": 65536},
        {"receive_flow_performance": {"latency_microseconds": 4294968, "frames_per_packet": 8}},
        {"transmit_flow_performance": {"latency_microseconds": 250, "frames_per_packet": True}},
        {"receiver_channel_names": {"65536": "receive"}},
        {"rx_subscriptions": {"0": None}},
        {"codec_gain": [{"channel": 1, "level": 256, "device_type": "input"}]},
        {"redundancy_mode": "future"},
        {
            "interfaces": [
                {
                    "mode": "static",
                    "ip_address": "127.0.0.1",
                    "netmask": "255.255.255.0",
                    "gateway": "0.0.0.0",
                    "dns_server": "0.0.0.0",
                }
            ]
        },
    ],
)
def test_preset_and_native_configuration_reject_the_same_invalid_protocol_values(values):
    with pytest.raises(core.NetaudioCoreError):
        core.validate_configuration(values)

    with pytest.raises(ValueError):
        normalize_device_config({"name": "Desk", **values})


def test_native_configuration_accepts_boundary_values_without_rewriting_extensions():
    values = {
        "sample_rate": 48000,
        "encoding": 24,
        "sample_rate_pullup": 0,
        "receive_flow_performance": {"latency_microseconds": 4294967, "frames_per_packet": 65535},
        "receive_flow_default_slots": 0,
        "clock_source_code": 2,
        "receiver_channel_names": {"65535": "receive"},
        "unknown_fields": {"vendor_annotation": "preserved"},
    }
    assert core.validate_configuration(values) is None
    config = normalize_device_config({"name": "Desk", **values})

    assert config["unknown_fields"] == values["unknown_fields"]
    assert config["receiver_channel_names"] == {65535: "receive"}


@pytest.mark.parametrize("keys", [[True], [1.5], ["01", 1]])
@pytest.mark.parametrize("field", ["receiver_channel_names", "rx_subscriptions"])
def test_preset_channel_keys_cannot_be_coerced_or_overwritten(keys, field):
    value = "receive" if field == "receiver_channel_names" else None

    with pytest.raises(ValueError):
        normalize_device_config({"name": "Desk", field: dict.fromkeys(keys, value)})
