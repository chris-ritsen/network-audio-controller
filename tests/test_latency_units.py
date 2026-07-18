from netaudio._common import format_devices_xml
from netaudio.dante.device import DanteDevice
from netaudio.dante.latency import milliseconds_to_microseconds, nanoseconds_to_milliseconds


def test_nanoseconds_convert_to_fractional_milliseconds_without_heuristics():
    assert nanoseconds_to_milliseconds(150_000) == 0.15
    assert nanoseconds_to_milliseconds("250000") == 0.25
    assert nanoseconds_to_milliseconds(1_000_000) == 1.0


def test_milliseconds_convert_to_preset_microseconds_without_heuristics():
    assert milliseconds_to_microseconds(0.15) == 150
    assert milliseconds_to_microseconds(1.0) == 1_000
    assert milliseconds_to_microseconds(5) == 5_000


def test_core_device_settings_are_normalized_to_milliseconds():
    controls = DanteDevice().controls_data_from_core(
        {
            "name": None,
            "counts": (0, 0, None),
            "aes67": None,
            "settings": {
                "sample_rate": 48_000,
                "default_latency_ns": 1_000_000,
                "latency_ns": 150_000,
                "min_latency_ns": 150_000,
                "max_latency_ns": 21_333_334,
            },
            "rx": [],
            "tx": [],
        }
    )

    assert controls["latency"] == 0.15
    assert controls["min_latency"] == 0.15
    assert controls["max_latency"] == 21.333334


def test_dante_controller_preset_exports_microseconds_from_milliseconds():
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.latency = 0.15

    xml = format_devices_xml({device.server_name: device})

    assert "<unicast_latency>150</unicast_latency>" in xml
