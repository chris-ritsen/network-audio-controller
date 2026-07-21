from types import SimpleNamespace

import pytest

from netaudio._common import format_devices_xml
from netaudio.dante.device import DanteDevice
from netaudio.dante.latency import (
    milliseconds_to_microseconds,
    nanoseconds_to_milliseconds,
    standard_latency_choices_for_range,
)


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
                "configured_latency_ns": 250_000,
                "active_latency_ns": 150_000,
                "latency_ns": 150_000,
                "min_latency_ns": 150_000,
                "max_latency_ns": 21_333_334,
            },
            "rx": [],
            "tx": [],
        }
    )

    assert controls["latency"] == 0.15
    assert controls["active_latency"] == 0.15
    assert controls["configured_latency"] == 0.25
    assert controls["default_latency"] == 1.0
    assert controls["min_latency"] == 0.15
    assert controls["max_latency"] == 21.333334


def test_configured_latency_remains_compatibility_latency_when_active_is_unavailable():
    controls = DanteDevice().controls_data_from_core(
        {
            "name": None,
            "counts": (0, 0, None),
            "aes67": None,
            "settings": {
                "configured_latency_ns": 250_000,
                "active_latency_ns": None,
                "latency_ns": 250_000,
            },
            "rx": [],
            "tx": [],
        }
    )

    assert controls["latency"] == 0.25
    assert controls["active_latency"] is None
    assert controls["configured_latency"] == 0.25


def test_legacy_compatibility_latency_populates_latency_and_configured_latency():
    controls = DanteDevice().controls_data_from_core(
        {
            "name": None,
            "counts": (0, 0, None),
            "aes67": None,
            "settings": {"latency_ns": 1_000_000},
            "rx": [],
            "tx": [],
        }
    )

    assert controls["latency"] == 1.0
    assert controls["configured_latency"] == 1.0
    assert "active_latency" not in controls


@pytest.mark.asyncio
async def test_device_settings_operation_preserves_configured_compatibility_latency():
    device = DanteDevice()
    core_client = SimpleNamespace(
        get_device_settings=lambda: {
            "configured_latency_ns": 250_000,
            "active_latency_ns": None,
            "latency_ns": 250_000,
        }
    )
    device._core_client = lambda: core_client

    await device.operations.get_device_settings()

    assert device.latency == 0.25
    assert device.active_latency is None
    assert device.configured_latency == 0.25


def test_standard_latency_choices_are_derived_only_from_advertised_range():
    assert standard_latency_choices_for_range(1.0, 20.3125) == [1.0, 2.0, 5.0]
    assert standard_latency_choices_for_range(0.15, 21.333334) == [0.15, 0.25, 0.5, 1.0, 2.0, 5.0]
    assert standard_latency_choices_for_range(None, 5.0) is None


def test_explicit_unavailable_latency_fields_clear_stale_device_state():
    device = DanteDevice()
    device.latency = 1.0
    device.active_latency = 1.0
    device.configured_latency = 0.25
    device.default_latency = 1.0
    device.min_latency = 0.15
    device.max_latency = 21.333334
    controls = device.controls_data_from_core(
        {
            "name": None,
            "counts": (0, 0, None),
            "aes67": None,
            "settings": {
                "latency_ns": None,
                "configured_latency_ns": None,
                "active_latency_ns": None,
                "default_latency_ns": None,
                "min_latency_ns": None,
                "max_latency_ns": None,
            },
            "rx": [],
            "tx": [],
        }
    )

    device.apply_controls(controls)

    assert device.latency is None
    assert device.active_latency is None
    assert device.configured_latency is None
    assert device.default_latency is None
    assert device.min_latency is None
    assert device.max_latency is None


def test_dante_controller_preset_exports_microseconds_from_milliseconds():
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.latency = 0.15

    xml = format_devices_xml({device.server_name: device})

    assert "<unicast_latency>150</unicast_latency>" in xml


def test_dante_controller_preset_exports_configured_not_active_latency():
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.latency = 1.0
    device.active_latency = 1.0
    device.configured_latency = 0.25

    xml = format_devices_xml({device.server_name: device})

    assert "<unicast_latency>250</unicast_latency>" in xml


def test_dante_controller_preset_omits_latency_when_only_active_value_is_known():
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.latency = 1.0
    device.active_latency = 1.0
    device.configured_latency = None

    xml = format_devices_xml({device.server_name: device})

    assert "<unicast_latency>" not in xml
