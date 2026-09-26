from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from netaudio import core
from netaudio.cli_support.output import format_devices_xml
from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.latency import (
    latency_choices,
    latency_state_from_settings,
    milliseconds_to_microseconds,
    nanoseconds_to_milliseconds,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "milliseconds,configured,acknowledgement,state",
    [
        (0.15, 150_000, "27ff000a000011010001", "confirmed"),
        (0.15, 250_000, "27ff000a000011010001", "unverified"),
        (0.15, None, "27ff000a000011010001", "unavailable"),
        (0.15, 150_000, "27ff000a000011010002", "rejected"),
        (0.15, 150_000, "", "unavailable"),
        (0.0000005, 1, "27ff000a000011010001", "confirmed"),
    ],
)
async def test_latency_completion_uses_acknowledgement_and_configured_readback(
    milliseconds, configured, acknowledgement, state
):
    application = DanteApplication()
    application.mutate_and_wait_for_notification = AsyncMock(return_value=bytes.fromhex(acknowledgement))
    application.get_latency_settings = AsyncMock(
        return_value={"active_latency_ns": 1_000_000, "configured_latency_ns": configured}
    )

    result = await application.set_latency(SimpleNamespace(), milliseconds)

    assert result["state"] == state
    assert result["effective_state_confirmed"] is (state == "confirmed")
    if state == "confirmed":
        assert result["configured_latency_ns"] == configured
        assert result["requested_latency_ns"] == configured
    if state == "rejected" or not acknowledgement:
        application.get_latency_settings.assert_not_awaited()


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
            "counts": {
                "tx_count": 0,
                "rx_count": 0,
                "locked": None,
                "transmit_flow_authoring_capability_word": 0,
                "receiver_telemetry_capacity": None,
            },
            "aes67": None,
            "settings": {
                "performance_values": [],
                "sample_rate": 48_000,
                "default_latency_ns": 1_000_000,
                "configured_latency_ns": 250_000,
                "active_latency_ns": 150_000,
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


def test_configured_latency_is_effective_when_active_is_unavailable():
    controls = DanteDevice().controls_data_from_core(
        {
            "name": None,
            "counts": {
                "tx_count": 0,
                "rx_count": 0,
                "locked": None,
                "transmit_flow_authoring_capability_word": 0,
                "receiver_telemetry_capacity": None,
            },
            "aes67": None,
            "settings": {
                "performance_values": [],
                "configured_latency_ns": 250_000,
                "active_latency_ns": None,
            },
            "rx": [],
            "tx": [],
        }
    )

    assert controls["latency"] == 0.25
    assert controls["active_latency"] is None
    assert controls["configured_latency"] == 0.25


@pytest.mark.asyncio
async def test_device_settings_operation_uses_configured_latency_when_active_is_unavailable():
    device = DanteDevice()
    core_client = SimpleNamespace(
        get_device_settings=lambda: {
            "configured_latency_ns": 250_000,
            "active_latency_ns": None,
        }
    )
    device.ipv4 = "192.0.2.10"

    async def call_core(operation, **_options):
        return operation(core_client)

    device.call_core = call_core

    await DanteApplication().get_device_settings(device)

    assert device.latency == 0.25
    assert device.active_latency is None
    assert device.configured_latency == 0.25


@pytest.mark.parametrize(
    "settings,choices,effective",
    [
        (
            {"min_latency_ns": 150000, "max_latency_ns": 5000000, "active_latency_ns": 750000},
            [0.15, 0.25, 0.5, 1.0, 2.0, 5.0],
            0.75,
        ),
        (
            {
                "min_latency_ns": 250000,
                "max_latency_ns": 0,
                "active_latency_ns": 250000,
                "configured_latency_ns": 1000000,
            },
            [0.25],
            0.25,
        ),
        ({"min_latency_ns": 0, "max_latency_ns": 0, "configured_latency_ns": 1000000}, [1.0], 1.0),
        ({"min_latency_ns": None, "max_latency_ns": 5000000, "active_latency_ns": 250000}, None, 0.25),
        (
            {"min_latency_ns": 0, "max_latency_ns": 0, "active_latency_ns": None, "configured_latency_ns": None},
            [],
            None,
        ),
    ],
)
def test_native_latency_configuration_matches_device_controls_and_choices(settings, choices, effective):
    configuration = core.latency_configuration(settings)

    assert configuration["state"].get("latency_options_ms") == choices
    assert configuration["controls"]["latency"] == effective
    assert latency_state_from_settings(settings) == configuration["state"]

    device = DanteDevice()
    device.apply_controls(configuration["controls"])
    assert device.standard_latency_choices == choices
    assert device.latency == effective


@pytest.mark.parametrize("value", [-1, True, 1.5, 4294967296, "250000"])
def test_native_latency_configuration_rejects_values_that_are_not_wire_nanoseconds(value):
    with pytest.raises(core.NetaudioCoreError):
        core.latency_configuration({"configured_latency_ns": value})


def test_device_without_a_usable_range_offers_only_its_current_latency():
    assert latency_choices(0.0, 999.0, 6.0, 6.0) == [6.0]
    assert latency_choices(0.25, 0.0, 0.25, 1.0) == [0.25]
    assert latency_choices(0.25, 0.0, None, 1.0) == [1.0]
    assert latency_choices(0.0, 0.0, None, None) == []
    assert latency_choices(1.0, 20.3125, 10.0, 10.0) == [1.0, 2.0, 5.0]
    assert latency_choices(None, 5.0, 1.0, 1.0) is None


def test_latency_state_marks_a_fixed_latency_as_the_only_choice():
    state = latency_state_from_settings(
        {
            "active_latency_ns": 250_000,
            "configured_latency_ns": 1_000_000,
            "min_latency_ns": 250_000,
            "max_latency_ns": 0,
        }
    )
    assert state["latency_options_ms"] == [0.25]
    assert state["latency_options_source"] == "device_reports_no_usable_range"
    assert state["active_latency_is_standard_choice"] is True
    assert state["configured_latency_is_standard_choice"] is False
    assert state["configured_latency_within_reported_range"] is False


def test_latency_state_preserves_raw_units_bounds_choices_and_off_list_status():
    state = latency_state_from_settings(
        {
            "default_latency_ns": 1_000_000,
            "configured_latency_ns": 250_000,
            "active_latency_ns": 750_000,
            "min_latency_ns": 150_000,
            "max_latency_ns": 5_000_000,
        }
    )

    assert state == {
        "active_latency_ms": 0.75,
        "active_latency_ns": 750_000,
        "configured_latency_ms": 0.25,
        "configured_latency_ns": 250_000,
        "default_latency_ms": 1.0,
        "default_latency_ns": 1_000_000,
        "min_latency_ms": 0.15,
        "min_latency_ns": 150_000,
        "max_latency_ms": 5.0,
        "max_latency_ns": 5_000_000,
        "latency_options_ms": [0.15, 0.25, 0.5, 1.0, 2.0, 5.0],
        "latency_options_ns": [150_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000],
        "latency_options_source": "controller_fixed_set_filtered_by_reported_range",
        "active_latency_is_standard_choice": False,
        "active_latency_within_reported_range": True,
        "configured_latency_is_standard_choice": True,
        "configured_latency_within_reported_range": True,
    }


def test_capture_backed_avio_bounds_filter_controller_latency_choices(load_fixture):
    from netaudio import core

    settings = core.parse_response("device_settings", load_fixture("core_device_settings_avio-aes3-1.bin"))
    state = latency_state_from_settings(settings)

    assert state["min_latency_ns"] == 1_000_000
    assert state["max_latency_ns"] == 20_312_500
    assert state["latency_options_ms"] == [1.0, 2.0, 5.0]
    assert state["latency_options_ns"] == [1_000_000, 2_000_000, 5_000_000]


@pytest.mark.asyncio
async def test_latency_settings_operation_uses_the_focused_latency_query(load_fixture):

    device = DanteDevice()
    response = load_fixture("core_latency_config_avio-aes3-1.bin")
    device.execute = AsyncMock(return_value=response)

    settings = await DanteApplication().get_latency_settings(device)

    device.execute.assert_awaited_once_with({"command": "query_latency_config"})
    assert settings["active_latency_ns"] == 1_000_000
    assert settings["min_latency_ns"] == 1_000_000
    assert settings["max_latency_ns"] == 20_312_500


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
            "counts": {
                "tx_count": 0,
                "rx_count": 0,
                "locked": None,
                "transmit_flow_authoring_capability_word": 0,
                "receiver_telemetry_capacity": None,
            },
            "aes67": None,
            "settings": {
                "performance_values": [],
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
