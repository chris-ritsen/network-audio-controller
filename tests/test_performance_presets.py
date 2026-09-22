from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from netaudio import DanteDevice, core
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.performance_configuration import (
    PerformanceOperationResult,
)
from netaudio.presets.loading import MatchedPresetDevice, apply_preset_plan, build_preset_plan
from netaudio.presets.parsing import parse_preset_xml
from netaudio.presets.schema import normalize_device_config
from netaudio.presets.serialization import device_preset_config, format_preset_configs


def _device() -> DanteDevice:
    device = DanteDevice("desk.local.")
    device.name = "Desk"
    device.platform_software_version = "3.0.0"
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.9"}}}
    device.settings_properties = [
        {"property_id": 0x8301, "flags": 0},
        {"property_id": 0x0310, "flags": 0},
    ]
    return device


def test_schema_v3_round_trips_typed_performance_fields():
    config = normalize_device_config(
        {
            "name": "Desk",
            "receive_flow_performance": {
                "latency_microseconds": 250,
                "frames_per_packet": 8,
            },
            "receive_flow_default_slots": 4,
        }
    )
    xml = format_preset_configs({"Desk": config}, sections={"audio"})
    _, parsed = parse_preset_xml(xml)

    assert 'schema_version="3"' in xml
    assert parsed["Desk"]["receive_flow_performance"] == config["receive_flow_performance"]
    assert parsed["Desk"]["receive_flow_default_slots"] == 4


def test_device_preset_save_projects_observed_performance_values():
    device = _device()
    device.performance_settings = {
        0x8301: 250_000,
        0x0310: 8,
    }

    config = device_preset_config(device, {"audio"})

    assert config["receive_flow_performance"] == {
        "latency_microseconds": 250,
        "frames_per_packet": 8,
    }


def test_empty_native_settings_refresh_removes_stale_performance_from_presets():
    device = _device()
    device.performance_settings = {0x8301: 250000, 0x0310: 8}
    packet = bytes.fromhex("2809000c0001110000010200")
    controls = device.controls_data_from_core(
        {
            "name": None,
            "counts": (0, 0, None, None),
            "settings": core.parse_response("device_settings", packet),
            "rx": [],
            "tx": [],
            "channels_included": False,
        }
    )
    device.apply_controls(controls)

    config = device_preset_config(device, {"audio"})

    assert device.performance_settings == {}
    assert "receive_flow_performance" not in config
    assert "unicast_performance" not in config


@pytest.mark.parametrize(
    "operation,values,expected",
    [
        (
            "receive_flow_performance",
            {0x8301: 250000, 0x0310: 8},
            {"latency_microseconds": 250, "frames_per_packet": 8},
        ),
        (
            "transmit_flow_performance",
            {0x8204: 500000, 0x0210: 4},
            {"latency_microseconds": 500, "frames_per_packet": 4},
        ),
        ("unicast_performance", {0x8205: 1000000, 0x0211: 16}, {"latency_microseconds": 1000, "frames_per_packet": 16}),
        ("receive_flow_default_slots", {0x0303: 4}, 4),
    ],
)
def test_native_performance_snapshot_round_trips_through_presets_and_command_plan(operation, values, expected):
    snapshot = core.performance_snapshot({"property_ids": list(values), "values": values})
    assert snapshot[operation] == expected

    device = _device()
    device.settings_properties = [{"property_id": property_id, "flags": 0} for property_id in values]
    device.performance_settings = values
    xml = format_preset_configs({"Desk": device_preset_config(device, {"audio"})}, sections={"audio"})
    _, parsed = parse_preset_xml(xml)
    payload = parsed["Desk"][operation]
    assert payload == expected

    specification = {
        "command": f"set_{operation}",
        "negotiated_protocol_id": 0x2809,
        "supported_property_ids": list(values),
        **({"default_slots": payload} if operation == "receive_flow_default_slots" else payload),
    }

    if operation in ("receive_flow_performance", "unicast_performance"):
        specification["platform_software_version"] = [3, 0, 0]

    plan = core.plan_performance_command(specification)
    assert {entry["property_id"]: entry["value"] for entry in plan} == values


@pytest.mark.parametrize(
    "properties,values",
    [
        ([], {0x8301: 250000, 0x0310: 8}),
        ([0x8301, 0x0310], {0x8301: 250000}),
        ([0x8301, 0x0310], {0x8301: 250001, 0x0310: 8}),
        ([0x8301, 0x0310], {0x8301: -1000, 0x0310: 8}),
        ([0x8301, 0x0310], {0x8301: 250000, 0x0310: 65536}),
        ([0x8301, 0x0310], {0x8301: 250000, 0x0310: True}),
        ([0x0303], {0x0303: 65536}),
        ([0x8304], {0x8304: 1000}),
    ],
)
def test_native_performance_snapshot_omits_unknown_incomplete_or_unrepresentable_settings(properties, values):
    assert core.performance_snapshot({"property_ids": properties, "values": values}) == {}


def test_device_preset_preserves_distinct_version_namespaces_and_provenance():
    device = _device()
    device.product_name = "Product"
    device.platform_model_name = "Platform"
    device.platform_software_version = "4.2.4.1"
    device.platform_hardware_version = "4.2.3.4"
    device.product_version = "1.3.4"
    device.friendly_product_version = "Release 1.3.4"
    device.manufacturer_software_version = "7.8.9"
    device.manufacturer_firmware_version = "2.3.4"
    device.cmc_server_version = "2.8.2"
    device.router_protocol_version = "4.0.2"
    device.ddm_product_version = "1.3.5"
    device.ddm_dante_version = "4.2.5.1"
    device.field_sources = {
        "platform_software_version": "conmon_platform_record",
        "product_version": "conmon_manufacturer_record",
        "cmc_server_version": "dns_sd",
        "ddm_dante_version": "ddm",
        "audio_configuration": "direct",
    }

    config = device_preset_config(device, {"audio"})
    identity = config["device_identity"]

    assert identity["platform_software_version"] == "4.2.4.1"
    assert identity["platform_hardware_version"] == "4.2.3.4"
    assert identity["product_version"] == "1.3.4"
    assert identity["friendly_product_version"] == "Release 1.3.4"
    assert identity["manufacturer_software_version"] == "7.8.9"
    assert identity["manufacturer_firmware_version"] == "2.3.4"
    assert identity["cmc_server_version"] == "2.8.2"
    assert identity["router_protocol_version"] == "4.0.2"
    assert identity["ddm_product_version"] == "1.3.5"
    assert identity["ddm_dante_version"] == "4.2.5.1"
    assert identity["field_sources"] == {
        "platform_software_version": "conmon_platform_record",
        "product_version": "conmon_manufacturer_record",
        "cmc_server_version": "dns_sd",
        "ddm_dante_version": "ddm",
    }


def test_device_preset_does_not_collapse_incomplete_or_disagreeing_unicast_properties():
    device = _device()
    assert device.settings_properties is not None
    device.settings_properties.extend(
        [
            {"property_id": 0x8205, "flags": 0},
            {"property_id": 0x0211, "flags": 0},
        ]
    )
    device.performance_settings = {
        0x8205: 250_000,
        0x0211: 8,
        0x8301: 500_000,
    }

    config = device_preset_config(device, {"audio"})

    assert "unicast_performance" not in config


@pytest.mark.asyncio
async def test_preset_plans_fresh_readback_then_stores_only_after_confirmation():
    device = _device()
    expected = {
        0x8301: 250_000,
        0x0310: 8,
    }
    application = type("Application", (), {})()
    application.get_performance_settings = AsyncMock(return_value={0x8301: 500_000, 0x0310: 8})
    application.set_receive_flow_performance = AsyncMock(
        return_value=PerformanceOperationResult(
            "set_receive_flow_performance",
            "confirmed",
            expected,
            expected,
            {"accepted": True},
            None,
            True,
            None,
            None,
            "fresh readback matched every affected property",
        )
    )
    application.store_current_configuration = AsyncMock(
        return_value=PerformanceOperationResult(
            "store_current_configuration",
            "request_acknowledged",
            {},
            {},
            None,
            None,
            None,
            {"accepted": True},
            None,
            "storage request acknowledged; persistence requires independent confirmation",
        )
    )
    config = {
        "name": "Desk",
        "receive_flow_performance": {
            "latency_microseconds": 250,
            "frames_per_packet": 8,
        },
    }

    plan = await build_preset_plan(
        application,
        [MatchedPresetDevice(config, device, "Desk", "desk.local.")],
    )
    report = await apply_preset_plan(
        application,
        plan,
        store_current_configuration=True,
    )

    assert plan.device_actions[0].actions[0].state.value == "change"
    application.set_receive_flow_performance.assert_awaited_once_with(device, 250, 8)
    application.store_current_configuration.assert_awaited_once_with(device)
    storage = report.operations[-1]
    assert storage.persistence_request_acknowledgement == {"accepted": True}
    assert storage.persistence_confirmation is None
    assert report.failures == 0
    assert report.unverified == 1

    from netaudio.commands.preset.loading import _report_preset_load
    from typer import Exit

    with pytest.raises(Exit) as exit_status:
        _report_preset_load(report)

    assert exit_status.value.exit_code != 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,failures,unverified",
    [
        ("rejected", 1, 0),
        ("contradicted", 1, 0),
        ("request_acknowledged", 0, 1),
        ("unverified", 0, 1),
        ("unknown_result", 0, 1),
    ],
)
async def test_uncertain_or_failed_preset_stops_before_remaining_changes_and_storage(state, failures, unverified):
    from types import SimpleNamespace
    from netaudio.presets.loading import PresetAction, PresetActionState, PresetDeviceActions, PresetLoadPlan

    device = _device()
    result = PerformanceOperationResult(
        "set_receive_flow_performance",
        state,
        {},
        {},
        {"accepted": state != "rejected"},
        None,
        None,
        None,
        None,
        "device outcome",
    )
    application = SimpleNamespace(
        set_receive_flow_performance=AsyncMock(return_value=result),
        store_current_configuration=AsyncMock(),
    )
    plan = PresetLoadPlan(
        [
            PresetDeviceActions(
                actions=[
                    PresetAction(
                        "receive_flow_performance",
                        {"latency_microseconds": value, "frames_per_packet": 8},
                        PresetActionState.CHANGE,
                    )
                    for value in (250, 500)
                ],
                config={},
                device=device,
                device_name=device.name,
                server_name=device.server_name,
            )
        ]
    )

    report = await apply_preset_plan(application, plan, stop_on_failure=True, store_current_configuration=True)

    application.set_receive_flow_performance.assert_awaited_once()
    application.store_current_configuration.assert_not_awaited()
    assert [operation.state for operation in report.operations] == [state]
    assert report.failures == failures
    assert report.unverified == unverified
