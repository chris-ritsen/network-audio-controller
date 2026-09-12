from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from netaudio import DanteDevice
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.performance_configuration import (
    PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
    PROPERTY_RX_FLOW_LATENCY_NS,
    PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET,
    PROPERTY_UNICAST_CONFIGURED_LATENCY_NS,
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
        {"property_id": PROPERTY_RX_FLOW_LATENCY_NS, "flags": 0},
        {"property_id": PROPERTY_RX_FLOW_FRAMES_PER_PACKET, "flags": 0},
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
        PROPERTY_RX_FLOW_LATENCY_NS: 250_000,
        PROPERTY_RX_FLOW_FRAMES_PER_PACKET: 8,
    }

    config = device_preset_config(device, {"audio"})

    assert config["receive_flow_performance"] == {
        "latency_microseconds": 250,
        "frames_per_packet": 8,
    }


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
            {"property_id": PROPERTY_UNICAST_CONFIGURED_LATENCY_NS, "flags": 0},
            {"property_id": PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET, "flags": 0},
        ]
    )
    device.performance_settings = {
        PROPERTY_UNICAST_CONFIGURED_LATENCY_NS: 250_000,
        PROPERTY_UNICAST_CONFIGURED_FRAMES_PER_PACKET: 8,
        PROPERTY_RX_FLOW_LATENCY_NS: 500_000,
    }

    config = device_preset_config(device, {"audio"})

    assert "unicast_performance" not in config


@pytest.mark.asyncio
async def test_preset_plans_fresh_readback_then_stores_only_after_confirmation():
    device = _device()
    expected = {
        PROPERTY_RX_FLOW_LATENCY_NS: 250_000,
        PROPERTY_RX_FLOW_FRAMES_PER_PACKET: 8,
    }
    application = type("Application", (), {})()
    application.get_performance_settings = AsyncMock(
        return_value={PROPERTY_RX_FLOW_LATENCY_NS: 500_000, PROPERTY_RX_FLOW_FRAMES_PER_PACKET: 8}
    )
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
    assert report.unverified == 0
