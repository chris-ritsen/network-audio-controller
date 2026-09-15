from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio.presets.loading import (
    MatchedPresetDevice,
    PresetActionState,
    _plan_redundancy,
    _plan_receiver_subscriptions,
    apply_preset_plan,
    build_preset_plan,
)
from netaudio.presets.parsing import parse_preset_xml
from netaudio.presets.schema import ParsedPresetDevices, normalize_device_config
from netaudio.presets.serialization import device_preset_config, format_preset_configs


def _flow() -> dict:
    return {
        "schema_version": 1,
        "media_mode": "native_dante",
        "flow_type": "multicast",
        "name": None,
        "channel_slots": [
            {"slot": 1, "transmitter_channel": 1, "raw_fields": {"mapping_flags": 7}},
            {"slot": 3, "transmitter_channel": 2, "future_slot_field": "kept"},
        ],
        "sample_rate_hz": 48000,
        "encoding_bits": 24,
        "frames_per_packet": None,
        "primary_destination": None,
        "secondary_destination": None,
        "redundancy": "device_default",
        "identity": {"global_flow_id": 2, "media_type_code": 1, "future_identity": 9},
        "protocol": {
            "protocol_id": 0x2729,
            "protocol_version": None,
            "cohort": "legacy_2729",
            "required_capabilities": [],
            "future_protocol": True,
        },
        "raw_fields": {"extension_hexadecimal": "0102"},
        "future_flow_field": {"value": 3},
    }


def _configuration() -> dict:
    return {
        "name": "Desk",
        "device_name": "Stage-Desk",
        "device_identity": {"server_name": "desk.local.", "mac_address": "00:1D:C1:00:00:01"},
        "preferred_leader": True,
        "external_word_clock": False,
        "sample_rate": 48000,
        "encoding": 24,
        "latency": 1.0,
        "sample_rate_pullup": 0,
        "clock_source_code": 2,
        "redundancy_mode": "redundant",
        "interfaces": [
            {"identity": "primary", "mode": "dynamic", "future_interface": "kept"},
            {
                "identity": "secondary",
                "mode": "static",
                "ip_address": "192.0.2.10",
                "netmask": "255.255.255.0",
                "gateway": "0.0.0.0",
                "dns_server": "0.0.0.0",
            },
        ],
        "transmitter_channel_names": {1: "Left", 2: "Right"},
        "receiver_channel_names": {1: "Program", 2: "Guest"},
        "transmit_flows": [_flow()],
        "rx_subscriptions": {
            1: {"kind": "native_dante", "tx_channel": "Left", "tx_device": "Source"},
            2: {
                "kind": "external_rtp",
                "flow_identity": {"source_ipv4": "198.51.100.10", "session_id": 42},
                "flow_slot": 1,
                "receiver_supports_multiple_interfaces": True,
                "raw_sdp_sha256": "a" * 64,
            },
        },
        "codec_gain": [{"channel": 1, "device_type": "input", "level": 3, "future_gain": 8}],
        "ha_bridge": {"enabled": True, "vendor_extension": 4},
        "unknown_fields": {
            "xml_attributes": {"vendor": "example"},
            "xml_elements": ['<vendor_setting code="7">opaque</vendor_setting>'],
        },
        "future_category": {"opaque": [1, 2, 3]},
    }


def test_schema_v2_round_trip_preserves_all_categories_and_unknowns():
    devices = ParsedPresetDevices(
        {"Desk": _configuration()},
        source_version="2.1.0",
        root_attributes={"vendorRevision": "9"},
        unknown_root_elements=['<vendor_root value="kept" />'],
    )

    xml = format_preset_configs(devices, preset_name="Complete")
    name, parsed = parse_preset_xml(xml)
    xml_again = format_preset_configs(parsed, preset_name=name)
    second_name, parsed_again = parse_preset_xml(xml_again)

    assert name == second_name == "Complete"
    assert parsed == parsed_again == {"Desk": normalize_device_config(_configuration())}
    assert parsed.source_version == "2.1.0"
    assert parsed.root_attributes == {"vendorRevision": "9"}
    assert len(parsed.unknown_root_elements) == 1
    assert xml.count("<vendor_setting") == xml_again.count("<vendor_setting") == 1


def test_schema_v2_round_trip_preserves_modern_rtp_transmit_flow_fields():
    flow = _flow()
    flow.update(
        {
            "media_mode": "rtp_aes67",
            "name": "RTP Program",
            "frames_per_packet": 48,
            "primary_destination": {"address": "239.69.1.2", "port": 5004, "interface": None},
            "secondary_destination": {"address": "239.69.1.3", "port": 5006, "interface": None},
            "identity": {"global_flow_id": None, "media_type_code": 3, "media_local_flow_id": 7},
            "protocol": {
                "protocol_id": 0x2809,
                "protocol_version": None,
                "cohort": "modern_2809",
                "required_capabilities": [],
            },
            "raw_fields": {"request_options_word": 0},
        }
    )
    configuration = _configuration()
    configuration["transmit_flows"] = [flow]

    xml = format_preset_configs({"Desk": configuration}, preset_name="RTP")
    name, parsed = parse_preset_xml(xml)

    assert name == "RTP"
    assert parsed["Desk"]["transmit_flows"] == [flow]


def test_schema_v2_rejects_conflicting_conventional_projection():
    xml = format_preset_configs({"Desk": _configuration()}, preset_name="Complete")
    xml = xml.replace("<samplerate>48000</samplerate>", "<samplerate>96000</samplerate>")
    with pytest.raises(ValueError, match="conflicting sample_rate"):
        parse_preset_xml(xml)


def _planning_device():
    channels = {
        1: SimpleNamespace(number=1, name="One", friendly_name="One"),
        2: SimpleNamespace(number=2, name="Two", friendly_name="Two"),
    }

    async def get_channels():
        return None

    async def fetch_device_name():
        return "Desk"

    return SimpleNamespace(
        name="Desk",
        server_name="desk.local.",
        mac_address="00:1D:C1:00:00:01",
        sample_rate=48000,
        supported_sample_rates=[48000],
        sample_rate_configuration_supported=True,
        sample_rate_update_mode=2,
        encoding=24,
        supported_encodings=[24],
        encoding_configuration_supported=True,
        encoding_update_mode=2,
        sample_rate_pullup_configuration_supported=True,
        sample_rate_pullup_update_mode=2,
        sample_rate_pullup_flags=0,
        is_locked=False,
        requires_managed_control=False,
        flow_protocol_id=0x2729,
        transmit_flow_authoring_capability_word=0,
        transmit_flow_authoring_opcode=0x2201,
        transmit_flow_authoring_protocol_id=0x2729,
        receiver_flow_inventory_opcode=0x3200,
        transmitter_flows=[],
        tx_channels=channels,
        rx_channels=channels,
        get_tx_channels=get_channels,
        get_rx_channels=get_channels,
        fetch_device_name=fetch_device_name,
        subscriptions=[],
        dante_redundancy={
            "current": "switched",
            "configured": "switched",
            "state_fresh": True,
            "available_modes": [
                {"code": 0, "label": "Switched", "mode": "switched"},
                {"code": 1, "label": "Redundant", "mode": "redundant"},
            ],
            "available_modes_source": "interface_status_flag_cohort",
            "available_modes_fresh": True,
        },
        switch_redundancy_supported=True,
        redundancy_advertised_support_source={"fresh": True, "field_reported": True},
        switch_redundancy_read_only=False,
        redundancy_read_only_source={"fresh": True, "field_reported": True},
        interface_status_protocol=0x0724,
        ipv4="192.0.2.10",
        control_transports=["direct"],
        generic_codec_control_supported=True,
        static_ipv4_configuration_supported=True,
        static_ipv4_configuration_read_only=False,
        interfaces=[],
    )


@pytest.mark.parametrize(
    ("change", "included"),
    [
        (None, True),
        ("unsupported", False),
        ("capability_unknown", False),
        ("state_stale", False),
        ("modes_stale", False),
        ("configured_unknown", False),
    ],
)
def test_preset_capture_requires_fresh_advertised_redundancy(change, included):
    device = _planning_device()
    if change == "unsupported":
        device.switch_redundancy_supported = False
    elif change == "capability_unknown":
        device.redundancy_advertised_support_source = None
    elif change == "state_stale":
        device.dante_redundancy["state_fresh"] = False
    elif change == "modes_stale":
        device.dante_redundancy["available_modes_fresh"] = False
    elif change == "configured_unknown":
        device.dante_redundancy["configured"] = None

    config = device_preset_config(device, {"network"})
    assert (config.get("redundancy_mode") == "switched") is included


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("condition", "expected_state", "reason"),
    [
        ("unsupported", PresetActionState.UNSUPPORTED, "unsupported"),
        ("capability_unknown", PresetActionState.UNAVAILABLE, "capability_unknown"),
        ("state_unavailable", PresetActionState.UNAVAILABLE, "configured redundancy was unavailable"),
        ("read_only", PresetActionState.UNAVAILABLE, "read_only"),
        ("locked", PresetActionState.UNAVAILABLE, "device_locked"),
        ("managed_permission_denied", PresetActionState.UNAVAILABLE, "managed_permission_denied"),
        ("mode_not_advertised", PresetActionState.UNSUPPORTED, "supported redundancy values"),
        ("writable", PresetActionState.CHANGE, "fresh readback differs"),
    ],
)
async def test_preset_plan_distinguishes_redundancy_availability(condition, expected_state, reason):
    from netaudio.dante.network_configuration import redundancy_snapshot

    device = _planning_device()
    if condition == "unsupported":
        device.switch_redundancy_supported = False
    elif condition == "capability_unknown":
        device.redundancy_advertised_support_source = None
    elif condition == "state_unavailable":
        device.dante_redundancy = None
    elif condition == "read_only":
        device.switch_redundancy_read_only = True
    elif condition == "locked":
        device.is_locked = True
    elif condition == "managed_permission_denied":
        device.requires_managed_control = True
        device.control_transports = ["ddm"]
        device.managed_operation_permissions = {"redundancy": False}
    elif condition == "mode_not_advertised":
        device.dante_redundancy["available_modes"] = [{"code": 0, "label": "Switched", "mode": "switched"}]

    application = SimpleNamespace(probe_dante_redundancy=AsyncMock(return_value=redundancy_snapshot(device)))
    action = await _plan_redundancy(application, device, "redundant")
    assert action.state is expected_state
    assert reason in action.reason


@pytest.mark.asyncio
async def test_plan_orders_supported_changes_and_preserves_unsupported_categories(monkeypatch):
    device = _planning_device()
    application = SimpleNamespace(
        probe_dante_redundancy=AsyncMock(
            return_value={
                "advertised_support": True,
                "current_mode": "switched",
                "configured_mode": "switched",
                "available_modes": [
                    {"code": 0, "label": "Switched", "mode": "switched"},
                    {"code": 1, "label": "Redundant", "mode": "redundant"},
                ],
                "available_modes_fresh": True,
            }
        ),
        probe_sample_rate_status=AsyncMock(
            return_value={"current_value": 48000, "available_values": [48000], "update_mode": 2}
        ),
        probe_encoding_status=AsyncMock(return_value={"current_value": 24, "available_values": [24], "update_mode": 2}),
        get_device_settings=AsyncMock(return_value={"active_latency_ns": 1_000_000}),
        probe_preferred_leader_state=AsyncMock(return_value=False),
        probe_sample_rate_pullup_status=AsyncMock(
            return_value={"current_value": 0, "available_values": [0, 1], "update_mode": 2}
        ),
        probe_clocking_status=AsyncMock(return_value={"clock_source_code": 0}),
        resolve_channel_name_protocol_identifier=AsyncMock(return_value=0x2809),
        probe_gain_adapter=AsyncMock(return_value=("input", [3, 3])),
        probe_interface_status=AsyncMock(
            return_value=[
                {"interface": "primary", "configured": {"mode": "dynamic"}},
                {
                    "interface": "secondary",
                    "configured": {
                        "mode": "static",
                        "ip_address": "192.0.2.10",
                        "netmask": "255.255.255.0",
                        "gateway": "0.0.0.0",
                        "dns_server": "0.0.0.0",
                    },
                },
            ]
        ),
    )
    monkeypatch.setattr(
        "netaudio.presets.loading.inspect_transmit_flows",
        AsyncMock(return_value={"flows": []}),
    )
    plan = await build_preset_plan(application, [MatchedPresetDevice(_configuration(), device, "Desk", "desk.local.")])
    actions = plan.device_actions[0].actions
    kinds = [action.kind for action in actions]

    assert kinds.index("redundancy") < kinds.index("sample_rate")
    assert kinds.index("sample_rate") < kinds.index("transmit_flow")
    assert kinds.index("transmit_flow") < kinds.index("receiver_subscriptions")
    assert kinds.index("receiver_subscriptions") < kinds.index("interface")
    assert kinds.count("receiver_subscriptions") == 1
    assert next(action for action in actions if action.kind == "transmit_flow").state is PresetActionState.CHANGE
    assert (
        next(action for action in actions if action.kind == "external_word_clock").state
        is PresetActionState.UNSUPPORTED
    )
    assert next(action for action in actions if action.kind == "ha_bridge").state is PresetActionState.UNSUPPORTED
    assert (
        next(action for action in actions if action.kind == "unknown:future_category").state
        is PresetActionState.UNSUPPORTED
    )
    assert len([action for action in actions if action.kind == "interface"]) == 2
    by_kind = {action.kind: action for action in actions if action.kind != "interface"}
    assert by_kind["device_name"].current == "Desk"
    assert by_kind["redundancy"].current == "switched"
    assert by_kind["sample_rate"].state is PresetActionState.UNCHANGED
    assert by_kind["encoding"].state is PresetActionState.UNCHANGED
    assert by_kind["latency"].current == 1.0
    assert by_kind["preferred_leader"].current is False
    assert by_kind["sample_rate_pullup"].current == 0
    assert by_kind["clock_source_code"].current == 0
    assert by_kind["transmitter_channel_names"].current == {1: "One", 2: "Two"}
    assert by_kind["receiver_channel_names"].current == {1: "One", 2: "Two"}
    assert by_kind["receiver_subscriptions"].current == {1: None}
    assert by_kind["external_receiver_subscriptions"].state is PresetActionState.UNAVAILABLE
    assert by_kind["codec_gain"].current == {
        "channel": 1,
        "device_type": "input",
        "level": 3,
    }
    assert all(action.state is PresetActionState.UNCHANGED for action in actions if action.kind == "interface")


@pytest.mark.asyncio
async def test_unchanged_fresh_value_is_not_scheduled_or_written():
    device = _planning_device()
    application = SimpleNamespace(
        probe_sample_rate_status=AsyncMock(
            return_value={
                "current_value": 48000,
                "requested_value": 48000,
                "available_values": [48000, 96000],
                "update_mode": 2,
            }
        ),
        set_sample_rate=AsyncMock(),
    )
    plan = await build_preset_plan(
        application,
        [MatchedPresetDevice({"sample_rate": 48000}, device, "Desk", "desk.local.")],
    )

    [action] = plan.device_actions[0].actions
    assert action.state is PresetActionState.UNCHANGED
    assert action.current == action.payload == 48000
    report = await apply_preset_plan(application, plan)
    application.set_sample_rate.assert_not_awaited()
    assert report.operations[0].state == "unchanged"


@pytest.mark.asyncio
async def test_external_subscription_is_unchanged_from_arc_identity_without_sdp(monkeypatch):
    from netaudio.dante import flows

    device = _planning_device()
    device.application = SimpleNamespace(external_flows=None)
    desired = {
        "rx_subscriptions": {
            2: {
                "kind": "external_rtp",
                "flow_identity": {"source_ipv4": "198.51.100.10", "session_id": 42},
                "flow_slot": 1,
                "interface_endpoints": [{"ipv4_address": "239.69.1.10", "udp_port": 5004}],
            }
        }
    }
    receiver_inventory = {
        "result_code": 1,
        "page_disposition": "complete",
        "flows": [
            {
                "external_identity": {"source_ipv4": "198.51.100.10", "session_id": 42},
                "sdp_correlation": {"matched": False},
                "effective_subscription_identities": [
                    {
                        "receiver_channel": 2,
                        "flow_slot": 1,
                        "source_ipv4": "198.51.100.10",
                        "session_id": 42,
                        "interface_endpoints": [{"ipv4_address": "239.69.1.10", "udp_port": 5004}],
                    }
                ],
            }
        ],
    }
    monkeypatch.setattr(
        flows,
        "query_preferred_receiver_flow_inventory",
        AsyncMock(return_value=receiver_inventory),
    )

    [action] = await _plan_receiver_subscriptions(device, "Desk", desired)

    assert action.kind == "external_receiver_subscriptions"
    assert action.state is PresetActionState.UNCHANGED


@pytest.mark.asyncio
async def test_unavailable_fresh_readback_is_not_promoted_to_a_mutation():
    device = _planning_device()
    application = SimpleNamespace(
        probe_sample_rate_status=AsyncMock(side_effect=RuntimeError("synthetic timeout")),
        set_sample_rate=AsyncMock(),
    )
    plan = await build_preset_plan(
        application,
        [MatchedPresetDevice({"sample_rate": 96000}, device, "Desk", "desk.local.")],
    )

    [action] = plan.device_actions[0].actions
    assert action.state is PresetActionState.UNAVAILABLE
    assert action.current is None
    report = await apply_preset_plan(application, plan)
    application.set_sample_rate.assert_not_awaited()
    assert report.operations[0].state == "unavailable"
    assert report.unverified == 1
