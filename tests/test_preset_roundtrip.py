from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.network_configuration import NetworkConfigurationUnverified

from netaudio.presets.loading import (
    MatchedPresetDevice,
    PresetActionState,
    PresetLoadReport,
    _apply_external_receiver_subscriptions,
    _apply_interface,
    _apply_audio_setting,
    _apply_codec_gain,
    _plan_interfaces,
    _plan_codec_gain,
    _plan_latency,
    _plan_redundancy,
    _plan_receiver_subscriptions,
    apply_preset_plan,
    build_preset_plan,
)
from netaudio.presets.parsing import parse_preset_xml
from netaudio.presets.schema import ParsedPresetDevices, normalize_device_config
from netaudio.presets.serialization import device_preset_config, format_preset_configs


@pytest.mark.asyncio
@pytest.mark.parametrize("supported,expected", [([1, 2, 3, 4, 5], "confirmed"), ([], "failed")])
async def test_gain_preset_requires_a_supported_readback_level(supported, expected):
    application = SimpleNamespace(
        set_gain_level=AsyncMock(
            return_value={"device_type": "input", "channel_levels": [3], "supported_levels": supported}
        )
    )
    report = PresetLoadReport()
    context = SimpleNamespace(application=application, report=report)
    entry = SimpleNamespace(device=SimpleNamespace(), device_name="Desk")
    action = SimpleNamespace(kind="codec_gain", payload={"channel": 1, "level": 3, "device_type": "input"})

    await _apply_codec_gain(context, entry, action)

    [result] = report.operations
    assert result.state == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [250_000, 1_000_000, None])
async def test_latency_preset_plans_and_verifies_configured_not_active_state(configured):
    settings = {"active_latency_ns": 1_000_000, "configured_latency_ns": configured}
    application = SimpleNamespace(
        get_device_settings=AsyncMock(return_value=settings),
        get_latency_settings=AsyncMock(return_value=settings),
        set_latency=AsyncMock(return_value=core.latency_control(1.0, settings, True)),
    )
    device = SimpleNamespace()
    plan = await _plan_latency(application, device, 1.0)
    expected = {
        250_000: PresetActionState.CHANGE,
        1_000_000: PresetActionState.UNCHANGED,
        None: PresetActionState.UNAVAILABLE,
    }

    assert plan.state is expected[configured]

    report = PresetLoadReport()
    context = SimpleNamespace(application=application, report=report)
    entry = SimpleNamespace(device=device, device_name="Desk", config={"latency": 1.0})
    await _apply_audio_setting(context, entry, SimpleNamespace(kind="latency", payload=1.0))

    [result] = report.operations
    assert result.state == ("confirmed" if configured == 1_000_000 else "failed")
    assert result.effective == configured
    application.get_device_settings.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", [16, 24, None])
async def test_encoding_preset_uses_application_readback_without_a_second_probe(observed):
    application = SimpleNamespace(
        set_encoding=AsyncMock(return_value={"current_value": observed} if observed is not None else None),
        probe_encoding_status=AsyncMock(side_effect=RuntimeError("no second readback")),
    )
    report = PresetLoadReport()
    context = SimpleNamespace(application=application, report=report)
    entry = SimpleNamespace(device=SimpleNamespace(), device_name="Desk", config={"encoding": 16})
    action = SimpleNamespace(kind="encoding", payload=16)

    await _apply_audio_setting(context, entry, action)

    [result] = report.operations
    assert result.state == ("confirmed" if observed == 16 else "failed")
    assert result.effective == observed
    application.probe_encoding_status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("record", [{"mode": "dynamic"}, {"interface": "primary", "mode": "dynamic"}])
async def test_interface_plan_does_not_guess_identity_or_configured_state(record):
    application = SimpleNamespace(probe_interface_status=AsyncMock(return_value=[record]))
    config = {"interfaces": [{"identity": "primary", "mode": "dynamic"}]}

    [action] = await _plan_interfaces(application, _planning_device(), config)

    assert action.state is PresetActionState.UNAVAILABLE


@pytest.mark.asyncio
@pytest.mark.parametrize("unverified", [False, True])
async def test_interface_preset_uses_the_verified_setter_result_without_another_probe(unverified):
    configured = {"mode": "dynamic"}
    application = SimpleNamespace(
        set_interface=AsyncMock(return_value=[{"interface": "primary", "configured": configured}]),
        probe_interface_status=AsyncMock(side_effect=RuntimeError("a second probe is unavailable")),
    )

    if unverified:
        application.set_interface.side_effect = NetworkConfigurationUnverified("fresh readback unavailable")

    report = PresetLoadReport()
    context = SimpleNamespace(application=application, report=report)
    entry = SimpleNamespace(device=SimpleNamespace(interface_reboot_required=False), device_name="Desk")
    action = SimpleNamespace(kind="interface", payload={"identity": "primary", "mode": "dhcp", "configuration": None})

    await _apply_interface(context, entry, action)

    [result] = report.operations
    assert result.state == ("acknowledged" if unverified else "confirmed")
    assert result.effective == (None if unverified else configured)
    assert result.effective_state_confirmation is (None if unverified else True)
    application.probe_interface_status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "readback,expected", [("matching", True), ("conflicting", False), ("changed_endpoint", False), ("missing", None)]
)
async def test_external_preset_completion_verifies_full_intent_not_command_confirmation(readback, expected):
    subscription = {
        "flow_identity": {"source_ipv4": "198.51.100.10", "session_id": 42},
        "flow_slot": 1,
        "interface_endpoints": [{"ipv4_address": "239.69.1.10", "udp_port": 5004}],
    }
    identity = {
        "receiver_channel": 2,
        "flow_slot": 1,
        **subscription["flow_identity"],
        "interface_endpoints": subscription["interface_endpoints"],
    }
    identities = [identity]

    if readback == "conflicting":
        identities.append({**identity, "session_id": 43})
    elif readback == "changed_endpoint":
        identities[0] = {**identity, "interface_endpoints": [{"ipv4_address": "239.69.1.99", "udp_port": 5004}]}

    inventory = {
        "result_code": 1,
        "page_disposition": "complete",
        "flows": [{"effective_subscription_identities": identities}],
    }
    operation = AsyncMock(
        return_value={
            "request_acknowledged": True,
            "arc_effective_state_confirmed": True,
            "receiver_flow_after": None if readback == "missing" else inventory,
        }
    )
    report = PresetLoadReport()
    context = SimpleNamespace(
        application=SimpleNamespace(
            external_flows=SimpleNamespace(get=lambda *_: object()), subscribe_external_rtp=operation
        ),
        report=report,
    )
    entry = SimpleNamespace(device=_planning_device(), device_name="Desk")
    action = SimpleNamespace(kind="external_receiver_subscriptions", payload=[(2, subscription)])

    await _apply_external_receiver_subscriptions(context, entry, action)

    [result] = report.operations
    assert result.state == ("confirmed" if expected is True else "failed" if expected is False else "acknowledged")
    assert result.effective_state_confirmation is expected
    assert operation.await_count == 1


@pytest.mark.asyncio
async def test_gain_preset_reports_direction_mismatch_without_reinterpreting_the_native_adapter():
    adapter = {"device_type": "input", "channel_levels": [3, 3], "supported_levels": [1, 2, 3, 4, 5]}
    application = SimpleNamespace(probe_gain_adapter=AsyncMock(return_value=adapter))
    requested = {"channel": 1, "device_type": "output", "level": 2}

    [action] = await _plan_codec_gain(application, _planning_device(), [requested])

    assert action.state is PresetActionState.UNSUPPORTED
    assert action.payload == requested
    assert "input" in action.reason


@pytest.mark.parametrize("media_mode", [None, "rtp_aes67"])
def test_preset_export_keeps_observed_transmitter_media_mode(media_mode):
    from pathlib import Path
    from netaudio import core
    from netaudio.dante.device import DanteDevice

    response = (Path(__file__).parent / "fixtures/transmit_flow_lifecycle/modern-2809-create-readback.bin").read_bytes()
    record = core.parse_response("transmitter_flow_status_page", response)["flows"][0]

    if media_mode is not None:
        record["media_mode"] = media_mode

    device = DanteDevice("desk.local.")
    device.name = "Desk"
    device.flow_protocol_id = 0x2809
    device.apply_transmitter_flow_status_page({"reported_flow_count": 1, "flows": [record]})
    exported = device_preset_config(device, {"routing"})
    assert exported["transmit_flows"][0]["media_mode"] == (media_mode or "unknown")
    assert exported["transmit_flows"][0]["raw_fields"] == record


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
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.41"}}},
        transmit_flow_authoring_capability_word=0,
        transmit_flow_authoring=core.flow_authoring_capabilities(0)["transmit_flow_authoring"],
        receiver_flow_inventory_family="legacy",
        routing_capacity_transmit_channel_count=32,
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
            return_value={
                "current_value": 48000,
                "requested_value": 48000,
                "available_values": [48000],
                "update_mode": 2,
            }
        ),
        probe_encoding_status=AsyncMock(
            return_value={"current_value": 24, "requested_value": 24, "available_values": [24], "update_mode": 2}
        ),
        get_latency_settings=AsyncMock(
            return_value={"active_latency_ns": 1_000_000, "configured_latency_ns": 1_000_000}
        ),
        probe_preferred_leader_state=AsyncMock(return_value=False),
        probe_sample_rate_pullup_status=AsyncMock(
            return_value={
                "current_value": 0,
                "requested_value": 0,
                "available_values": [0, 1],
                "update_mode": 2,
                "flags": 0,
                "host_disabled": False,
            }
        ),
        preview_clock_configuration=AsyncMock(
            return_value={
                "before": {"clock_source": 0, "preferred_leader": False},
                "changes": {"clock_source": 2, "preferred_leader": True},
            }
        ),
        resolve_channel_name_protocol_identifier=AsyncMock(return_value=0x2809),
        probe_gain_adapter=AsyncMock(
            return_value={"device_type": "input", "channel_levels": [3, 3], "supported_levels": [1, 2, 3, 4, 5]}
        ),
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
    configuration = _configuration()
    configuration["transmit_flows"][0]["channel_slots"][1]["slot"] = 2
    plan = await build_preset_plan(application, [MatchedPresetDevice(configuration, device, "Desk", "desk.local.")])
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
    assert by_kind["clock_configuration"].current["preferred_leader"] is False
    assert by_kind["sample_rate_pullup"].current == 0
    assert by_kind["clock_configuration"].current["clock_source"] == 0
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
@pytest.mark.parametrize("mode", [0, 2, 77])
@pytest.mark.parametrize("choices", [[], [48000, 96000]])
async def test_unchanged_fresh_value_is_not_scheduled_or_written(mode, choices):
    device = _planning_device()
    application = SimpleNamespace(
        probe_sample_rate_status=AsyncMock(
            return_value={
                "current_value": 48000,
                "requested_value": 48000,
                "available_values": choices,
                "update_mode": mode,
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
async def test_preset_pullup_plan_uses_fresh_native_host_disable_state():
    import struct

    from netaudio import core
    from tests.test_services import SAMPLE_RATE_PULLUP_STATUS_PACKET

    packet = bytearray(SAMPLE_RATE_PULLUP_STATUS_PACKET)
    struct.pack_into(">I", packet, 52, 1)
    observed = core.parse_response("sample_rate_pullup_status", bytes(packet))
    device = _planning_device()
    device.sample_rate_pullup_host_disabled = False
    application = SimpleNamespace(probe_sample_rate_pullup_status=AsyncMock(return_value=observed))

    plan = await build_preset_plan(
        application,
        [MatchedPresetDevice({"sample_rate_pullup": 2}, device, "Desk", "desk.local.")],
    )

    [action] = plan.device_actions[0].actions
    assert action.state is PresetActionState.UNAVAILABLE
    assert "host_disabled" in action.reason
    assert device.sample_rate_pullup_host_disabled is True


@pytest.mark.asyncio
@pytest.mark.parametrize("readback", ["matching", "duplicate", "conflicting", "incomplete", "malformed"])
async def test_external_subscription_plan_requires_unambiguous_complete_arc_identity(monkeypatch, readback):
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
    identities = receiver_inventory["flows"][0]["effective_subscription_identities"]

    if readback == "duplicate":
        identities.append(dict(identities[0]))
    elif readback == "conflicting":
        identities.append({**identities[0], "session_id": 43})
    elif readback == "incomplete":
        receiver_inventory["page_disposition"] = "more_pages"
    elif readback == "malformed":
        identities.append({"receiver_channel": 2})

    monkeypatch.setattr(
        flows,
        "query_preferred_receiver_flow_inventory",
        AsyncMock(return_value=receiver_inventory),
    )

    [action] = await _plan_receiver_subscriptions(device, "Desk", desired)

    assert action.kind == "external_receiver_subscriptions"
    assert (
        action.state
        is {
            "matching": PresetActionState.UNCHANGED,
            "duplicate": PresetActionState.CHANGE,
            "conflicting": PresetActionState.CHANGE,
            "incomplete": PresetActionState.UNAVAILABLE,
            "malformed": PresetActionState.UNAVAILABLE,
        }[readback]
    )


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
