from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.commands import flow as flow_commands
from netaudio.dante.commands import DanteCommands
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante import flows
from netaudio.dante.flows import (
    FlowValidationError,
    external_receiver_subscription_specification,
    subscribe_external_rtp,
)
from netaudio.dante.sap import SapFlowInventory
from tests.cli_test_support import invoke
from tests.http_api_test_support import get, make_device, make_http_server, post

FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "external_subscription"
SAP_FIXTURE = Path(__file__).parent / "fixtures" / "sap_sdp" / "synthetic-aes67-announcement.bin"

LEGACY_SPEC = {
    "command": "subscribe_external_rtp",
    "device_protocol": 0x2729,
    "message_id": 0x1234,
    "receiver_channel_ids": [1, 2, 17],
    "flow_slot_assignments": [1, 2, 0],
    "advertised_flow_slot_count": 2,
    "source_address": "192.0.2.44",
    "session_id": 0x0102030405060708,
    "clock_offset": 0x11223344,
    "primary_destination": {"address": "239.69.1.10", "port": 5004},
    "advertisement_supports_multiple_interfaces": False,
    "receiver_supports_multiple_interfaces": False,
}

MODERN_SPEC = {
    "command": "subscribe_external_rtp",
    "device_protocol": 0x280F,
    "message_id": 0x5678,
    "receiver_channel_ids": [1, 3, 12, 20],
    "flow_slot_assignments": [1, 2, 3, 4],
    "advertised_flow_slot_count": 4,
    "source_address": "192.0.2.44",
    "session_id": 0x0102030405060708,
    "clock_offset": 0x11223344,
    "primary_destination": {"address": "239.69.1.10", "port": 0},
    "secondary_destination": {"address": "239.69.1.11", "port": 5006},
    "advertisement_supports_multiple_interfaces": True,
    "receiver_supports_multiple_interfaces": True,
}


@pytest.mark.parametrize(
    ("fixture_name", "specification"),
    [("legacy-2729-3201.bin", LEGACY_SPEC), ("modern-2809-3201.bin", MODERN_SPEC)],
)
def test_external_subscription_digest_bound_goldens(fixture_name, specification):
    provenance = json.loads((FIXTURE_DIRECTORY / "provenance.json").read_text())
    expected = (FIXTURE_DIRECTORY / fixture_name).read_bytes()

    assert hashlib.sha256(expected).hexdigest() == provenance["fixtures"][fixture_name]["sha256"]
    assert core.build_command(specification) == expected


def test_external_subscription_modern_packet_caps_protocol_and_retains_opcode():
    packet = core.build_command(MODERN_SPEC)

    assert packet[:2] == bytes.fromhex("2809")
    assert packet[6:8] == bytes.fromhex("3201")
    assert packet[8:12] == bytes.fromhex("00000202")
    assert packet[8 + 0x12 : 8 + 0x1E] == bytes.fromhex("00200028004c005000540058")


def test_command_frontends_expose_the_same_external_subscription_specification():
    commands = DanteCommands()
    commands._sequence = 0x1233
    specification = commands.subscribe_external_rtp(
        device_protocol=LEGACY_SPEC["device_protocol"],
        receiver_channel_ids=LEGACY_SPEC["receiver_channel_ids"],
        flow_slot_assignments=LEGACY_SPEC["flow_slot_assignments"],
        advertised_flow_slot_count=LEGACY_SPEC["advertised_flow_slot_count"],
        source_address=LEGACY_SPEC["source_address"],
        session_id=LEGACY_SPEC["session_id"],
        clock_offset=LEGACY_SPEC["clock_offset"],
        primary_destination=LEGACY_SPEC["primary_destination"],
    )
    assert specification == {
        **{key: value for key, value in LEGACY_SPEC.items() if key != "message_id"},
        "sequence": 0x1234,
    }

    packet, service = DanteDeviceCommands().command_subscribe_external_rtp(
        device_protocol=LEGACY_SPEC["device_protocol"],
        receiver_channel_ids=LEGACY_SPEC["receiver_channel_ids"],
        flow_slot_assignments=LEGACY_SPEC["flow_slot_assignments"],
        advertised_flow_slot_count=LEGACY_SPEC["advertised_flow_slot_count"],
        source_address=LEGACY_SPEC["source_address"],
        session_id=LEGACY_SPEC["session_id"],
        clock_offset=LEGACY_SPEC["clock_offset"],
        primary_destination=LEGACY_SPEC["primary_destination"],
        transaction_id=0x1234,
    )
    assert packet == (FIXTURE_DIRECTORY / "legacy-2729-3201.bin").read_bytes()
    assert service == SERVICE_ARC


def discovered_flow():
    inventory = SapFlowInventory()
    change = inventory.ingest(
        SAP_FIXTURE.read_bytes(),
        announcement_interface="eth0",
        packet_source_ipv4="192.0.2.44",
        received_monotonic=1,
        wall_time=1,
    )
    return change.flow


def device(*, protocol="2.8.9", rx_channels=None, managed=False):
    return SimpleNamespace(
        name="Receiver",
        server_name="receiver.local.",
        ipv4="192.0.2.10",
        requires_managed_control=managed,
        services={
            "arc": {
                "type": SERVICE_ARC,
                "properties": {"arcp_vers": protocol},
            }
        },
        rx_channels={number: object() for number in ([1, 2] if rx_channels is None else rx_channels)},
        topology_mutation_lock=DeferredAsyncioLock(),
        execute=AsyncMock(),
    )


def test_discovered_flow_maps_to_external_subscription_and_gates_secondary_destination():
    commands = DanteCommands()
    commands._sequence = 7
    flow = discovered_flow()
    target = device()

    primary_only = external_receiver_subscription_specification(
        commands,
        target,
        flow,
        [1, 2],
        [1, 2],
        receiver_supports_multiple_interfaces=False,
    )
    assert primary_only["sequence"] == 8
    assert primary_only["source_address"] == "192.0.2.44"
    assert primary_only["session_id"] == 123456789012
    assert primary_only["clock_offset"] == 17
    assert primary_only["primary_destination"] == {"address": "239.69.1.10", "port": 5004}
    assert primary_only.get("secondary_destination") is None
    assert primary_only["advertisement_supports_multiple_interfaces"] is True

    both_interfaces = external_receiver_subscription_specification(
        commands,
        target,
        flow,
        [1, 2],
        [1, 2],
        receiver_supports_multiple_interfaces=True,
    )
    assert both_interfaces["secondary_destination"] == {"address": "239.69.1.11", "port": 5004}


@pytest.mark.parametrize(
    ("receiver_ids", "assignments", "message"),
    [
        ([1], [], "parallel non-empty"),
        ([1, 1], [1, 2], "must be unique"),
        ([1, 2], [1, 3], "advertised slot count"),
        ([1, 3], [1, 2], "receiver channel not found"),
    ],
)
def test_high_level_external_mapping_fails_closed(receiver_ids, assignments, message):
    with pytest.raises(FlowValidationError, match=message):
        external_receiver_subscription_specification(
            DanteCommands(),
            device(),
            discovered_flow(),
            receiver_ids,
            assignments,
            receiver_supports_multiple_interfaces=False,
        )


@pytest.mark.parametrize(
    ("receiver_ids", "assignments"),
    [
        ([2, 1], [2, 1]),
        ([1, 2], [1, 1]),
        ([1, 2], [0, 0]),
    ],
)
def test_high_level_external_mapping_accepts_bitmap_supported_forms(receiver_ids, assignments):
    specification = external_receiver_subscription_specification(
        DanteCommands(),
        device(),
        discovered_flow(),
        receiver_ids,
        assignments,
        receiver_supports_multiple_interfaces=False,
    )

    assert specification["receiver_channel_ids"] == receiver_ids
    assert specification["flow_slot_assignments"] == assignments


@pytest.mark.asyncio
async def test_external_subscription_reports_arc_sdp_and_media_evidence_separately(monkeypatch):
    target = device()
    target.execute.return_value = bytes.fromhex("280900144a263410000100000000000001000000")
    application = SimpleNamespace(commands=DanteCommands(), external_flows=populated_inventory())
    target.application = application
    expected_identity = {
        "receiver_channel": 1,
        "flow_slot": 1,
        "source_ipv4": "192.0.2.44",
        "session_id": 123456789012,
        "interface_endpoints": [
            {"ipv4_address": "239.69.1.10", "udp_port": 5004},
            {"ipv4_address": "239.69.1.11", "udp_port": 5004},
        ],
    }
    after = {
        "result_code": 1,
        "page_disposition": "complete",
        "maximum_flow_slots": 4,
        "reported_flow_count": 1,
        "flows": [
            {
                "external_identity": {"source_ipv4": "192.0.2.44", "session_id": 123456789012},
                "effective_subscription_identities": [
                    expected_identity,
                    {**expected_identity, "receiver_channel": 2, "flow_slot": 2},
                ],
                "sdp_correlation": {"matched": True},
            }
        ],
    }
    inventories = iter(
        (
            {**after, "reported_flow_count": 0, "flows": []},
            after,
        )
    )
    monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", AsyncMock(side_effect=inventories))

    result = await subscribe_external_rtp(
        application,
        target,
        discovered_flow(),
        [1, 2],
        [1, 2],
        receiver_supports_multiple_interfaces=True,
    )

    assert result["request_acknowledged"] is True
    assert result["result_code"] == 1
    assert result["arc_effective_state_confirmed"] is True
    assert result["sdp_correlation_confirmed"] is True
    assert result["rtp_packet_reception_confirmed"] is None
    assert result["clock_lock_confirmed"] is None
    assert result["persistence_confirmed"] is None
    assert result["decoded_audio_confirmed"] is None
    target.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_external_subscription_does_not_send_without_complete_fresh_baseline(monkeypatch):
    target = device()
    application = SimpleNamespace(commands=DanteCommands(), external_flows=populated_inventory())
    target.application = application
    monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", AsyncMock(return_value=None))

    result = await subscribe_external_rtp(
        application,
        target,
        discovered_flow(),
        [1, 2],
        [1, 2],
        receiver_supports_multiple_interfaces=True,
    )

    assert result["mutation_sent"] is False
    assert result["request_acknowledged"] is False
    assert result["arc_effective_state_confirmed"] is None
    target.execute.assert_not_awaited()


def test_managed_only_device_is_rejected_before_build_or_send():
    target = device(managed=True)
    with pytest.raises(FlowValidationError, match="managed-only"):
        external_receiver_subscription_specification(
            DanteCommands(),
            target,
            discovered_flow(),
            [1, 2],
            [1, 2],
            receiver_supports_multiple_interfaces=True,
        )


def test_receiver_subscription_requires_known_receiver_channel_inventory():
    target = device(rx_channels=[])
    with pytest.raises(FlowValidationError, match="receiver channel not found"):
        external_receiver_subscription_specification(
            DanteCommands(),
            target,
            discovered_flow(),
            [1],
            [1],
            receiver_supports_multiple_interfaces=False,
        )


def populated_inventory() -> SapFlowInventory:
    inventory = SapFlowInventory()
    inventory.ingest(
        SAP_FIXTURE.read_bytes(),
        announcement_interface="eth0",
        packet_source_ipv4="192.0.2.44",
        received_monotonic=1,
        wall_time=1,
    )
    return inventory


def acknowledged_result() -> dict:
    return {
        "result_code": 1,
        "request_acknowledged": True,
        "arc_effective_state_confirmed": None,
        "sdp_correlation_confirmed": None,
        "rtp_packet_reception_confirmed": None,
        "clock_lock_confirmed": None,
        "persistence_confirmed": None,
        "decoded_audio_confirmed": None,
        "mutation_sent": True,
        "message": "request acknowledged but complete fresh ARC readback was unavailable",
        "flow_identity": {"source_ipv4": "192.0.2.44", "session_id": 123456789012},
        "receiver_channel_ids": [1, 2],
        "flow_slot_assignments": [1, 2],
    }


def test_cli_external_list_reports_discovered_flow_without_device_discovery():
    application = SimpleNamespace(external_flows=populated_inventory())

    result = invoke(flow_commands.run_external_flow_list, application, {}, 0)

    assert result.exit_code == 0
    assert result.exception is None
    assert "Studio Feed" in result.output
    assert "239.69.1.10:5004" in result.output


def test_cli_external_subscribe_reports_acknowledgement_without_claiming_media():
    target = device()
    application = SimpleNamespace(
        external_flows=populated_inventory(),
        subscribe_external_rtp=AsyncMock(return_value=acknowledged_result()),
    )

    result = invoke(
        flow_commands.run_external_flow_subscribe,
        application,
        {target.server_name: target},
        "192.0.2.44",
        123456789012,
        [1, 2],
        [1, 2],
        True,
        0,
    )

    assert result.exit_code == 0
    assert result.exception is None
    assert "acknowledged" in result.output
    assert result.output.count("not confirmed") == 6
    application.subscribe_external_rtp.assert_awaited_once()


@pytest.mark.asyncio
async def test_http_external_flow_inventory_and_subscription_preserve_verification_boundaries():
    target = make_device(server_name="receiver.local.", name="Receiver", ipv4="192.0.2.10")
    server = make_http_server({target.server_name: target})
    server.application.external_flows = populated_inventory()
    server.application.subscribe_external_rtp.return_value = acknowledged_result()

    get_status, inventory = await get(server, "/external-flows")
    post_status, result = await post(
        server,
        "/external-flows/subscribe",
        {
            "rx_device": "receiver.local.",
            "source_ipv4": "192.0.2.44",
            "session_id": 123456789012,
            "receiver_channel_ids": [1, 2],
            "flow_slot_assignments": [1, 2],
            "receiver_supports_multiple_interfaces": True,
        },
    )

    assert get_status == 200
    assert inventory["192.0.2.44/123456789012"]["flow_name"] == "Studio Feed"
    assert post_status == 202
    assert result["success"] is True
    assert result["request_acknowledged"] is True
    assert result["arc_effective_state_confirmed"] is None
    assert result["sdp_correlation_confirmed"] is None
    assert result["rtp_packet_reception_confirmed"] is None
    assert result["clock_lock_confirmed"] is None
    assert result["persistence_confirmed"] is None
    assert result["decoded_audio_confirmed"] is None
    server.application.subscribe_external_rtp.assert_awaited_once()
