from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante import flows
from netaudio.dante.const import RESULT_CODE_SUCCESS, SERVICE_ARC


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, "2.8.16"])
async def test_preferred_transmitter_inventory_does_not_query_unknown_revisions(version):
    device = SimpleNamespace(
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": version}}},
        execute=AsyncMock(),
    )

    assert await flows.query_preferred_tx_flow_inventory("192.0.2.10", 4440, 0x2729, device=device) is None
    device.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_preferred_transmitter_inventory_replays_native_fallback_commands():
    from tests.test_flow_inventory import synthetic_page

    device = SimpleNamespace(
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.41"}}},
        execute=AsyncMock(side_effect=[None, synthetic_page([1])]),
    )
    inventory = await flows.query_preferred_tx_flow_inventory("192.0.2.10", 4440, 0x2729, device=device)
    assert inventory["flow_protocol_id"] == 0x2729
    assert inventory["flows"][0]["flow_number"] == 1
    assert [call.args[0]["flow_protocol_id"] for call in device.execute.await_args_list] == [0x2809, 0x2729]


@pytest.mark.asyncio
async def test_preferred_transmitter_inventory_uses_modern_capture_without_fallback():
    response = (Path(__file__).parent / "fixtures/transmit_flow_lifecycle/modern-2809-create-readback.bin").read_bytes()
    device = SimpleNamespace(
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.9"}}},
        execute=AsyncMock(return_value=response),
    )

    inventory = await flows.query_preferred_tx_flow_inventory("192.0.2.10", 4440, 0x2809, device=device)
    assert inventory["flow_protocol_id"] == 0x2809
    assert inventory["flows"][0]["global_flow_id"] == 2
    device.execute.assert_awaited_once_with(
        {"command": "query_tx_flows", "flow_protocol_id": 0x2809, "starting_flow": 1}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("family", [None, "unrecognized"])
async def test_receiver_query_does_not_guess_missing_or_unknown_capabilities(monkeypatch, family):
    modern_query = AsyncMock(return_value={"page_disposition": "complete", "flows": []})
    legacy_query = AsyncMock()
    monkeypatch.setattr(flows, "query_receiver_flow_inventory", legacy_query)
    device = SimpleNamespace(
        application=SimpleNamespace(query_modern_arc_receiver_flow_status=modern_query),
        receiver_flow_inventory_family=family,
        requires_managed_control=False,
    )

    assert await flows.query_preferred_receiver_flow_inventory(device) is None
    modern_query.assert_not_awaited()
    legacy_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_rejected_modern_receiver_query_never_falls_back_to_legacy(monkeypatch):
    modern_query = AsyncMock(side_effect=RuntimeError("device rejected query"))
    legacy_query = AsyncMock()
    monkeypatch.setattr(flows, "query_receiver_flow_inventory", legacy_query)
    device = SimpleNamespace(
        application=SimpleNamespace(query_modern_arc_receiver_flow_status=modern_query),
        receiver_flow_inventory_family="modern",
        requires_managed_control=False,
    )

    assert await flows.query_preferred_receiver_flow_inventory(device) is None
    modern_query.assert_awaited_once_with(device)
    legacy_query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, "2.8.16", "garbage"])
async def test_flow_detection_does_not_probe_guessed_revisions(monkeypatch, version):
    device = SimpleNamespace(
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": version}}},
    )
    request = AsyncMock(return_value=b"unexpected reply")
    monkeypatch.setattr(flows, "_request", request)
    monkeypatch.setattr(core, "parse_response", lambda *_: RESULT_CODE_SUCCESS)

    assert await flows.detect_flow_protocol("192.0.2.10", 4440, device=device) is None
    request.assert_not_awaited()


@pytest.mark.asyncio
async def test_flow_protocol_detection_accepts_more_pages(monkeypatch):
    from tests.test_flow_inventory import synthetic_page

    command_specifications = []

    async def request(device_ip, arc_port, command_specification, timeout_ms, attempts, device=None):
        command_specifications.append(command_specification)
        return synthetic_page([1], more=True)

    monkeypatch.setattr(flows, "_request", request)

    device = SimpleNamespace(services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.41"}}})
    flow_protocol_id = await flows.detect_flow_protocol("192.0.2.10", 4440, device=device)

    assert flow_protocol_id == 0x2729
    assert command_specifications == [{"command": "query_tx_flows", "flow_protocol_id": 0x2729, "starting_flow": 1}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flow_pages",
    [
        ({"maximum_flow_slots": 32, "flows": []},),
        ({"maximum_flow_slots": 32, "flows": [{"flow_number": 32}]},),
        (
            {"maximum_flow_slots": 32, "flows": [{"flow_number": 1}]},
            {"maximum_flow_slots": 32, "flows": [{"flow_number": 1}]},
        ),
        (
            {"maximum_flow_slots": 32, "flows": [{"flow_number": 28}]},
            {"maximum_flow_slots": 32, "flows": [{"flow_number": 5}]},
        ),
        (
            {"maximum_flow_slots": 32, "flows": [{"flow_number": 16}]},
            {"maximum_flow_slots": 16, "flows": [{"flow_number": 17}]},
        ),
    ],
)
async def test_flow_query_rejects_invalid_pagination(monkeypatch, flow_pages):
    from tests.test_flow_inventory import synthetic_page

    responses = [
        synthetic_page(
            [record["flow_number"] for record in page["flows"]],
            capacity=page["maximum_flow_slots"],
            more=True,
        )
        for page in flow_pages
    ]
    monkeypatch.setattr(flows, "_request", AsyncMock(side_effect=responses))

    assert await flows.query_tx_flow_inventory("192.0.2.10", 4440, 0x2729) is None


def test_external_identity_correlation_retains_arc_readback_without_sdp():
    inventory = {
        "flows": [
            {
                "external_identity": {"source_ipv4": "192.0.2.44", "session_id": 42},
                "effective_subscription_identities": [
                    {
                        "receiver_channel": 3,
                        "flow_slot": 2,
                        "source_ipv4": "192.0.2.44",
                        "session_id": 42,
                        "interface_endpoints": [{"ipv4_address": "239.69.1.10", "udp_port": 5004}],
                    }
                ],
            }
        ]
    }

    correlated = flows.correlate_receiver_flow_inventory(inventory, None)

    assert correlated["flows"][0]["external_identity"] == inventory["flows"][0]["external_identity"]
    assert correlated["flows"][0]["sdp_correlation"]["matched"] is False
    assert flows.effective_external_subscription_index(correlated)[3][0]["flow_slot"] == 2


@pytest.mark.asyncio
async def test_receiver_inventory_family_follows_channel_count_capability_selection():
    legacy_commands = []

    async def legacy_execute(specification):
        legacy_commands.append(specification)
        return bytes.fromhex("2729000e00003200000101000000")

    modern_page = {
        "result_code": 1,
        "page_disposition": "complete",
        "maximum_flow_slots": 2,
        "reported_flow_count": 0,
        "flows": [],
    }
    modern_query = AsyncMock(return_value=modern_page)
    legacy = SimpleNamespace(
        flow_protocol_id=0x2729,
        application=SimpleNamespace(query_modern_arc_receiver_flow_status=modern_query, external_flows=None),
        receiver_flow_inventory_family="legacy",
        requires_managed_control=False,
        ipv4="192.0.2.10",
        _arc_port=lambda: 4440,
        execute=legacy_execute,
    )
    modern = SimpleNamespace(
        application=SimpleNamespace(query_modern_arc_receiver_flow_status=modern_query, external_flows=None),
        receiver_flow_inventory_family="modern",
        requires_managed_control=False,
    )

    assert (await flows.query_preferred_receiver_flow_inventory(legacy))["page_disposition"] == "complete"
    modern_inventory = await flows.query_preferred_receiver_flow_inventory(modern)

    assert legacy_commands == [{"command": "query_receiver_flows", "starting_flow": 1}]
    assert modern_query.await_count == 1
    assert modern_inventory == modern_page


def _run_without_context(run, *arguments, **options):
    import asyncio

    return asyncio.run(run(None, {}, *arguments, **options))


def _flow_list_device():
    from types import SimpleNamespace

    return SimpleNamespace(
        name="avio-usb-1", server_name="AVIOUSB-1.local.", ipv4="192.0.2.10", flow_protocol_id=0x2809
    )


def test_receiver_flow_list_formats_latency_in_milliseconds(monkeypatch):
    from typer.testing import CliRunner

    from netaudio.cli import OutputFormat, state
    from netaudio.commands import flow as flow_commands

    monkeypatch.setattr(state, "output_format", OutputFormat.plain)
    device = _flow_list_device()

    async def query_inventory(queried_device):
        assert queried_device is device
        return {
            "flows": [
                {
                    "flow_number": 1,
                    "flow_type": "unicast",
                    "receiver_channel_numbers_by_flow_channel": [[1], [2]],
                    "subscription_status_code": 0x0009,
                    "destination_internet_protocol_version_four_address": "192.0.2.10",
                    "destination_user_datagram_port": 14336,
                    "sample_rate": 48000,
                    "encoding": 24,
                    "frames_per_packet": 8,
                    "latency_nanoseconds": 1_000_000,
                }
            ],
            "maximum_flow_slots": 2,
            "page_disposition": "complete",
            "result_code": 1,
        }

    monkeypatch.setattr(flow_commands, "_selected_device", lambda _devices: (device, 4440))
    monkeypatch.setattr(flow_commands, "run_command", _run_without_context)
    monkeypatch.setattr(flows, "query_preferred_receiver_flow_inventory", query_inventory)

    result = CliRunner().invoke(flow_commands.app, ["receiver-list"])

    assert result.exit_code == 0, result.output
    header, row = [line for line in result.output.splitlines() if line.strip()][:2]
    assert "Latency" in header
    assert "Latency (ns)" not in header
    assert "1 ms" in row and "48 kHz" in row and "PCM24" in row
    assert "1000000" not in row
