import hashlib
import json
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from typer.testing import CliRunner

from netaudio import core
from netaudio.commands import flow as flow_commands
from netaudio.dante import flows, multicast
from netaudio.dante.const import SERVICE_ARC
from tests.http_api_test_support import make_device, make_http_server, post


EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "multicast_creation_2809.json").read_text())
AES3_EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "multicast_creation_2809_aes3.json").read_text())
ALLOCATION_EVIDENCE = json.loads(
    (Path(__file__).parent / "fixtures" / "multicast_allocation_2809_aes3.json").read_text()
)


@pytest.mark.parametrize(
    "exchange", EVIDENCE["exchanges"] + AES3_EVIDENCE["exchanges"] + ALLOCATION_EVIDENCE["exchanges"]
)
def test_multicast_creation_matches_digest_bound_exchange(exchange):
    for direction in ("request", "response"):
        record = exchange[direction]
        assert hashlib.sha256(bytes.fromhex(record["hexadecimal"])).hexdigest() == record["sha256"]

    request = core.build_command(
        {
            "command": "create_multicast_flow_2809",
            "channels": exchange["channels"],
            "request_options_word": exchange["request_options_word"],
            "transaction_id": exchange["transaction_id"],
        }
    )
    assert request.hex() == exchange["request"]["hexadecimal"]
    result = core.parse_response("multicast_flow_creation_2809", bytes.fromhex(exchange["response"]["hexadecimal"]))
    assert result == {
        "global_flow_id": 2,
        "media_type_code": 3,
        "media_local_flow_id": 2,
        "channels": exchange["channels"],
    }


@pytest.mark.parametrize("offset,value", [(0, 0x27), (6, 0x36), (9, 0), (18, 1), (35, 0), (41, 1), (47, 0)])
def test_multicast_creation_rejects_unverified_response_variants(offset, value):
    response = bytearray.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])
    response[offset] = value
    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("multicast_flow_creation_2809", bytes(response))


def test_multicast_creation_rejects_every_truncation():
    response = bytes.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])
    for length in range(len(response)):
        with pytest.raises(core.NetaudioCoreError):
            core.parse_response("multicast_flow_creation_2809", response[:length])


def test_multicast_creation_requires_explicit_options():
    with pytest.raises(core.NetaudioCoreError):
        core.build_command({"command": "create_multicast_flow_2809", "channels": [1]})


def test_cli_allocation_defaults_to_zero_options(monkeypatch):
    run = Mock()
    monkeypatch.setattr(flow_commands, "run_command", run)
    result = CliRunner().invoke(flow_commands.app, ["allocate", "--channels", "1,2", "--confirmed"])
    assert result.exit_code == 0, result.output
    run.assert_called_once_with(flow_commands.run_flow_allocate, [1, 2], 0)


def test_cli_allocation_still_requires_confirmation(monkeypatch):
    run = Mock()
    monkeypatch.setattr(flow_commands, "run_command", run)
    result = CliRunner().invoke(flow_commands.app, ["allocate", "--channels", "1"])
    assert result.exit_code != 0
    assert "--confirmed is required" in result.output
    run.assert_not_called()


def allocation_device():
    return SimpleNamespace(
        ipv4="192.0.2.10",
        services={"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.9"}}},
        tx_channels={1: object(), 2: object()},
        topology_mutation_lock=asyncio.Lock(),
        _arc_port=lambda: 4440,
        call_core=AsyncMock(),
    )


def allocated_flow():
    return {
        "global_flow_id": 2,
        "media_type_code": 3,
        "media_local_flow_id": 2,
        "flow_type": "multicast",
        "populated_transmitter_channel_ids": [1, 2],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{}, {"request_options_word": 1}, {"request_options_word": 113}])
async def test_allocation_is_single_attempt_and_verified_under_device_lock(monkeypatch, options):
    device = allocation_device()
    snapshots = iter([{"max_flow_slots": 2, "flows": []}, {"max_flow_slots": 2, "flows": [allocated_flow()]}])

    async def query(*arguments, **options):
        assert device.topology_mutation_lock.locked()
        assert arguments == ("192.0.2.10", 4440, 0x2809)
        assert options == {"device": device}
        return next(snapshots)

    async def call(operation, **options):
        assert device.topology_mutation_lock.locked()
        assert options == {"request_attempts": 1}
        client = SimpleNamespace(execute=lambda specification: specification)
        specification = operation(client)
        assert specification == {
            "command": "create_multicast_flow_2809",
            "channels": [1, 2],
            "request_options_word": expected_options,
        }
        return bytes.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])

    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    device.call_core.side_effect = call
    expected_options = options.get("request_options_word", 0)
    result = await multicast.create_multicast_flow_2809(device, [1, 2], **options)
    assert result == {"flow_protocol_id": 0x2809, "flow": allocated_flow(), "verified": True}
    device.call_core.assert_awaited_once()
    assert not device.topology_mutation_lock.locked()


@pytest.mark.asyncio
async def test_allocation_refuses_existing_media_local_identifier(monkeypatch):
    device = allocation_device()
    query = AsyncMock(return_value={"max_flow_slots": 2, "flows": [allocated_flow()]})
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    with pytest.raises(flows.FlowValidationError, match="already in use"):
        await multicast.create_multicast_flow_2809(device, [1, 2], 0x71)
    device.call_core.assert_not_awaited()


@pytest.mark.asyncio
async def test_allocation_does_not_retry_after_lost_response(monkeypatch):
    device = allocation_device()
    device.call_core.return_value = None
    monkeypatch.setattr(flows, "query_tx_flow_inventory", AsyncMock(return_value={"max_flow_slots": 2, "flows": []}))
    with pytest.raises(flows.FlowValidationError, match="outcome is unknown"):
        await multicast.create_multicast_flow_2809(device, [1, 2], 0x71)
    device.call_core.assert_awaited_once()


@pytest.mark.asyncio
async def test_allocation_does_not_claim_success_without_matching_readback(monkeypatch):
    device = allocation_device()
    device.call_core.return_value = bytes.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])
    monkeypatch.setattr(flows, "query_tx_flow_inventory", AsyncMock(return_value={"max_flow_slots": 2, "flows": []}))
    with pytest.raises(flows.FlowValidationError, match="exactly one new flow"):
        await multicast.create_multicast_flow_2809(device, [1, 2], 0x71)
    device.call_core.assert_awaited_once()


@pytest.mark.asyncio
async def test_allocation_reports_changed_preexisting_flow_state(monkeypatch):
    device = allocation_device()
    device.call_core.return_value = bytes.fromhex(EVIDENCE["exchanges"][0]["response"]["hexadecimal"])
    previous = {**allocated_flow(), "global_flow_id": 1, "media_local_flow_id": 1, "flow_type": "unicast"}
    monkeypatch.setattr(
        flows,
        "query_tx_flow_inventory",
        AsyncMock(
            side_effect=[
                {"max_flow_slots": 2, "flows": [previous]},
                {"max_flow_slots": 2, "flows": [allocated_flow()]},
            ]
        ),
    )
    with pytest.raises(flows.FlowValidationError, match="existing flow state changed"):
        await multicast.create_multicast_flow_2809(device, [1, 2], 0x71)
    device.call_core.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", ["2.8.15", "2.8.1", "2.7.41", None])
async def test_allocation_requires_the_observed_revision(monkeypatch, revision):
    device = allocation_device()
    device.services["arc"]["properties"]["arcp_vers"] = revision
    query = AsyncMock()
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    with pytest.raises(flows.FlowValidationError, match="requires advertised ARC"):
        await multicast.create_multicast_flow_2809(device, [1], 1)
    query.assert_not_awaited()
    device.call_core.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{}, {"request_options_word": 0}, {"request_options_word": 113}])
async def test_http_allocation_returns_verified_assigned_flow(monkeypatch, options):
    device = make_device()
    result = {"flow_protocol_id": 0x2809, "flow": allocated_flow(), "verified": True}
    allocate = AsyncMock(return_value=result)
    monkeypatch.setattr(multicast, "create_multicast_flow_2809", allocate)
    status, body = await post(
        make_http_server({"dev1": device}),
        "/flows/allocate",
        {
            "device": "dev1",
            "channels": [1, 2],
            "confirmed": True,
            **options,
        },
    )
    assert status == 200
    assert body == {"success": True, **result}
    allocate.assert_awaited_once_with(device, [1, 2], options.get("request_options_word", 0))


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [None, True, False, 0.0, 1.0, "0", 2, -1, 65536])
async def test_allocation_invalid_options_fail_before_io(monkeypatch, options):
    device = allocation_device()
    query = AsyncMock()
    monkeypatch.setattr(flows, "query_tx_flow_inventory", query)
    with pytest.raises(flows.FlowValidationError, match="request_options_word"):
        await multicast.create_multicast_flow_2809(device, [1], options)
    query.assert_not_awaited()
    device.call_core.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("parameters", [{"confirmed": False}, {"confirmed": True, "flow_slot": 2}])
async def test_http_allocation_rejects_missing_confirmation_and_explicit_global_slot(monkeypatch, parameters):
    allocate = AsyncMock()
    monkeypatch.setattr(multicast, "create_multicast_flow_2809", allocate)
    status, _ = await post(
        make_http_server({"dev1": make_device()}),
        "/flows/allocate",
        {
            "device": "dev1",
            "channels": [1],
            "request_options_word": 1,
            **parameters,
        },
    )
    assert status == 400
    allocate.assert_not_awaited()
