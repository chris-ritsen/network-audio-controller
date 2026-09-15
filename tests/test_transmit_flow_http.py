from __future__ import annotations

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from netaudio.daemon.http import configuration
from netaudio.dante.transmit_flow import FlowLifecycleState, FlowOperationResult
from tests.http_api_test_support import get, make_device, make_http_server, post


def canonical_specification():
    return {
        "schema_version": 1,
        "media_mode": "native_dante",
        "flow_type": "multicast",
        "name": None,
        "channel_slots": [{"slot": 1, "transmitter_channel": 1}],
        "sample_rate_hz": 48_000,
        "encoding_bits": 24,
        "frames_per_packet": None,
        "primary_destination": None,
        "secondary_destination": None,
        "redundancy": "device_default",
        "identity": {"global_flow_id": 2, "media_type_code": None, "media_local_flow_id": None},
        "protocol": {
            "protocol_id": 0x2729,
            "protocol_version": None,
            "cohort": "legacy_2729",
            "required_capabilities": [],
        },
        "raw_fields": {},
    }


def server():
    device = make_device()
    device.online = True
    device.is_locked = False
    device.flow_protocol_id = 0x2729
    device.transmit_flow_authoring_capability_word = 0
    device.transmit_flow_authoring_opcode = 0x2201
    device.transmit_flow_authoring_protocol_id = 0x2729
    device.receiver_flow_inventory_opcode = 0x3200
    device.tx_channels = {1: object()}
    device.sample_rate = 48_000
    device.encoding = 24
    return make_http_server({"dev1": device})


@pytest.mark.asyncio
async def test_http_plan_uses_the_canonical_specification_without_mutation():
    instance = server()

    status, payload = await post(
        instance,
        "/transmit-flows/plan",
        {"device": "dev1", "specification": canonical_specification()},
    )

    assert status == 200
    assert payload["plan"]["supported"] is True
    assert payload["plan"]["serializer_cohort"] == "legacy_2729_explicit_slot_multicast"


@pytest.mark.asyncio
async def test_http_plan_preserves_modern_rtp_authoring_scope_and_preconditions():
    instance = server()
    device = instance.application.devices["dev1"]
    device.flow_protocol_id = 0x2809
    device.transmit_flow_authoring_capability_word = 0x1000
    device.transmit_flow_authoring_opcode = 0x2601
    device.transmit_flow_authoring_protocol_id = 0x2809
    device.receiver_flow_inventory_opcode = 0x3600
    source = canonical_specification()
    source.update(
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
    original = deepcopy(source)

    status, payload = await post(
        instance,
        "/transmit-flows/plan",
        {"device": "dev1", "specification": source},
    )

    assert status == 200
    assert source == original
    assert payload["plan"]["supported"] is True
    assert payload["plan"]["serializer_cohort"] == "modern_2809_static_rtp_aes67"
    assert payload["plan"]["specification"] == source
    assert payload["plan"]["state_preconditions"] == {"sample_rate_hz": 48_000, "encoding_bits": 24}
    assert "identity.media_local_flow_id" in payload["plan"]["wire_authored_fields"]
    assert "sample_rate_hz" not in payload["plan"]["wire_authored_fields"]
    assert "encoding_bits" not in payload["plan"]["wire_authored_fields"]


@pytest.mark.asyncio
async def test_http_inspect_returns_canonical_inventory(monkeypatch):
    instance = server()
    inventory = {
        "schema_version": 1,
        "flow_protocol_id": 0x2729,
        "max_flow_slots": 4,
        "reported_flow_count": 0,
        "flows": [],
        "unparsed_records": [],
    }
    inspect = AsyncMock(return_value=inventory)
    monkeypatch.setattr(configuration, "inspect_transmit_flows", inspect)

    status, payload = await get(instance, "/transmit-flows/dev1")

    assert status == 200
    assert payload == {"device": "dev1", **inventory}
    inspect.assert_awaited_once_with(instance.application.devices["dev1"])


@pytest.mark.asyncio
async def test_http_create_preserves_partial_completion_as_accepted(monkeypatch):
    instance = server()
    result = FlowOperationResult(
        operation="create",
        state=FlowLifecycleState.PARTIAL,
        transport="direct",
        request_acknowledgement={"received": True, "accepted": True, "result_code": 1},
        device_confirmation=None,
        persistence_confirmation=None,
        effective_state_confirmation=None,
        requested=None,
        effective=None,
        comparison=None,
        message="request acknowledged; readback unavailable",
    )
    create = AsyncMock(return_value=result)
    monkeypatch.setattr(configuration, "create_transmit_flow", create)

    status, payload = await post(
        instance,
        "/transmit-flows/create",
        {"device": "dev1", "specification": canonical_specification(), "confirmed": True},
    )

    assert status == 202
    assert payload["state"] == "partial"
    assert payload["request_acknowledgement"]["accepted"] is True
    assert payload["device_confirmation"] is None
    assert payload["effective_state_confirmation"] is None


@pytest.mark.asyncio
async def test_http_canonical_mutations_require_confirmation(monkeypatch):
    instance = server()
    create = AsyncMock()
    delete = AsyncMock()
    monkeypatch.setattr(configuration, "create_transmit_flow", create)
    monkeypatch.setattr(configuration, "delete_transmit_flow", delete)

    assert (
        await post(
            instance,
            "/transmit-flows/create",
            {"device": "dev1", "specification": canonical_specification()},
        )
    )[0] == 400
    assert (await post(instance, "/transmit-flows/delete", {"device": "dev1", "flow_id": 2}))[0] == 400
    create.assert_not_awaited()
    delete.assert_not_awaited()
