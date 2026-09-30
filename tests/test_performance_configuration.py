from __future__ import annotations

from types import SimpleNamespace

import pytest

from netaudio import core
from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.performance_configuration import (
    performance_operation_availability,
    get_performance_settings,
    requested_performance_properties,
    set_receive_flow_default_slots,
    set_receive_flow_performance,
    set_transmit_flow_performance,
    set_unicast_performance,
    store_current_configuration,
)


def _arc_response(opcode: int, body: bytes = b"", result_code: int = 1) -> bytes:
    length = 10 + len(body)
    return (
        b"\x28\x09"
        + length.to_bytes(2, "big")
        + b"\x00\x01"
        + opcode.to_bytes(2, "big")
        + result_code.to_bytes(2, "big")
        + body
    )


def _settings_response(properties: dict[int, int]) -> bytes:
    count = len(properties)
    body = bytearray(count.to_bytes(2, "big"))
    referenced = [(property_id, value) for property_id, value in properties.items() if property_id & 0x8000]
    first_pointer = 10 + 2 + count * 4
    referenced_index = 0
    for property_id, value in properties.items():
        body.extend(property_id.to_bytes(2, "big"))
        if property_id & 0x8000:
            body.extend((first_pointer + referenced_index * 4).to_bytes(2, "big"))
            referenced_index += 1
        else:
            body.extend(value.to_bytes(2, "big"))
    for _, value in referenced:
        body.extend(value.to_bytes(4, "big"))
    return _arc_response(0x1100, bytes(body))


class FakeDevice:
    def __init__(self, supported, responses, *, platform_software_version="3.0.0", managed=False):
        self.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.8.15"}}}
        self.settings_properties = [{"property_id": property_id, "flags": 0} for property_id in supported]
        self.platform_software_version = platform_software_version
        self.requires_managed_control = managed
        self.topology_mutation_lock = DeferredAsyncioLock()
        self.performance_settings = None
        self.specifications = []
        self._responses = iter(responses)

    async def call_core(self, operation, request_attempts=None):
        assert request_attempts == 1
        client = SimpleNamespace(execute=self._execute)
        return operation(client)

    def _execute(self, specification):
        self.specifications.append(specification)
        return next(self._responses)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "properties", [[True], [-1], [65536], ["1"], pytest.param([0x8301] * 32768, id="oversized-packet")]
)
async def test_invalid_performance_query_preserves_cache_and_never_sends(properties):
    device = FakeDevice([], [])
    device.performance_settings = {0x8301: 250000}

    with pytest.raises((ValueError, core.NetaudioCoreError)):
        await get_performance_settings(device, properties)

    assert device.performance_settings == {0x8301: 250000}
    assert device.specifications == []


def test_native_performance_readback_preserves_width_and_leaves_unknown_properties_uninterpreted():
    packet = _settings_response({0x8301: 0xFFFFFFFF, 0x0310: 0xFFFF, 0x9999: 0x12345678})

    parsed = core.parse_response("device_settings", packet)

    assert parsed["performance_values"] == [
        {"property_id": 0x8301, "value": 0xFFFFFFFF},
        {"property_id": 0x0310, "value": 0xFFFF},
    ]
    assert any(entry["info_code"] == 0x9999 for entry in parsed["referenced_values"])


@pytest.mark.parametrize("protocol", [0, 0x2602, 0x280B, 0x2810, 0xFFFF])
@pytest.mark.parametrize(
    "command,fields",
    [
        ("query_performance_settings", {"property_ids": [0x8301]}),
        ("store_current_configuration", {}),
        ("set_receive_flow_default_slots", {"default_slots": 8, "supported_property_ids": [0x0303]}),
        (
            "set_transmit_flow_performance",
            {"latency_microseconds": 250, "frames_per_packet": 4, "supported_property_ids": [0x8204, 0x0210]},
        ),
    ],
)
def test_performance_commands_do_not_guess_unknown_negotiated_revisions(protocol, command, fields):
    with pytest.raises(core.NetaudioCoreError):
        core.build_command({"command": command, "negotiated_protocol_id": protocol, **fields})


@pytest.mark.parametrize(
    "protocol,wire_protocol",
    [
        (0x2601, 0x2601),
        (0x2729, 0x2729),
        (0x27FF, 0x27FF),
        (0x2801, 0x2801),
        (0x2809, 0x2809),
        (0x280A, 0x2809),
        (0x280F, 0x2809),
    ],
)
def test_performance_commands_preserve_explicitly_supported_negotiated_revisions(protocol, wire_protocol):
    packet = core.build_command(
        {"command": "query_performance_settings", "negotiated_protocol_id": protocol, "property_ids": [0x8301]}
    )

    assert packet[:2] == wire_protocol.to_bytes(2, "big")


@pytest.mark.parametrize("protocol", [0, 0x2602, 0x280B, 0x2810, 0xFFFF])
def test_unknown_revision_never_offers_performance_or_storage_controls(protocol):
    capabilities = core.performance_capabilities(
        {
            "protocol_id": protocol,
            "managed": False,
            "property_ids": [0x8204, 0x0210, 0x8301, 0x0310, 0x0303],
            "platform_software_version": "3.0.0",
        }
    )

    assert all(not operation["writable"] for operation in capabilities["operations"].values())
    assert all("protocol_unsupported" in operation["reasons"] for operation in capabilities["operations"].values())


@pytest.mark.asyncio
async def test_performance_readback_does_not_invent_numeric_meaning_for_unknown_properties():
    device = FakeDevice([0x8301], [_settings_response({0x8301: 250000, 0x9999: 0x12345678})])

    values = await get_performance_settings(device, [0x8301, 0x9999])

    assert values == {0x8301: 250000}
    assert device.performance_settings == values


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [_settings_response({0x8301: 250000}), None, b"invalid"])
async def test_fresh_query_discards_queried_cached_values_but_preserves_unqueried_values(response):
    device = FakeDevice([0x8301, 0x0310, 0x0303], [response])
    device.performance_settings = {0x8301: 500000, 0x0310: 8, 0x0303: 4}

    if response is None or response == b"invalid":
        with pytest.raises((RuntimeError, core.NetaudioCoreError)):
            await get_performance_settings(device, [0x8301, 0x0310])

        assert device.performance_settings == {0x0303: 4}
    else:
        assert await get_performance_settings(device, [0x8301, 0x0310]) == {0x8301: 250000}
        assert device.performance_settings == {0x8301: 250000, 0x0303: 4}


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [None, b"invalid"])
async def test_unverified_write_does_not_leave_old_affected_values_in_cache(response):
    device = FakeDevice([0x8204, 0x0210], [_arc_response(0x1101), response])
    device.performance_settings = {0x8204: 250000, 0x0210: 8, 0x0303: 4}

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.effective_state_confirmation is None
    assert device.performance_settings == {0x0303: 4}


@pytest.mark.asyncio
async def test_receive_performance_sends_once_then_verifies_every_property():
    expected = {
        0x8301: 250_000,
        0x0310: 8,
        0x8304: 1,
    }
    device = FakeDevice(
        expected, [_arc_response(0x1101), _settings_response(expected)], platform_software_version="2.9.9"
    )

    result = await set_receive_flow_performance(device, 250, 8)

    assert result.state == "confirmed"
    assert result.effective_state_confirmation is True
    assert result.device_confirmation is None
    assert result.persistence_confirmation is None
    assert result.requested_properties == expected
    assert [specification["command"] for specification in device.specifications] == [
        "set_receive_flow_performance",
        "query_performance_settings",
    ]
    assert device.specifications[0]["platform_software_version"] == [2, 9, 9]
    assert device.specifications[1]["property_ids"] == list(expected)


@pytest.mark.asyncio
async def test_transmit_performance_reports_correlated_readback_mismatch():
    supported = [0x8204, 0x0210]
    readback = {0x8204: 999_000, 0x0210: 4}
    device = FakeDevice(supported, [_arc_response(0x1101), _settings_response(readback)])

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.state == "contradicted"
    assert result.effective_state_confirmation is False
    assert result.requested_properties[0x8204] == 500_000
    assert result.effective_properties[0x8204] == 999_000


@pytest.mark.asyncio
async def test_omitted_readback_property_is_unverified_instead_of_contradicted():
    supported = [0x8204, 0x0210]
    readback = {0x8204: 500_000}
    device = FakeDevice(supported, [_arc_response(0x1101), _settings_response(readback)])
    device.performance_settings = {0x8204: 250_000, 0x0210: 4}

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.state == "request_acknowledged"
    assert result.effective_state_confirmation is None
    assert result.verification_observations[0]["outcome"] == "incomplete"
    assert result.verification_observations[0]["missing_property_ids"] == ["0x0210"]
    assert device.performance_settings == readback


@pytest.mark.asyncio
async def test_rejected_request_still_records_fresh_readback_separately():
    expected = {0x8204: 500_000, 0x0210: 4}
    device = FakeDevice(expected, [_arc_response(0x1101, result_code=2), _settings_response(expected)])

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.state == "rejected"
    assert result.request_acknowledgement["accepted"] is False
    assert result.effective_state_confirmation is True
    assert [specification["command"] for specification in device.specifications] == [
        "set_transmit_flow_performance",
        "query_performance_settings",
    ]


@pytest.mark.asyncio
async def test_fresh_readback_can_confirm_state_without_inventing_acknowledgement():
    expected = {0x8204: 500_000, 0x0210: 4}
    device = FakeDevice(expected, [None, _settings_response(expected)])

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.state == "confirmed"
    assert result.request_acknowledgement is None
    assert result.effective_state_confirmation is True
    assert "acknowledgement was not established" in result.message


@pytest.mark.parametrize(
    "acknowledgement", [None, b"invalid", _arc_response(0x1101), _arc_response(0x1101, result_code=2)]
)
@pytest.mark.parametrize(
    "readback,outcome,confirmation",
    [
        (None, "no_response", None),
        (b"invalid", "unparseable", None),
        (_settings_response({0x8204: 500_000, 0x0210: 4}), "matched", True),
        (_settings_response({0x8204: 500_000}), "incomplete", None),
        (_settings_response({0x8204: 999_000}), "mismatch", False),
    ],
)
def test_native_performance_completion_separates_acknowledgement_from_readback(
    acknowledgement, readback, outcome, confirmation
):
    result = core.performance_completion(
        {
            "kind": "configuration",
            "requested": {str(0x8204): 500_000, str(0x0210): 4},
            "acknowledgement": list(acknowledgement) if acknowledgement is not None else None,
            "readback": list(readback) if readback is not None else None,
        }
    )

    acknowledged = acknowledgement == _arc_response(0x1101)
    rejected = acknowledgement == _arc_response(0x1101, result_code=2)
    expected = (
        "rejected"
        if rejected
        else "confirmed"
        if confirmation is True
        else "contradicted"
        if confirmation is False
        else "request_acknowledged"
        if acknowledged
        else "unverified"
    )
    assert result["state"] == expected
    assert result["readback_outcome"] == outcome
    assert result["effective_state_confirmation"] is confirmation
    assert result["persistence_confirmation"] is None


@pytest.mark.parametrize(
    "acknowledgement,state",
    [
        (None, "unverified"),
        (b"invalid", "unverified"),
        (_arc_response(0x1F01), "request_acknowledged"),
        (_arc_response(0x1F01, result_code=2), "rejected"),
    ],
)
def test_native_storage_acknowledgement_never_claims_persistence(acknowledgement, state):
    result = core.performance_completion(
        {"kind": "storage", "acknowledgement": list(acknowledgement) if acknowledgement is not None else None}
    )

    assert result["state"] == state
    assert result["persistence_confirmation"] is None
    assert result["effective_state_confirmation"] is None


@pytest.mark.parametrize(
    "evidence",
    [
        {"kind": "configuration", "requested": {}},
        {"kind": "configuration", "requested": {"1": True}},
        {"kind": "storage", "requested": {"1": 2}},
        {"kind": "storage", "readback": []},
    ],
)
def test_native_performance_completion_rejects_invalid_evidence(evidence):
    with pytest.raises(core.NetaudioCoreError):
        core.performance_completion(evidence)


@pytest.mark.asyncio
async def test_unicast_requests_only_advertised_properties():
    requested = {0x8205: 1_000_000}
    device = FakeDevice(requested, [_arc_response(0x1101), _settings_response(requested)])

    result = await set_unicast_performance(device, 1_000, 16)

    assert result.effective_state_confirmation is True
    assert result.requested_properties == requested
    assert device.specifications[0]["supported_property_ids"] == [0x8205]


@pytest.mark.asyncio
async def test_default_slots_and_store_have_distinct_confirmation_fields():
    slots = {0x0303: 4}
    device = FakeDevice(slots, [_arc_response(0x1101), _settings_response(slots), _arc_response(0x1F01)])

    slots_result = await set_receive_flow_default_slots(device, 4)
    store_result = await store_current_configuration(device)

    assert slots_result.effective_state_confirmation is True
    assert store_result.request_acknowledgement is None
    assert store_result.persistence_request_acknowledgement["accepted"] is True
    assert store_result.persistence_confirmation is None
    assert store_result.state == "request_acknowledged"


@pytest.mark.asyncio
async def test_operations_fail_closed_before_managed_or_unadvertised_writes():
    managed = FakeDevice([], [], managed=True)
    with pytest.raises(RuntimeError, match="no established managed transport"):
        await store_current_configuration(managed)

    missing = FakeDevice([0x8204], [])
    with pytest.raises(core.NetaudioCoreError):
        await set_transmit_flow_performance(missing, 1, 1)
    assert missing.specifications == []


def test_latency_overflow_and_software_version_are_rejected_before_writes():
    device = FakeDevice([], [])
    with pytest.raises(core.NetaudioCoreError):
        set_transmit_flow_performance(device, 0xFFFFFFFF // 1_000 + 1, 1).send(None)

    device.platform_software_version = "3.0.x"
    with pytest.raises(RuntimeError, match="x.y.z"):
        set_receive_flow_performance(device, 1, 1).send(None)


def test_availability_exposes_each_typed_operation():
    supported = [
        0x8301,
        0x0310,
        0x8204,
        0x0210,
        0x0303,
    ]
    device = FakeDevice(supported, [])
    availability = performance_operation_availability(device)
    assert availability["receive_flow_performance"]["writable"] is True
    assert availability["transmit_flow_performance"]["writable"] is True
    assert availability["receive_flow_default_slots"]["writable"] is True
    assert availability["unicast_performance"]["writable"] is True
    assert availability["store_current_configuration"]["writable"] is True


@pytest.mark.parametrize(
    "properties,version,writable",
    [
        ([0x8301, 0x0310], "3.0.0", {"receive_flow_performance", "unicast_performance"}),
        ([0x8301, 0x0310], "2.9.9", set()),
        ([0x8304], "2.9.9", {"unicast_performance"}),
        ([0x8304], "3.0.0", set()),
        ([0x8204, 0x0210], None, {"transmit_flow_performance"}),
        ([0x0303], None, {"receive_flow_default_slots"}),
        ([], "3.0.0", set()),
    ],
)
def test_native_performance_availability_matches_encoder_requirements(properties, version, writable):
    result = core.performance_capabilities(
        {
            "protocol_id": 0x280F,
            "managed": False,
            "property_ids": properties,
            "platform_software_version": version,
        }
    )
    operations = result["operations"]

    assert {name for name, state in operations.items() if state["writable"]} == writable | {
        "store_current_configuration"
    }

    for operation in operations:
        if operation == "store_current_configuration":
            continue

        fields = (
            {"default_slots": 8}
            if operation == "receive_flow_default_slots"
            else {
                "latency_microseconds": 250,
                "frames_per_packet": 4,
            }
        )

        if operation in ("receive_flow_performance", "unicast_performance"):
            fields["platform_software_version"] = result["platform_software_version"]

        spec = {
            "command": f"set_{operation}",
            "negotiated_protocol_id": 0x280F,
            "supported_property_ids": properties,
            **fields,
        }

        if operation in writable:
            assert core.plan_performance_command(spec)
        else:
            with pytest.raises(core.NetaudioCoreError):
                core.plan_performance_command(spec)


def test_unavailable_directory_is_not_reported_as_a_device_rejection():
    device = FakeDevice([], [])
    device.settings_properties = {}

    availability = performance_operation_availability(device)

    assert availability["transmit_flow_performance"]["reasons"] == ["property_directory_unknown"]


@pytest.mark.parametrize(
    "command,fields,expected",
    [
        (
            "set_receive_flow_performance",
            {"latency_microseconds": 250, "frames_per_packet": 8, "platform_software_version": [2, 9, 9]},
            {0x8301: 250000, 0x0310: 8, 0x8304: 1},
        ),
        (
            "set_transmit_flow_performance",
            {"latency_microseconds": 500, "frames_per_packet": 4},
            {0x8204: 500000, 0x0210: 4},
        ),
        (
            "set_unicast_performance",
            {"latency_microseconds": 1000, "frames_per_packet": 4, "platform_software_version": [2, 9, 9]},
            {0x8304: 1000},
        ),
        ("set_receive_flow_default_slots", {"default_slots": 8}, {0x0303: 8}),
    ],
)
def test_native_performance_plan_identifies_every_property_written(command, fields, expected):
    specification = {
        "command": command,
        "negotiated_protocol_id": 0x280F,
        "supported_property_ids": list(expected),
        **fields,
    }
    plan = core.plan_performance_command(specification)

    assert {entry["property_id"]: entry["value"] for entry in plan} == expected
    assert core.build_command(specification)


def test_performance_preview_cannot_replace_observed_device_capabilities():
    device = FakeDevice([0x8204], [])

    with pytest.raises(ValueError):
        requested_performance_properties(
            device,
            "transmit_flow_performance",
            {
                "latency_microseconds": 250,
                "frames_per_packet": 8,
                "supported_property_ids": [0x8204, 0x0210],
            },
        )
