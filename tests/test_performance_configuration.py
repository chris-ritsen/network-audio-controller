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
    set_receive_flow_performance,
    set_transmit_flow_performance,
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
@pytest.mark.parametrize("response", [None, b"invalid"])
async def test_unverified_write_does_not_leave_old_affected_values_in_cache(response):
    device = FakeDevice([0x8204, 0x0210], [_arc_response(0x1101), response])
    device.performance_settings = {0x8204: 250000, 0x0210: 8, 0x0303: 4}

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.effective_state_confirmation is None
    assert device.performance_settings == {0x0303: 4}














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
