from __future__ import annotations

from types import SimpleNamespace

import pytest

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.commands import DanteCommands
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.performance_configuration import (
    PROPERTY_PRE_3_COMPATIBILITY,
    PROPERTY_RX_FLOW_DEFAULT_SLOTS,
    PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
    PROPERTY_RX_FLOW_LATENCY_NS,
    PROPERTY_TX_FLOW_FRAMES_PER_PACKET,
    PROPERTY_TX_FLOW_LATENCY_NS,
    PROPERTY_UNICAST_CONFIGURED_LATENCY_NS,
    performance_operation_availability,
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
async def test_receive_performance_sends_once_then_verifies_every_property():
    expected = {
        PROPERTY_RX_FLOW_LATENCY_NS: 250_000,
        PROPERTY_RX_FLOW_FRAMES_PER_PACKET: 8,
        PROPERTY_PRE_3_COMPATIBILITY: 1,
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
    supported = [PROPERTY_TX_FLOW_LATENCY_NS, PROPERTY_TX_FLOW_FRAMES_PER_PACKET]
    readback = {PROPERTY_TX_FLOW_LATENCY_NS: 999_000, PROPERTY_TX_FLOW_FRAMES_PER_PACKET: 4}
    device = FakeDevice(supported, [_arc_response(0x1101), _settings_response(readback)])

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.state == "contradicted"
    assert result.effective_state_confirmation is False
    assert result.requested_properties[PROPERTY_TX_FLOW_LATENCY_NS] == 500_000
    assert result.effective_properties[PROPERTY_TX_FLOW_LATENCY_NS] == 999_000


@pytest.mark.asyncio
async def test_omitted_readback_property_is_unverified_instead_of_contradicted():
    supported = [PROPERTY_TX_FLOW_LATENCY_NS, PROPERTY_TX_FLOW_FRAMES_PER_PACKET]
    readback = {PROPERTY_TX_FLOW_LATENCY_NS: 500_000}
    device = FakeDevice(supported, [_arc_response(0x1101), _settings_response(readback)])

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.state == "request_acknowledged"
    assert result.effective_state_confirmation is None
    assert result.verification_observations[0]["outcome"] == "incomplete"
    assert result.verification_observations[0]["missing_property_ids"] == ["0x0210"]


@pytest.mark.asyncio
async def test_rejected_request_still_records_fresh_readback_separately():
    expected = {PROPERTY_TX_FLOW_LATENCY_NS: 500_000, PROPERTY_TX_FLOW_FRAMES_PER_PACKET: 4}
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
    expected = {PROPERTY_TX_FLOW_LATENCY_NS: 500_000, PROPERTY_TX_FLOW_FRAMES_PER_PACKET: 4}
    device = FakeDevice(expected, [None, _settings_response(expected)])

    result = await set_transmit_flow_performance(device, 500, 4)

    assert result.state == "confirmed"
    assert result.request_acknowledgement is None
    assert result.effective_state_confirmation is True
    assert "acknowledgement was not established" in result.message


@pytest.mark.asyncio
async def test_unicast_requests_only_advertised_properties():
    requested = {PROPERTY_UNICAST_CONFIGURED_LATENCY_NS: 1_000_000}
    device = FakeDevice(requested, [_arc_response(0x1101), _settings_response(requested)])

    result = await set_unicast_performance(device, 1_000, 16)

    assert result.effective_state_confirmation is True
    assert result.requested_properties == requested
    assert device.specifications[0]["supported_property_ids"] == [PROPERTY_UNICAST_CONFIGURED_LATENCY_NS]


@pytest.mark.asyncio
async def test_default_slots_and_store_have_distinct_confirmation_fields():
    slots = {PROPERTY_RX_FLOW_DEFAULT_SLOTS: 4}
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

    missing = FakeDevice([PROPERTY_TX_FLOW_LATENCY_NS], [])
    with pytest.raises(RuntimeError, match="0x0210"):
        await set_transmit_flow_performance(missing, 1, 1)
    assert missing.specifications == []


def test_latency_overflow_and_software_version_are_rejected_before_writes():
    device = FakeDevice([], [])
    with pytest.raises(ValueError, match="too large"):
        set_transmit_flow_performance(device, 0xFFFFFFFF // 1_000 + 1, 1).send(None)

    device.platform_software_version = "3.0.0.1"
    with pytest.raises(RuntimeError, match="x.y.z"):
        set_receive_flow_performance(device, 1, 1).send(None)


def test_availability_exposes_each_typed_operation():
    supported = [
        PROPERTY_RX_FLOW_LATENCY_NS,
        PROPERTY_RX_FLOW_FRAMES_PER_PACKET,
        PROPERTY_TX_FLOW_LATENCY_NS,
        PROPERTY_TX_FLOW_FRAMES_PER_PACKET,
        PROPERTY_RX_FLOW_DEFAULT_SLOTS,
    ]
    device = FakeDevice(supported, [])
    availability = performance_operation_availability(device)
    assert availability["receive_flow_performance"]["writable"] is True
    assert availability["transmit_flow_performance"]["writable"] is True
    assert availability["receive_flow_default_slots"]["writable"] is True
    assert availability["unicast_performance"]["writable"] is True
    assert availability["store_current_configuration"]["writable"] is True


def test_python_command_frontends_expose_typed_serializers():
    specification = DanteCommands().set_receive_flow_performance(
        0x280F,
        [PROPERTY_RX_FLOW_LATENCY_NS, PROPERTY_RX_FLOW_FRAMES_PER_PACKET],
        250,
        8,
        (3, 0, 0),
    )
    assert specification == {
        "command": "set_receive_flow_performance",
        "negotiated_protocol_id": 0x280F,
        "supported_property_ids": [PROPERTY_RX_FLOW_LATENCY_NS, PROPERTY_RX_FLOW_FRAMES_PER_PACKET],
        "latency_microseconds": 250,
        "frames_per_packet": 8,
        "platform_software_version": [3, 0, 0],
    }

    packet, service = DanteDeviceCommands().command_store_current_configuration(0x280F, 0x1234)
    assert service == SERVICE_ARC
    assert packet == bytes.fromhex("2809000a12341f010000")
