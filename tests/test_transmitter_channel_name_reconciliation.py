import asyncio
from types import SimpleNamespace

import pytest

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.transmitter_channel_name_reconciliation import (
    reconcile_transmitter_channel_names,
)


def _device(routing_name_state):
    device = SimpleNamespace(
        ipv4="192.0.2.50",
        services={},
        topology_mutation_lock=DeferredAsyncioLock(),
        transmitter_channel_name_protocol_identifier=None,
    )

    async def get_transmitter_channels():
        device.tx_channels = {
            channel_number: SimpleNamespace(
                number=channel_number,
                name=f"Factory-{channel_number}",
                friendly_name=routing_name,
            )
            for channel_number, routing_name in routing_name_state.items()
        }

    device.get_tx_channels = get_transmitter_channels
    return device


@pytest.mark.asyncio
async def test_reconciliation_uses_2729_after_authentic_a32_frontend_rejection():
    routing_name_state = {1: "Old-1"}
    device = _device(routing_name_state)
    packets = []

    async def send(packet, *_args, **_kwargs):
        packets.append(packet)
        opcode = int.from_bytes(packet[6:8], "big")
        if opcode == 0x2400:
            return bytes.fromhex("2809000a285224000030")
        if opcode == 0x2013:
            routing_name_state[1] = "New-1"
            return bytes.fromhex("2729000c0302201300010000")
        raise AssertionError(f"unexpected opcode 0x{opcode:04X}")

    result = await reconcile_transmitter_channel_names(send, device, {1: "New-1"})

    assert result.verified == {1: "New-1"}
    assert result.failures == {}
    assert [int.from_bytes(packet[0:2], "big") for packet in packets] == [0x2809, 0x2729]
    assert [int.from_bytes(packet[6:8], "big") for packet in packets] == [0x2400, 0x2013]


@pytest.mark.asyncio
async def test_reconciliation_is_idempotent_and_sends_nothing():
    device = _device({1: "Current-1", 2: "Current-2"})
    packets = []

    async def send(packet, *_args, **_kwargs):
        packets.append(packet)

    result = await reconcile_transmitter_channel_names(
        send,
        device,
        {1: "Current-1", 2: "Current-2"},
    )

    assert result.unchanged == {1: "Current-1", 2: "Current-2"}
    assert result.verified == {}
    assert result.failures == {}
    assert packets == []
