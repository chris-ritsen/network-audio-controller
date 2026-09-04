from types import SimpleNamespace

import pytest

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.application import DanteApplication
from netaudio.dante.transmitter_channel_name_reconciliation import (
    reconcile_transmitter_channel_names,
)


class _Application(DanteApplication):
    def __init__(self):
        super().__init__()
        self.renames = []

    async def set_channel_name(self, device, channel_type, channel_number, name):
        self.renames.append((channel_type, channel_number, name))
        return await self.send_set_channel_name(device, channel_type, channel_number, name)


def _device(routing_name_state, respond):
    device = SimpleNamespace(
        ipv4="192.0.2.50",
        services={},
        topology_mutation_lock=DeferredAsyncioLock(),
        transmitter_channel_name_protocol_identifier=None,
    )
    device.executed = []

    async def execute(specification):
        device.executed.append(specification)
        return respond(specification)

    device.execute = execute

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

    def respond(specification):
        if specification["command"] == "query_modern_arc_transmitter_channel_status":
            return bytes.fromhex("2809000a285224000030")
        if specification["command"] == "set_channel_name":
            routing_name_state[1] = "New-1"
            return bytes.fromhex("2729000c0302201300010000")
        raise AssertionError(f"unexpected command {specification['command']}")

    device = _device(routing_name_state, respond)
    application = _Application()

    result = await reconcile_transmitter_channel_names(application, device, {1: "New-1"})

    assert result.verified == {1: "New-1"}
    assert result.failures == {}
    assert application.renames == [("tx", 1, "New-1")]
    assert [specification["command"] for specification in device.executed] == [
        "query_modern_arc_transmitter_channel_status",
        "set_channel_name",
    ]
    assert device.executed[-1]["protocol_id"] == 0x2729
    assert device.transmitter_channel_name_protocol_identifier == 0x2729


@pytest.mark.asyncio
async def test_reconciliation_is_idempotent_and_sends_nothing():
    def respond(specification):
        raise AssertionError(f"unexpected command {specification['command']}")

    device = _device({1: "Current-1", 2: "Current-2"}, respond)
    application = _Application()

    result = await reconcile_transmitter_channel_names(
        application,
        device,
        {1: "Current-1", 2: "Current-2"},
    )

    assert result.unchanged == {1: "Current-1", 2: "Current-2"}
    assert result.verified == {}
    assert result.failures == {}
    assert device.executed == []
    assert application.renames == []
