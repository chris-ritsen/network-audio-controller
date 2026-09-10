import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from netaudio.daemon import subscription_readback as module


def receiver():
    return SimpleNamespace(
        server_name="receiver",
        online=True,
        subscriptions=[],
        rx_channels={1: object(), 2: object()},
        topology_mutation_lock=asyncio.Lock(),
        get_rx_channels=AsyncMock(),
    )


def subscription(number=1, severity="ok", channel="Out", transmitter="Mixer"):
    return SimpleNamespace(
        rx_channel=SimpleNamespace(number=number),
        tx_channel_name=channel,
        tx_device_name=transmitter,
        to_json=lambda: {"status": {"severity": severity}},
    )


@pytest.mark.asyncio
async def test_immediate_readback_retries_stale_and_pending_then_publishes_connected(monkeypatch):
    monkeypatch.setattr(module, "READBACK_INTERVAL_SECONDS", 0)
    device = receiver()
    states = [[], [subscription(severity="progress")], [subscription()]]

    async def read():
        device.subscriptions = states.pop(0)

    device.get_rx_channels.side_effect = read
    published = []
    reader = module.SubscriptionReadback(lambda device: published.append(list(device.subscriptions)))
    reader.request(device, [(1, "Out", "Mixer")])
    await reader.tasks[device.server_name]
    assert len(published) == 3
    assert published[-1][0].to_json()["status"]["severity"] == "ok"
    assert not reader.tasks


@pytest.mark.asyncio
async def test_overlapping_requests_keep_both_channels_and_latest_target(monkeypatch):
    monkeypatch.setattr(module, "READBACK_INTERVAL_SECONDS", 0)
    device = receiver()
    reader = module.SubscriptionReadback(Mock())
    reader.request(device, [(1, "Old", "Mixer")])
    first_task = reader.tasks[device.server_name]
    reader.request(device, [(1, "New", "Mixer"), (2, "Out", "Mixer")])
    assert reader.tasks[device.server_name] is first_task
    calls = 0

    async def read():
        nonlocal calls
        calls += 1
        device.subscriptions = [subscription(channel="New")]
        if calls == 2:
            device.subscriptions.append(subscription(number=2))

    device.get_rx_channels.side_effect = read
    await first_task
    assert calls == 2


@pytest.mark.asyncio
async def test_unsubscribe_retries_until_source_is_absent(monkeypatch):
    monkeypatch.setattr(module, "READBACK_INTERVAL_SECONDS", 0)
    device = receiver()
    states = [[subscription()], []]

    async def read():
        device.subscriptions = states.pop(0)

    device.get_rx_channels.side_effect = read
    reader = module.SubscriptionReadback(Mock())
    reader.request(device, [(1, "", "")])
    await reader.tasks[device.server_name]
    assert device.get_rx_channels.await_count == 2


@pytest.mark.asyncio
async def test_unresponsive_read_is_bounded_and_shutdown_cancels(monkeypatch):
    monkeypatch.setattr(module, "READBACK_WINDOW_SECONDS", 0.02)
    device = receiver()
    device.get_rx_channels.side_effect = lambda: None

    async def hang():
        await asyncio.Event().wait()

    device.get_rx_channels.side_effect = hang
    publish = Mock()
    reader = module.SubscriptionReadback(publish)
    reader.request(device, [(1, "Out", "Mixer")])
    await asyncio.wait_for(reader.tasks[device.server_name], 1)
    publish.assert_not_called()
    reader.request(device, [(1, "Out", "Mixer")])
    await reader.stop()
    assert not reader.tasks and not reader.pending


@pytest.mark.asyncio
async def test_errors_are_terminal_but_wrong_sources_and_missing_channels_are_not():
    device = receiver()
    device.subscriptions = [subscription(severity="error")]
    assert module.subscriptions_settled(device, {1: ("Out", "Mixer")})
    assert not module.subscriptions_settled(device, {1: ("Other", "Mixer")})
    assert not module.subscriptions_settled(device, {3: ("", "")})


@pytest.mark.asyncio
async def test_readback_can_complete_after_two_seconds():
    device = receiver()

    async def read():
        await asyncio.sleep(2.1)
        device.subscriptions = [subscription()]

    device.get_rx_channels.side_effect = read
    reader = module.SubscriptionReadback(Mock())
    reader.request(device, [(1, "Out", "Mixer")])
    try:
        await asyncio.wait_for(reader.tasks[device.server_name], 3)
        device.get_rx_channels.assert_awaited_once()
        reader.publish.assert_called_once_with(device)
    finally:
        await reader.stop()


@pytest.mark.asyncio
async def test_managed_readback_uses_inventory_and_publishes_fresh_status():
    from dataclasses import replace
    from netaudio.dante.device import DanteDevice
    from netaudio.dante.channel import DanteChannel
    from tests.test_managed_inventory import _managed_device

    fresh = _managed_device(name="Receiver")
    fresh = replace(
        fresh,
        rx_channels=(
            replace(
                fresh.rx_channels[0],
                subscribed_device=".",
                subscribed_channel="managed-tx",
                status="SUBSCRIBE_SELF",
                summary="CONNECTED",
            ),
        ),
    )
    device = DanteDevice(server_name="receiver")
    device.name = "Receiver"
    device.management_state = "managed"
    device.online = True
    channel = DanteChannel()
    channel.number = 1
    channel.name = "managed-rx"
    channel.factory_name = "Input 1"
    device.rx_channels = {1: channel}
    device.get_rx_channels = AsyncMock(side_effect=AssertionError("Managed readback must use inventory"))
    transport = SimpleNamespace(fetch_device=AsyncMock(return_value=fresh))
    app = SimpleNamespace(managed_transport=Mock(return_value=transport))
    reader = module.SubscriptionReadback(Mock(), app)
    reader.request(device, [(1, "managed-tx", "Receiver")])
    await reader.tasks[device.server_name]
    transport.fetch_device.assert_awaited_once_with(device)
    device.get_rx_channels.assert_not_called()
    assert device.rx_channels[1].factory_name == "Input 1"
    assert device.rx_channels[1].device is device
    assert device.subscriptions[0].rx_channel is device.rx_channels[1]
    assert device.subscriptions[0].to_json()["status"]["state"] == "connected"
    reader.publish.assert_called_once_with(device)
