import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from netaudio.daemon import subscription_readback as module
from netaudio.dante.device import DanteDevice
from netaudio.dante.subscription_operations import read_subscription_readback
from netaudio.dante.subscription import DanteSubscription
from netaudio import core


@pytest.mark.parametrize(
    "code,managed,state,settled",
    [
        (8, None, "in_progress", False),
        (9, None, "connected", True),
        (3, None, "error", True),
        (65535, None, "unknown", False),
        (None, "DYNAMIC", "connected", True),
        (None, "FUTURE_STATUS", "unknown", False),
    ],
)
def test_native_subscription_readback_separates_configuration_from_connection(code, managed, state, settled):
    request = {
        "channels": [1, 2],
        "subscriptions": [
            {"number": 1, "tx_channel": "Out", "tx_device": "Mixer", "status_code": code, "managed_status": managed}
        ],
        "expected": [{"number": 1, "source": ["Out", "Mixer"]}, {"number": 2, "source": None}],
    }
    result = core.subscription_readback(request)

    assert result["matched"] is True
    assert result["settled"] is settled
    assert result["channels"][0]["connection_state"] == state
    assert result["channels"][1]["source"] is None
    assert result["channels"][1]["settled"] is True

    request["expected"][0]["source"] = ["Other", "Mixer"]
    result = core.subscription_readback(request)

    assert result["matched"] is False
    assert result["settled"] is False


@pytest.mark.parametrize(
    "invalid",
    [
        "duplicate_channel",
        "duplicate_source",
        "partial_source",
        "missing_channel",
        "duplicate_intent",
        "partial_intent",
        "boolean_identity",
    ],
)
def test_native_subscription_readback_rejects_ambiguous_or_incomplete_evidence(invalid):
    request = {"channels": [1], "subscriptions": [], "expected": [{"number": 1, "source": None}]}

    if invalid == "duplicate_channel":
        request["channels"] = [1, 1]
    elif invalid == "duplicate_source":
        request["subscriptions"] = [{"number": 1}, {"number": 1}]
    elif invalid == "partial_source":
        request["subscriptions"] = [{"number": 1, "tx_device": "Mixer"}]
    elif invalid == "missing_channel":
        request["channels"] = [2]
    elif invalid == "duplicate_intent":
        request["expected"] *= 2
    elif invalid == "partial_intent":
        request["expected"][0]["source"] = ["", "Mixer"]
    else:
        request["channels"] = [True]

    with pytest.raises(core.NetaudioCoreError):
        core.subscription_readback(request)


@pytest.mark.asyncio
async def test_readback_does_not_depend_on_presentation_serialization():
    device = receiver()
    entry = subscription()
    entry.to_json = Mock(side_effect=AssertionError("Presentation is not protocol evidence"))
    device.subscriptions = [entry]

    assert (await read_subscription_readback(device, {1: ("Out", "Mixer")}))["matched"]
    assert module.subscriptions_settled(device, {1: ("Out", "Mixer")})


def receiver():
    return SimpleNamespace(
        server_name="receiver",
        online=True,
        subscriptions=[],
        rx_channels={number: SimpleNamespace(number=number) for number in (1, 2)},
        topology_mutation_lock=asyncio.Lock(),
        get_rx_channels=AsyncMock(),
    )


def subscription(number=1, code=9, channel="Out", transmitter="Mixer", managed=None, summary=None):
    result = DanteSubscription()
    result.rx_channel = SimpleNamespace(number=number)
    result.tx_channel_name = channel
    result.tx_device_name = transmitter
    result.status_code = code if managed is None else None
    result.rx_channel_status_code = 257
    result.ddm_status = managed
    result.ddm_summary = summary

    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("source", [None, ("Out", "Mixer")])
async def test_missing_control_address_cannot_confirm_cached_subscription(source):
    from netaudio.dante.const import SERVICE_ARC

    device = DanteDevice(server_name="receiver.local.")
    device.services = {"arc": {"type": SERVICE_ARC, "properties": {"arcp_vers": "2.7.255"}}}
    device.rx_channels = {1: SimpleNamespace(number=1)}
    device.subscriptions = [subscription()] if source else []
    transport = SimpleNamespace(call=AsyncMock())
    device._app = SimpleNamespace(transport=transport)

    with pytest.raises(RuntimeError, match="no control address"):
        await read_subscription_readback(device, {1: source})
    transport.call.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("source", [{"source_device_name": "Mixer"}, {"source_channel_name": "Out"}])
async def test_modern_incomplete_source_cannot_confirm_removal(existing, source):
    device = DanteDevice(server_name="receiver.local.")
    device.get_rx_channels = AsyncMock()

    if existing:
        device.apply_receiver_channel_inventory(
            {"records": [{"channel_number": 1, "source_device_name": "Mixer", "source_channel_name": "Out"}]}
        )

    device.apply_receiver_channel_inventory({"records": [{"channel_number": 1, **source}]})
    with pytest.raises(core.NetaudioCoreError, match="incomplete"):
        await read_subscription_readback(device, {1: None})
    assert not module.subscriptions_settled(device, {1: ("", "")})

    device.apply_receiver_channel_inventory({"records": [{"channel_number": 1}]})
    result = await read_subscription_readback(device, {1: None})

    assert result["matched"]
    assert module.subscriptions_settled(device, {1: ("", "")})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid", ["duplicate", "unidentified", "boolean_identity", "float_identity", "missing_channel", "missing_device"]
)
async def test_ambiguous_inventory_cannot_confirm_removal_or_end_polling(monkeypatch, invalid):
    monkeypatch.setattr(module, "READBACK_INTERVAL_SECONDS", 0)
    device = receiver()
    unresolved = subscription(channel="", transmitter="")

    if invalid == "duplicate":
        device.subscriptions = [subscription(), unresolved]
    elif invalid == "unidentified":
        unresolved.rx_channel = None
        device.subscriptions = [unresolved]
    elif invalid in {"boolean_identity", "float_identity"}:
        unresolved.rx_channel.number = True if invalid == "boolean_identity" else 1.0
        device.subscriptions = [unresolved]
    elif invalid == "missing_channel":
        device.subscriptions = [subscription(channel="")]
    else:
        device.subscriptions = [subscription(transmitter="")]

    with pytest.raises(core.NetaudioCoreError):
        await read_subscription_readback(device, {1: None})
    device.get_rx_channels.reset_mock()
    reads = 0

    async def read():
        nonlocal reads
        reads += 1

        if reads == 2:
            device.subscriptions = []

    device.get_rx_channels.side_effect = read
    reader = module.SubscriptionReadback(Mock())
    reader.request(device, [(1, "", "")])
    await asyncio.wait_for(reader.tasks[device.server_name], 1)
    assert reads == 2
    assert not reader.pending


@pytest.mark.asyncio
@pytest.mark.parametrize("numbers", [(2,), (1, 1), (True,), (1.0,)])
async def test_channel_inventory_keys_cannot_confirm_missing_or_ambiguous_receiver(numbers):
    device = receiver()
    device.rx_channels = {key: SimpleNamespace(number=number) for key, number in enumerate(numbers, 1)}

    with pytest.raises(core.NetaudioCoreError):
        await read_subscription_readback(device, {1: None})
    assert module.subscriptions_settled(device, {1: ("", "")}) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("code,settled", [(8, False), (3, True), (9, True), (35, False), (65535, False)])
async def test_configured_source_confirmation_is_distinct_from_connection_completion(code, settled):
    device = receiver()
    device.subscriptions = [subscription(code=code)]
    result = await read_subscription_readback(device, {1: ("Out", "Mixer")})
    assert result["matched"]
    assert module.subscriptions_settled(device, {1: ("Out", "Mixer")}) is settled


@pytest.mark.asyncio
async def test_immediate_readback_retries_stale_and_pending_then_publishes_connected(monkeypatch):
    monkeypatch.setattr(module, "READBACK_INTERVAL_SECONDS", 0)
    device = receiver()
    states = [[], [subscription(code=8)], [subscription()]]

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
    device.subscriptions = [subscription(code=3)]
    assert module.subscriptions_settled(device, {1: ("Out", "Mixer")})
    assert not module.subscriptions_settled(device, {1: ("Other", "Mixer")})
    assert not module.subscriptions_settled(device, {3: ("", "")})


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", ["IN_PROGRESS", "TX_NOT_READY", "FUTURE_STATUS"])
async def test_managed_summary_cannot_end_polling_before_known_terminal_status(monkeypatch, identifier):
    monkeypatch.setattr(module, "READBACK_INTERVAL_SECONDS", 0)
    device = receiver()
    states = [
        [subscription(managed=identifier, summary="CONNECTED")],
        [subscription(managed="DYNAMIC", summary="CONNECTED")],
    ]

    async def read():
        device.subscriptions = states.pop(0)

    device.get_rx_channels.side_effect = read
    reader = module.SubscriptionReadback(Mock())
    reader.request(device, [(1, "Out", "Mixer")])
    await asyncio.wait_for(reader.tasks[device.server_name], 1)
    assert device.get_rx_channels.await_count == 2
    assert device.subscriptions[0].to_json()["status"]["settled"] is True


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
    channel.name = "Stale receiver label"
    channel.factory_name = "Input 1"
    device.rx_channels = {"Stale receiver label": channel}
    device.get_rx_channels = AsyncMock(side_effect=AssertionError("Managed readback must use inventory"))
    transport = SimpleNamespace(fetch_device=AsyncMock(return_value=fresh))
    app = SimpleNamespace(managed_transport=Mock(return_value=transport))
    reader = module.SubscriptionReadback(Mock(), app)
    reader.request(device, [(1, "managed-tx", "Receiver")])
    await reader.tasks[device.server_name]
    transport.fetch_device.assert_awaited_once_with(device)
    device.get_rx_channels.assert_not_called()
    assert device.rx_channels[1].name == "managed-rx"
    assert device.rx_channels[1].factory_name == "Input 1"
    assert device.rx_channels[1].device is device
    assert device.subscriptions[0].rx_channel is device.rx_channels[1]
    assert device.subscriptions[0].to_json()["status"]["state"] == "connected"
    reader.publish.assert_called_once_with(device)
