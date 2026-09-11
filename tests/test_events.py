import asyncio

import pytest

from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType


@pytest.fixture
def dispatcher():
    return DanteEventDispatcher()


def test_dante_event_defaults():
    event = DanteEvent(type=EventType.DEVICE_DISCOVERED)
    assert event.type == EventType.DEVICE_DISCOVERED
    assert event.device_name == ""
    assert event.server_name == ""
    assert event.data == {}


def test_event_payload_defaults_are_not_shared():
    first = DanteEvent(type=EventType.DEVICE_UPDATED)
    second = DanteEvent(type=EventType.DEVICE_UPDATED)
    first.data["sample_rate"] = 48000
    assert second.data == {}


@pytest.mark.asyncio
async def test_off_stops_delivery_without_removing_other_listeners(dispatcher):
    received = []
    retained = []

    async def callback(event):
        received.append(event)

    async def other_callback(event):
        retained.append(event)

    dispatcher.on(EventType.DEVICE_DISCOVERED, callback)
    dispatcher.on(EventType.DEVICE_DISCOVERED, other_callback)
    before = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="before")
    await dispatcher.start()
    await dispatcher.emit(before)
    await dispatcher.stop()  # Drain the queued event before unregistering.
    dispatcher.off(EventType.DEVICE_DISCOVERED, callback)
    after = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="after")
    await dispatcher.start()
    try:
        await dispatcher.emit(after)
    finally:
        await dispatcher.stop()
    assert received == [before]
    assert retained == [before, after]


def test_off_nonexistent_callback(dispatcher):
    async def callback(event):
        pass

    # Should not raise
    dispatcher.off(EventType.DEVICE_DISCOVERED, callback)


@pytest.mark.asyncio
async def test_emit_nowait(dispatcher):
    received = []
    dispatched = asyncio.Event()

    async def callback(event):
        received.append(event)
        dispatched.set()

    dispatcher.on(EventType.DEVICE_DISCOVERED, callback)
    event = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="Test")
    dispatcher.emit_nowait(event)
    await dispatcher.start()
    try:
        await asyncio.wait_for(dispatched.wait(), timeout=1)
    finally:
        await dispatcher.stop()

    assert received == [event]


@pytest.mark.asyncio
async def test_emit(dispatcher):
    received = []
    dispatched = asyncio.Event()

    async def callback(event):
        received.append(event)
        dispatched.set()

    dispatcher.on(EventType.DEVICE_REMOVED, callback)
    await dispatcher.start()
    event = DanteEvent(type=EventType.DEVICE_REMOVED, server_name="test.local.")
    try:
        await dispatcher.emit(event)
        await asyncio.wait_for(dispatched.wait(), timeout=1)
    finally:
        await dispatcher.stop()

    assert received == [event]


@pytest.mark.asyncio
async def test_dispatch_loop(dispatcher):
    received = []
    dispatched = asyncio.Event()

    async def callback(event):
        received.append(event)
        dispatched.set()

    dispatcher.on(EventType.DEVICE_DISCOVERED, callback)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="Test")
    dispatcher.emit_nowait(event)

    try:
        await asyncio.wait_for(dispatched.wait(), timeout=1)
    finally:
        await dispatcher.stop()

    assert received == [event]


@pytest.mark.asyncio
async def test_dispatch_loop_multiple_listeners(dispatcher):
    received_a = []
    received_b = []
    dispatched = asyncio.Event()

    async def callback_a(event):
        received_a.append(event)

    async def callback_b(event):
        received_b.append(event)
        dispatched.set()

    dispatcher.on(EventType.DEVICE_UPDATED, callback_a)
    dispatcher.on(EventType.DEVICE_UPDATED, callback_b)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_UPDATED, device_name="Test")
    dispatcher.emit_nowait(event)

    try:
        await asyncio.wait_for(dispatched.wait(), timeout=1)
    finally:
        await dispatcher.stop()

    assert received_a == [event]
    assert received_b == [event]


@pytest.mark.asyncio
async def test_dispatch_loop_ignores_other_types(dispatcher):
    received = []
    dispatched = asyncio.Event()

    async def callback(event):
        received.append(event)

    async def removed_callback(event):
        dispatched.set()

    dispatcher.on(EventType.DEVICE_DISCOVERED, callback)
    dispatcher.on(EventType.DEVICE_REMOVED, removed_callback)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_REMOVED, device_name="Test")
    dispatcher.emit_nowait(event)

    try:
        await asyncio.wait_for(dispatched.wait(), timeout=1)
    finally:
        await dispatcher.stop()

    assert received == []


@pytest.mark.asyncio
async def test_dispatch_loop_error_handling(dispatcher):
    received = []
    dispatched = asyncio.Event()

    async def bad_callback(event):
        raise ValueError("test error")

    async def good_callback(event):
        received.append(event)
        dispatched.set()

    dispatcher.on(EventType.DEVICE_DISCOVERED, bad_callback)
    dispatcher.on(EventType.DEVICE_DISCOVERED, good_callback)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="Test")
    dispatcher.emit_nowait(event)

    try:
        await asyncio.wait_for(dispatched.wait(), timeout=1)
    finally:
        await dispatcher.stop()

    assert received == [event]


@pytest.mark.asyncio
async def test_start_stop_idempotent(dispatcher):
    await dispatcher.start()
    task = dispatcher._dispatch_task
    try:
        await dispatcher.start()
        assert dispatcher._dispatch_task is task
        assert not task.done()
    finally:
        await dispatcher.stop()
    assert task.done()
    assert dispatcher._dispatch_task is None
    assert dispatcher._queue is None
    await dispatcher.stop()
    assert dispatcher._dispatch_task is None


@pytest.mark.asyncio
async def test_notification_burst_keeps_one_pending_readback_per_device(dispatcher):
    entered = asyncio.Event()
    release = asyncio.Event()
    received = []

    async def callback(event):
        received.append((event.server_name, event.data["revision"]))
        if len(received) == 1:
            entered.set()
            await release.wait()

    def notification(device, revision):
        return DanteEvent(
            type=EventType.NOTIFICATION_RECEIVED,
            server_name=device,
            data={"notification_id": 1, "revision": revision},
        )

    dispatcher.on(EventType.NOTIFICATION_RECEIVED, callback)
    await dispatcher.start()
    dispatcher.emit_nowait(notification("first.local.", 0))
    await asyncio.wait_for(entered.wait(), 1)
    try:
        for revision in range(1, 101):
            await dispatcher.emit(notification("first.local.", revision))
        dispatcher.emit_nowait(notification("second.local.", 1))
    finally:
        release.set()
        await dispatcher.stop()
    assert received == [("first.local.", 0), ("first.local.", 100), ("second.local.", 1)]


@pytest.mark.asyncio
async def test_partial_meter_pages_and_lifecycle_events_preserve_order(dispatcher):
    received = []

    async def callback(event):
        received.append(event)

    dispatcher.on(EventType.METER_VALUES, callback)
    dispatcher.on(EventType.DEVICE_REMOVED, callback)
    events = [
        DanteEvent(
            type=EventType.METER_VALUES, server_name="device", data={"metering_source": "detailed", "rx": {1: 1}}
        ),
        DanteEvent(type=EventType.DEVICE_REMOVED, server_name="device"),
        DanteEvent(
            type=EventType.METER_VALUES, server_name="device", data={"metering_source": "detailed", "rx": {1: 2}}
        ),
        DanteEvent(
            type=EventType.METER_VALUES, server_name="device", data={"metering_source": "signal_presence", "rx": {2: 3}}
        ),
        DanteEvent(
            type=EventType.METER_VALUES, server_name="device", data={"metering_source": "detailed", "rx": {1: 4}}
        ),
    ]
    for event in events:
        dispatcher.emit_nowait(event)
    await dispatcher.start()
    await dispatcher.stop()
    assert received == events
