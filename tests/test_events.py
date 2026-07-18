import asyncio

import pytest

from netaudio.dante.events import DanteEvent, DanteEventDispatcher, EventType


@pytest.fixture
def dispatcher():
    return DanteEventDispatcher()


def test_event_type_enum():
    assert EventType.DEVICE_DISCOVERED.name == "DEVICE_DISCOVERED"
    assert EventType.DEVICE_REMOVED.name == "DEVICE_REMOVED"
    assert EventType.DEVICE_UPDATED.name == "DEVICE_UPDATED"
    assert EventType.METER_VALUES.name == "METER_VALUES"
    assert EventType.NOTIFICATION_RECEIVED.name == "NOTIFICATION_RECEIVED"


def test_dante_event_defaults():
    event = DanteEvent(type=EventType.DEVICE_DISCOVERED)
    assert event.type == EventType.DEVICE_DISCOVERED
    assert event.device_name == ""
    assert event.server_name == ""
    assert event.data == {}


def test_dante_event_with_data():
    event = DanteEvent(
        type=EventType.DEVICE_UPDATED,
        device_name="My Device",
        server_name="device.local.",
        data={"field": "sample_rate", "value": 48000},
    )
    assert event.device_name == "My Device"
    assert event.server_name == "device.local."
    assert event.data["field"] == "sample_rate"
    assert event.data["value"] == 48000


def test_on_off_callback(dispatcher):
    received = []

    async def callback(event):
        received.append(event)

    dispatcher.on(EventType.DEVICE_DISCOVERED, callback)
    assert len(dispatcher._listeners[EventType.DEVICE_DISCOVERED]) == 1

    dispatcher.off(EventType.DEVICE_DISCOVERED, callback)
    assert len(dispatcher._listeners[EventType.DEVICE_DISCOVERED]) == 0


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
    await dispatcher.start()  # Should not raise or create duplicate tasks

    await dispatcher.stop()
    await dispatcher.stop()  # Should not raise
