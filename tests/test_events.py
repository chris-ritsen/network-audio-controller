import asyncio

import pytest

from netaudio_lib.dante.events import DanteEvent, DanteEventDispatcher, EventType


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


def test_emit_nowait(dispatcher):
    event = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="Test")
    dispatcher.emit_nowait(event)
    assert dispatcher._queue.qsize() == 1


@pytest.mark.asyncio
async def test_emit(dispatcher):
    event = DanteEvent(type=EventType.DEVICE_REMOVED, server_name="test.local.")
    await dispatcher.emit(event)
    assert dispatcher._queue.qsize() == 1


@pytest.mark.asyncio
async def test_dispatch_loop(dispatcher):
    received = []

    async def callback(event):
        received.append(event)

    dispatcher.on(EventType.DEVICE_DISCOVERED, callback)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="Test")
    dispatcher.emit_nowait(event)

    # Give the dispatch loop time to process
    await asyncio.sleep(0.1)

    assert len(received) == 1
    assert received[0].device_name == "Test"

    await dispatcher.stop()


@pytest.mark.asyncio
async def test_dispatch_loop_multiple_listeners(dispatcher):
    received_a = []
    received_b = []

    async def callback_a(event):
        received_a.append(event)

    async def callback_b(event):
        received_b.append(event)

    dispatcher.on(EventType.DEVICE_UPDATED, callback_a)
    dispatcher.on(EventType.DEVICE_UPDATED, callback_b)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_UPDATED, device_name="Test")
    dispatcher.emit_nowait(event)

    await asyncio.sleep(0.1)

    assert len(received_a) == 1
    assert len(received_b) == 1

    await dispatcher.stop()


@pytest.mark.asyncio
async def test_dispatch_loop_ignores_other_types(dispatcher):
    received = []

    async def callback(event):
        received.append(event)

    dispatcher.on(EventType.DEVICE_DISCOVERED, callback)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_REMOVED, device_name="Test")
    dispatcher.emit_nowait(event)

    await asyncio.sleep(0.1)

    assert len(received) == 0

    await dispatcher.stop()


@pytest.mark.asyncio
async def test_dispatch_loop_error_handling(dispatcher):
    received = []

    async def bad_callback(event):
        raise ValueError("test error")

    async def good_callback(event):
        received.append(event)

    dispatcher.on(EventType.DEVICE_DISCOVERED, bad_callback)
    dispatcher.on(EventType.DEVICE_DISCOVERED, good_callback)
    await dispatcher.start()

    event = DanteEvent(type=EventType.DEVICE_DISCOVERED, device_name="Test")
    dispatcher.emit_nowait(event)

    await asyncio.sleep(0.1)

    # The good callback should still be called despite the bad one raising
    assert len(received) == 1

    await dispatcher.stop()


@pytest.mark.asyncio
async def test_start_stop_idempotent(dispatcher):
    await dispatcher.start()
    await dispatcher.start()  # Should not raise or create duplicate tasks

    await dispatcher.stop()
    await dispatcher.stop()  # Should not raise
