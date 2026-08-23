import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import netaudio.shure.manager as manager_module
from netaudio.shure.device import ShureDeviceInfo, ShureDeviceType
from netaudio.shure.manager import ShureConnection, ShureManager


@pytest.mark.asyncio
async def test_duplicate_shure_connect_attempts_share_one_tracked_task():
    manager = ShureManager(MagicMock())
    manager._running = True
    started = asyncio.Event()
    cancelled = asyncio.Event()
    calls = 0

    async def pending_connect(ip, mac):
        nonlocal calls
        calls += 1
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    manager._connect_device = pending_connect
    manager._schedule_connect("192.0.2.10", "00:11:22:33:44:55")
    first_task = manager._connect_tasks["192.0.2.10"]
    manager._schedule_connect("192.0.2.10", "00:11:22:33:44:55")

    await started.wait()
    assert manager._connect_tasks["192.0.2.10"] is first_task
    assert calls == 1

    await manager.stop()

    assert cancelled.is_set()
    assert first_task.done()
    assert manager._connect_tasks == {}


@pytest.mark.asyncio
async def test_failed_shure_connect_closes_partial_connection(monkeypatch):
    connection = MagicMock()
    connection.connect = AsyncMock(side_effect=ConnectionError("bad probe"))
    connection.close = AsyncMock()
    monkeypatch.setattr(manager_module, "ShureConnection", MagicMock(return_value=connection))

    manager = ShureManager(MagicMock())
    manager._running = True
    manager._schedule_reconnect = MagicMock()

    await manager._connect_device("192.0.2.11", "00:11:22:33:44:66")

    connection.close.assert_awaited_once()
    manager._schedule_reconnect.assert_called_once_with("192.0.2.11", "00:11:22:33:44:66")


@pytest.mark.asyncio
async def test_unknown_shure_protocol_closes_socket_without_writes():
    connection = ShureConnection("192.0.2.12", "00:11:22:33:44:77", MagicMock())
    writer = MagicMock()
    writer.is_closing.return_value = False
    writer.wait_closed = AsyncMock()
    connection.writer = writer
    connection.reader = MagicMock()
    connection._send = AsyncMock()

    await connection.close()

    connection._send.assert_not_awaited()
    writer.close.assert_called_once()
    writer.wait_closed.assert_awaited_once()
    assert connection.reader is None
    assert connection.writer is None


@pytest.mark.asyncio
async def test_explicit_shure_close_does_not_request_reconnect():
    manager = MagicMock()
    connection = ShureConnection(
        "192.0.2.13",
        "00:11:22:33:44:88",
        manager,
    )
    read_started = asyncio.Event()

    async def read_until_cancelled(_size):
        read_started.set()
        await asyncio.Event().wait()

    reader = MagicMock()
    reader.read = AsyncMock(side_effect=read_until_cancelled)
    writer = MagicMock()
    writer.is_closing.return_value = True
    connection.reader = reader
    connection.writer = writer
    connection._running = True
    connection._read_task = asyncio.create_task(connection._read_loop())

    await read_started.wait()
    await connection.close()

    manager._on_connection_lost.assert_called_once_with(
        connection,
        reconnect=False,
    )


@pytest.mark.asyncio
async def test_lost_shure_connection_closes_socket_before_reconnect():
    manager = MagicMock()
    connection = ShureConnection(
        "192.0.2.14",
        "00:11:22:33:44:99",
        manager,
    )
    reader = MagicMock()
    reader.read = AsyncMock(return_value=b"")
    writer = MagicMock()
    writer.is_closing.return_value = False
    writer.wait_closed = AsyncMock()
    connection.reader = reader
    connection.writer = writer
    connection._running = True

    await connection._read_loop()

    writer.close.assert_called_once()
    writer.wait_closed.assert_awaited_once()
    manager._on_connection_lost.assert_called_once_with(
        connection,
        reconnect=True,
    )
    assert connection.reader is None
    assert connection.writer is None


def test_lost_shure_connection_keeps_device_offline_with_last_seen():
    dispatcher = MagicMock()
    manager = ShureManager(dispatcher)
    device = ShureDeviceInfo(
        ip="192.0.2.13",
        mac="00:0e:dd:48:96:29",
        device_type=ShureDeviceType.ad4d,
        name="AD4D-A",
    )
    device.mark_seen(1_700_000_000.0)
    manager.devices[device.mac] = device
    connection = ShureConnection(device.ip, device.mac, manager)
    manager._connections[device.ip] = connection

    manager._on_connection_lost(connection, reconnect=False)

    retained = manager.devices[device.mac]
    assert retained.online is False
    assert retained.last_seen == 1_700_000_000.0
    assert device.ip not in manager._connections
    dispatcher.emit_nowait.assert_called()


@pytest.mark.asyncio
async def test_reconnect_timer_schedules_a_tracked_connect(monkeypatch):
    monkeypatch.setattr(manager_module, "RECONNECT_DELAY", 0.01)
    manager = ShureManager(MagicMock())
    manager._running = True
    connect_started = asyncio.Event()

    async def pending_connect(ip_address, mac_address):
        connect_started.set()
        await asyncio.Event().wait()

    manager._connect_device = pending_connect
    manager._schedule_reconnect("192.0.2.15", "00:11:22:33:44:aa")
    reconnect_task = manager._reconnect_tasks["192.0.2.15"]

    await reconnect_task
    await connect_started.wait()

    assert "192.0.2.15" in manager._connect_tasks
    await manager.stop()
