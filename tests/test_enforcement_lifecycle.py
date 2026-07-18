import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import netaudio.daemon.enforcement as enforcement_module
from netaudio.daemon.enforcement import EnforcementManager
from netaudio.dante.events import EventType


def _manager() -> tuple[EnforcementManager, MagicMock]:
    dispatcher = MagicMock()
    daemon = SimpleNamespace(application=SimpleNamespace(dispatcher=dispatcher, devices={}), shure=None)
    return EnforcementManager(daemon), dispatcher


def test_enforcement_listeners_register_and_unregister_once():
    manager, dispatcher = _manager()

    manager._register_event_listeners()
    manager._register_event_listeners()
    manager._unregister_event_listeners()
    manager._unregister_event_listeners()

    assert dispatcher.on.call_count == 4
    assert dispatcher.off.call_count == 4
    dispatcher.off.assert_any_call(EventType.DEVICE_DISCOVERED, manager._on_device_event)
    dispatcher.off.assert_any_call(EventType.SHURE_DEVICE_UPDATED, manager._on_shure_device_event)


@pytest.mark.asyncio
async def test_enforcement_stop_cancels_and_awaits_all_tasks(monkeypatch, tmp_path):
    manager, _ = _manager()
    manager._running = True
    cancelled = asyncio.Event()

    async def pending_command(mac, settings):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    manager._execute_shure_command = pending_command
    monkeypatch.setattr(enforcement_module, "SOCKET_PATH", str(tmp_path / "enforce.sock"))

    manager._schedule_shure_command("00:11:22:33:44:55", {})
    command_task = next(iter(manager._command_tasks))
    await asyncio.sleep(0)
    await manager.stop()

    assert cancelled.is_set()
    assert command_task.done()
    assert manager._command_tasks == set()
    assert manager._enforce_tasks == {}


@pytest.mark.asyncio
async def test_enforcement_stop_awaits_delayed_tasks(monkeypatch, tmp_path):
    manager, _ = _manager()
    manager._running = True
    task = asyncio.create_task(asyncio.Event().wait())
    manager._enforce_tasks["dante:stagebox"] = task
    monkeypatch.setattr(enforcement_module, "SOCKET_PATH", str(tmp_path / "enforce.sock"))

    await manager.stop()

    assert task.cancelled()
    assert manager._enforce_tasks == {}
