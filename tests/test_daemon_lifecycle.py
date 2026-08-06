import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.daemon.server import NetaudioDaemon


async def _pending_until_cancelled(cancelled):
    try:
        await asyncio.Event().wait()
    finally:
        cancelled.append(True)


def _component(**methods):
    return SimpleNamespace(**methods)


def test_invalid_shure_correlations_are_reported(monkeypatch, tmp_path, caplog):
    from netaudio.common import config_loader

    config_path = tmp_path / "config.toml"
    config_path.write_text("[shure\n")
    monkeypatch.setattr(config_loader, "default_config_path", lambda: config_path)
    daemon = object.__new__(NetaudioDaemon)

    with caplog.at_level("WARNING", logger="netaudio"):
        correlations = daemon._load_shure_correlations()

    assert correlations == {}
    assert f"Unable to load Shure correlations from {config_path}" in caplog.text


@pytest.mark.asyncio
async def test_concurrent_daemon_stop_is_idempotent_and_awaits_tasks():
    daemon = object.__new__(NetaudioDaemon)
    daemon._startup_task = None
    daemon._stop_event = asyncio.Event()
    daemon._stop_lock = asyncio.Lock()
    daemon._stop_complete = False
    daemon.running = True

    revalidate_cancelled = []
    offline_cancelled = []
    daemon._revalidate_task = asyncio.create_task(_pending_until_cancelled(revalidate_cancelled))
    offline_task = asyncio.create_task(_pending_until_cancelled(offline_cancelled))
    daemon._pending_offline_tasks = {"rack.local.": offline_task}
    daemon._background_tasks = set()
    recovery_task = daemon._spawn_background(
        daemon._recover_known_devices(delay=30, offline_only=True),
        name="recover-known-devices",
    )
    await asyncio.sleep(0)

    daemon._dbus = _component(stop=AsyncMock())
    daemon.heartbeat = _component(stop=AsyncMock())
    daemon.shure = _component(stop=AsyncMock())
    daemon.relay = _component(stop=AsyncMock())
    daemon.metering = _component(stop=AsyncMock())
    daemon._redis = _component(aclose=AsyncMock())
    daemon.application = _component(shutdown=AsyncMock())
    daemon._packet_store = _component(close=MagicMock())
    daemon.browser = _component(async_cancel=AsyncMock())
    daemon.zeroconf = _component(async_close=AsyncMock())

    dbus = daemon._dbus
    heartbeat = daemon.heartbeat
    redis = daemon._redis
    packet_store = daemon._packet_store
    browser = daemon.browser
    zeroconf = daemon.zeroconf

    await asyncio.gather(daemon.stop(), daemon.stop())
    await daemon.stop()

    assert revalidate_cancelled == [True]
    assert offline_cancelled == [True]
    assert recovery_task is not None and recovery_task.done()
    assert daemon._background_tasks == set()
    assert daemon._pending_offline_tasks == {}
    dbus.stop.assert_awaited_once()
    heartbeat.stop.assert_awaited_once()
    daemon.shure.stop.assert_awaited_once()
    daemon.relay.stop.assert_awaited_once()
    daemon.metering.stop.assert_awaited_once()
    redis.aclose.assert_awaited_once()
    daemon.application.shutdown.assert_awaited_once()
    packet_store.close.assert_called_once()
    browser.async_cancel.assert_awaited_once()
    zeroconf.async_close.assert_awaited_once()
    assert daemon._dbus is None
    assert daemon.heartbeat is None
    assert daemon._redis is None
    assert daemon._packet_store is None
    assert daemon.browser is None
    assert daemon.zeroconf is None


@pytest.mark.asyncio
async def test_component_failure_does_not_skip_remaining_shutdown():
    daemon = object.__new__(NetaudioDaemon)
    daemon._startup_task = None
    daemon._stop_event = asyncio.Event()
    daemon._stop_lock = asyncio.Lock()
    daemon._stop_complete = False
    daemon.running = True
    daemon._revalidate_task = None
    daemon._pending_offline_tasks = {}
    daemon._background_tasks = set()
    daemon._dbus = _component(stop=AsyncMock(side_effect=RuntimeError("dbus failed")))
    daemon.heartbeat = _component(stop=AsyncMock(side_effect=RuntimeError("heartbeat failed")))
    daemon.shure = None
    daemon.relay = _component(stop=AsyncMock())
    daemon.metering = _component(stop=AsyncMock())
    daemon._redis = None
    daemon.application = _component(shutdown=AsyncMock())
    daemon._packet_store = None
    daemon.browser = None
    daemon.zeroconf = None

    await daemon.stop()

    daemon.relay.stop.assert_awaited_once()
    daemon.metering.stop.assert_awaited_once()
    daemon.application.shutdown.assert_awaited_once()
    assert daemon._stop_complete is True


@pytest.mark.asyncio
async def test_partial_start_failure_unwinds_started_components():
    daemon = object.__new__(NetaudioDaemon)
    daemon._stop_event = asyncio.Event()
    daemon._start_lock = asyncio.Lock()
    daemon._startup_task = None
    daemon._startup_waiters = 0
    daemon._stop_lock = asyncio.Lock()
    daemon._stop_complete = False
    daemon._revalidate_task = None
    daemon._pending_offline_tasks = {}
    daemon._background_tasks = set()
    daemon._dbus = None
    daemon.heartbeat = None
    daemon.shure = _component(stop=AsyncMock())
    daemon.relay = _component(start=AsyncMock(), stop=AsyncMock())
    daemon.metering = _component(stop=AsyncMock())
    daemon._redis = None
    daemon.application = _component(shutdown=AsyncMock())
    daemon._packet_store = None
    daemon.browser = None
    daemon.zeroconf = None

    entered = asyncio.Event()
    release = asyncio.Event()

    async def partial_start():
        await daemon.relay.start()
        entered.set()
        await release.wait()
        raise RuntimeError("metering startup failed")

    daemon._start_once = partial_start
    first = asyncio.create_task(daemon.start())
    await entered.wait()
    second = asyncio.create_task(daemon.start())
    for _ in range(20):
        if daemon._startup_waiters == 2:
            break
        await asyncio.sleep(0)
    release.set()

    results = await asyncio.gather(first, second, return_exceptions=True)

    assert all(isinstance(result, RuntimeError) for result in results)
    assert all("metering startup failed" in str(result) for result in results)

    daemon.relay.start.assert_awaited_once()
    daemon.relay.stop.assert_awaited_once()
    daemon.shure.stop.assert_awaited_once()
    daemon.metering.stop.assert_awaited_once()
    daemon.application.shutdown.assert_awaited_once()
    assert daemon._stop_complete is True


@pytest.mark.asyncio
async def test_concurrent_start_callers_share_one_initialization_and_exit_together():
    daemon = object.__new__(NetaudioDaemon)
    daemon._stop_event = asyncio.Event()
    daemon._start_lock = asyncio.Lock()
    daemon._startup_task = None
    daemon._startup_waiters = 0
    daemon._stop_lock = asyncio.Lock()
    daemon._stop_complete = False
    daemon._revalidate_task = None
    daemon._pending_offline_tasks = {}
    daemon._background_tasks = set()
    daemon._dbus = None
    daemon.heartbeat = None
    daemon.shure = None
    daemon.relay = _component(stop=AsyncMock())
    daemon.metering = _component(stop=AsyncMock())
    daemon._redis = None
    daemon.application = _component(shutdown=AsyncMock())
    daemon._packet_store = None
    daemon.browser = None
    daemon.zeroconf = None
    initialized = asyncio.Event()
    initialize_calls = 0

    async def initialize_once():
        nonlocal initialize_calls
        initialize_calls += 1
        initialized.set()

    daemon._start_once = initialize_once
    first = asyncio.create_task(daemon.start())
    second = asyncio.create_task(daemon.start())
    await initialized.wait()
    await asyncio.sleep(0)

    assert initialize_calls == 1
    assert not first.done()
    assert not second.done()

    daemon.request_shutdown()
    await asyncio.gather(first, second)
    await daemon.stop()

    assert initialize_calls == 1
    daemon.relay.stop.assert_awaited_once()
