import asyncio
import json
from queue import Queue
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from zeroconf import DNSService, ServiceStateChange

import netaudio.dante.browser as browser_module
from netaudio.dante.browser import DanteBrowser


def test_importing_browser_does_not_patch_process_json_encoder():
    class HasToJson:
        def to_json(self):
            return {"unexpected": "global serialization"}

    with pytest.raises(TypeError):
        json.dumps(HasToJson())


@pytest.mark.asyncio
async def test_async_state_change_publishes_to_configured_queue(monkeypatch):
    info = SimpleNamespace(
        port=4440,
        properties={b"id": b"001122334455"},
        async_request=AsyncMock(return_value=True),
        parsed_addresses=MagicMock(return_value=["192.0.2.20"]),
    )
    monkeypatch.setattr(browser_module, "AsyncServiceInfo", MagicMock(return_value=info))
    record = MagicMock(spec=DNSService)
    record.server = "stagebox.local."
    zeroconf = SimpleNamespace(cache=SimpleNamespace(entries_with_name=MagicMock(return_value=[record])))
    events = Queue()
    browser = DanteBrowser(mdns_timeout=0, queue=events)

    await browser.async_parse_state_change(
        zeroconf,
        "_netaudio-arc._udp.local.",
        "stagebox._netaudio-arc._udp.local.",
        ServiceStateChange.Added,
    )

    message = events.get_nowait()
    assert message["service"]["ipv4"] == "192.0.2.20"
    assert message["service"]["server_name"] == "stagebox.local."
    assert message["service"]["properties"] == {"id": "001122334455"}
    assert message["state_change"]["value"] == ServiceStateChange.Added.value


@pytest.mark.asyncio
async def test_discovery_without_event_queue_skips_duplicate_state_lookup():
    browser = DanteBrowser(mdns_timeout=0)
    browser.async_parse_state_change = AsyncMock()
    browser.async_parse_netaudio_service = AsyncMock(return_value=None)

    browser.async_on_service_state_change(
        MagicMock(),
        "_netaudio-arc._udp.local.",
        "stagebox._netaudio-arc._udp.local.",
        ServiceStateChange.Added,
    )
    await asyncio.gather(*browser.services)

    browser.async_parse_state_change.assert_not_awaited()
    browser.async_parse_netaudio_service.assert_awaited_once()
    assert browser._state_change_tasks == set()


@pytest.mark.asyncio
async def test_browser_close_cancels_and_awaits_state_tasks():
    browser = DanteBrowser(mdns_timeout=0)
    active_browser = SimpleNamespace(async_cancel=AsyncMock())
    active_zeroconf = SimpleNamespace(async_close=AsyncMock())
    browser.aio_browser = active_browser
    browser.aio_zc = active_zeroconf
    cancelled = asyncio.Event()

    async def pending_state_change():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    task = asyncio.create_task(pending_state_change())
    browser._state_change_tasks.add(task)
    await asyncio.sleep(0)

    await browser.async_close()

    assert cancelled.is_set()
    assert task.done()
    assert browser._state_change_tasks == set()
    active_browser.async_cancel.assert_awaited_once()
    active_zeroconf.async_close.assert_awaited_once()


@pytest.mark.asyncio
async def test_browser_close_is_safe_before_start_and_after_close():
    browser = DanteBrowser(mdns_timeout=0)
    active_browser = SimpleNamespace(async_cancel=AsyncMock())
    active_zeroconf = SimpleNamespace(async_close=AsyncMock())
    browser.aio_browser = active_browser
    browser.aio_zc = active_zeroconf

    await browser.async_close()
    await browser.async_close()

    active_browser.async_cancel.assert_awaited_once()
    active_zeroconf.async_close.assert_awaited_once()
    assert browser.aio_browser is None
    assert browser.aio_zc is None


@pytest.mark.asyncio
async def test_browser_close_cancels_and_awaits_service_tasks():
    browser = DanteBrowser(mdns_timeout=0)
    cancelled = asyncio.Event()
    started = asyncio.Event()

    async def pending_service_resolution():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    service_task = asyncio.create_task(pending_service_resolution())
    browser.services.append(service_task)
    await started.wait()

    await browser.async_close()

    assert cancelled.is_set()
    assert service_task.done()
    assert browser.services == []


@pytest.mark.asyncio
async def test_timed_async_run_closes_zeroconf_without_cancelling_service_results(monkeypatch):
    browser = DanteBrowser(mdns_timeout=0.001)
    browser._async_close = AsyncMock()
    monkeypatch.setattr(browser_module, "AsyncZeroconf", MagicMock())
    monkeypatch.setattr(browser_module, "AsyncServiceBrowser", MagicMock())

    await browser.async_run()

    browser._async_close.assert_awaited_once_with(cancel_service_tasks=False)


@pytest.mark.asyncio
async def test_assembling_services_logs_malformed_device_and_continues(caplog):
    event_loop = asyncio.get_running_loop()
    malformed_service = event_loop.create_future()
    malformed_service.set_result(
        {
            "name": "malformed._netaudio-arc._udp.local.",
            "server_name": "malformed.local.",
        }
    )
    valid_service = event_loop.create_future()
    valid_service.set_result(
        {
            "name": "valid._netaudio-arc._udp.local.",
            "server_name": "valid.local.",
        }
    )
    application = MagicMock()
    valid_device = MagicMock()
    application._apply_discovered_services.side_effect = [ValueError("invalid rate"), valid_device]
    browser = DanteBrowser(mdns_timeout=0, app=application)
    browser.services = [malformed_service, valid_service]

    browser._assemble_completed_services()

    assert browser.devices == {"valid.local.": valid_device}
    assert "Failed to assemble discovered Dante device malformed.local." in caplog.text


def test_assembling_services_ignores_resolution_added_after_wait_snapshot():
    event_loop = asyncio.new_event_loop()
    try:
        pending_service = event_loop.create_future()
        application = MagicMock()
        browser = DanteBrowser(mdns_timeout=0, app=application)
        browser.services = [pending_service]

        browser._assemble_completed_services()

        application._apply_discovered_services.assert_not_called()
        assert browser.devices == {}
    finally:
        event_loop.close()
