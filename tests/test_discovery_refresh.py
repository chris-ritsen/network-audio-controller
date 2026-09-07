from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from typer.testing import CliRunner
from zeroconf import DNSIncoming

from netaudio.commands import discovery as discovery_commands
from netaudio.daemon import client
from netaudio.daemon import discovery as daemon_discovery
from netaudio.daemon.discovery import DanteDiscoveryMixin
from netaudio.dante.browser import DanteBrowser
from netaudio.dante.const import SERVICES
from netaudio.dante.discovery import request_discovery
from tests.http_api_test_support import make_http_server, post


@pytest.mark.parametrize("address", [None, "192.0.2.17", "169.254.1.2"])
def test_refresh_encodes_one_standard_query_for_existing_service_types(address):
    zeroconf = SimpleNamespace(started=True, async_send=Mock())
    result = request_discovery(zeroconf, address)
    zeroconf.async_send.assert_called_once()
    arguments, options = zeroconf.async_send.call_args
    assert options == {"addr": address, "port": 5353}
    packets = arguments[0].packets()
    assert len(packets) == 1
    query = DNSIncoming(packets[0])
    assert query.valid
    assert not query.is_response()
    assert {question.name for question in query.questions} == set(SERVICES)
    assert all(question.type == 12 and question.class_ == 1 for question in query.questions)
    assert all(question.unique == (address is None) for question in query.questions)
    assert result == {"query_requested": True, "destination": address or "224.0.0.251", "service_types": SERVICES}


@pytest.mark.parametrize(
    "address", ["", "device.local", "::1", "0.0.0.0", "224.0.0.251", "255.255.255.255", 12, True, []]
)
def test_invalid_directed_address_never_sends(address):
    zeroconf = SimpleNamespace(started=True, async_send=Mock())
    with pytest.raises(ValueError):
        request_discovery(zeroconf, address)
    zeroconf.async_send.assert_not_called()


@pytest.mark.parametrize("zeroconf", [None, SimpleNamespace(started=False, async_send=Mock())])
def test_stopped_discovery_does_not_start_another_browser(zeroconf):
    with pytest.raises(RuntimeError, match="not running"):
        request_discovery(zeroconf)


@pytest.mark.asyncio
async def test_daemon_and_library_reuse_their_active_discovery_transport():
    zeroconf = SimpleNamespace(started=True, async_send=Mock())
    daemon = SimpleNamespace(zeroconf=SimpleNamespace(zeroconf=zeroconf))
    browser = DanteBrowser(0)
    browser.aio_zc = daemon.zeroconf
    expected = browser.refresh_discovery("192.0.2.17")
    assert await DanteDiscoveryMixin.refresh_discovery(daemon, "192.0.2.17") == expected
    assert zeroconf.async_send.call_count == 2
    assert browser.devices == {}


@pytest.mark.parametrize("address", [None, "192.0.2.62"])
def test_daemon_discovery_honors_configured_interface(monkeypatch, address):
    settings = SimpleNamespace(interface="test-interface" if address else None, interface_ip=address)
    monkeypatch.setattr(daemon_discovery, "app_settings", settings)
    transport = Mock(return_value=SimpleNamespace(zeroconf=object()))
    browser = Mock()
    monkeypatch.setattr(daemon_discovery, "AsyncZeroconf", transport)
    monkeypatch.setattr(daemon_discovery, "AsyncServiceBrowser", browser)
    daemon = SimpleNamespace(on_service_state_change=Mock())
    DanteDiscoveryMixin._start_discovery(daemon)
    transport.assert_called_once_with(**({"interfaces": [address]} if address else {}))
    browser.assert_called_once_with(daemon.zeroconf.zeroconf, SERVICES, handlers=[daemon.on_service_state_change])


def test_missing_configured_interface_never_starts_discovery_on_another_interface(monkeypatch):
    monkeypatch.setattr(daemon_discovery, "app_settings", SimpleNamespace(interface="missing", interface_ip=None))
    transport = Mock()
    monkeypatch.setattr(daemon_discovery, "AsyncZeroconf", transport)
    with pytest.raises(RuntimeError, match="configured discovery interface"):
        DanteDiscoveryMixin._start_discovery(SimpleNamespace())
    transport.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("address", [None, "192.0.2.17"])
async def test_http_refresh_requests_queries_without_claiming_discovery(address):
    server = make_http_server()
    result = {"query_requested": True, "destination": address or "224.0.0.251", "service_types": SERVICES}
    server.refresh_discovery = AsyncMock(return_value=result)
    status, body = await post(server, "/discovery/refresh", {"address": address})
    assert status == 200
    assert body == {"success": True, **result}
    server.refresh_discovery.assert_awaited_once_with(address)
    server.state.refresh_all_devices.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_invalid_address_does_not_call_transport():
    server = make_http_server()
    server.refresh_discovery = AsyncMock()
    status, _ = await post(server, "/discovery/refresh", {"address": "0.0.0.0"})
    assert status == 400
    server.refresh_discovery.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("callback", [None, AsyncMock(side_effect=RuntimeError("mDNS discovery is not running"))])
async def test_http_stopped_discovery_returns_service_unavailable(callback):
    server = make_http_server()
    server.refresh_discovery = callback
    status, _ = await post(server, "/discovery/refresh", {})
    assert status == 503


@pytest.mark.asyncio
async def test_client_passes_explicit_destination(monkeypatch):
    request = AsyncMock(return_value=(200, {"success": True}))
    monkeypatch.setattr(client, "_daemon_request", request)
    assert await client.refresh_discovery_on_daemon("192.0.2.17") == (200, {"success": True})
    request.assert_awaited_once_with("POST", "/discovery/refresh", {"address": "192.0.2.17"})


def test_cli_validates_address_before_contacting_daemon(monkeypatch):
    request = AsyncMock()
    monkeypatch.setattr(discovery_commands, "refresh_discovery_on_daemon", request)
    result = CliRunner().invoke(discovery_commands.app, ["--address", "invalid"])
    assert result.exit_code == 2
    request.assert_not_awaited()


@pytest.mark.parametrize(
    "status,body,exit_code",
    [(200, {"success": True, "query_requested": True}, 0), (None, None, 1), (503, {"error": "not running"}, 1)],
)
def test_cli_reports_daemon_result_without_starting_fallback_discovery(monkeypatch, status, body, exit_code):
    request = AsyncMock(return_value=(status, body))
    monkeypatch.setattr(discovery_commands, "refresh_discovery_on_daemon", request)
    result = CliRunner().invoke(discovery_commands.app, ["--address", "192.0.2.17"])
    assert result.exit_code == exit_code, result.output
    request.assert_awaited_once_with("192.0.2.17")
