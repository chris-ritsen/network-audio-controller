from unittest.mock import Mock

import pytest

from netaudio.common.app_config import settings
from tests.http_api_test_support import FakeWriter, make_http_server


@pytest.mark.asyncio
async def test_import_copies_configured_key_without_extraction(monkeypatch):
    key = "0123456789abcdef0123456789abcdef"
    monkeypatch.setattr(settings, "device_lock_key", key.encode())
    finder = Mock(side_effect=AssertionError("Configured keys must not trigger extraction"))
    monkeypatch.setattr("netaudio.common.key_extract.find_dante_controller_binary", finder)
    writer = FakeWriter()
    await make_http_server()._dispatch("POST", "/device-lock-key", b"{}", writer)
    assert writer.response() == (200, {"key": key})
    finder.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", ["darwin", "win32"])
async def test_import_extracts_when_no_key_is_configured(monkeypatch, tmp_path, platform):
    key = "0123456789abcdef0123456789abcdef"
    binary = tmp_path / "synthetic-controller"
    monkeypatch.setattr(settings, "device_lock_key", None)
    monkeypatch.setattr("netaudio.daemon.http.connections.sys.platform", platform)
    monkeypatch.setattr("netaudio.common.key_extract.find_dante_controller_binary", lambda: binary)
    extractor = Mock(return_value=key.encode())
    monkeypatch.setattr("netaudio.common.key_extract.extract_key_from_binary", extractor)
    writer = FakeWriter()
    await make_http_server()._dispatch("POST", "/device-lock-key", b"{}", writer)
    assert writer.response() == (200, {"key": key})
    extractor.assert_called_once_with(binary)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("platform", "installed", "status"), [("linux", False, 501), ("darwin", False, 404), ("win32", True, 422)]
)
async def test_import_reports_extraction_failures(monkeypatch, tmp_path, platform, installed, status):
    monkeypatch.setattr(settings, "device_lock_key", None)
    monkeypatch.setattr("netaudio.daemon.http.connections.sys.platform", platform)
    monkeypatch.setattr(
        "netaudio.common.key_extract.find_dante_controller_binary",
        lambda: tmp_path / "synthetic" if installed else None,
    )
    monkeypatch.setattr("netaudio.common.key_extract.extract_key_from_binary", lambda path: None)
    writer = FakeWriter()
    await make_http_server()._dispatch("POST", "/device-lock-key", b"{}", writer)
    assert writer.response()[0] == status
    assert "error" in writer.response()[1]


@pytest.mark.asyncio
async def test_import_rejects_cross_origin_browser_requests(monkeypatch):
    handler = Mock(side_effect=AssertionError("Cross-origin requests must not read keys"))
    server = make_http_server()
    server.post_handlers["/device-lock-key"] = handler
    writer = FakeWriter()
    await server._dispatch(
        "POST", "/device-lock-key", b"{}", writer, {"host": "localhost:9000", "origin": "https://example.com"}
    )
    assert writer.response()[0] == 403
    handler.assert_not_called()
