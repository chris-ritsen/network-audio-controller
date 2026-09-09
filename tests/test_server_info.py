import json
from importlib.metadata import PackageNotFoundError
from types import SimpleNamespace

import pytest

from netaudio.daemon import server_info as server_info_module
from tests.http_api_test_support import get, make_http_server


def install_metadata(monkeypatch, source=None):
    installed = SimpleNamespace(version="1.2.3", read_text=lambda _: source)
    monkeypatch.setattr(server_info_module, "distribution", lambda _: installed)


def test_reports_installed_release_without_inventing_revision(monkeypatch):
    install_metadata(monkeypatch)
    assert server_info_module.server_info() == {"product": "netaudio", "version": "1.2.3"}


def test_reports_git_install_commit_without_source_url(monkeypatch):
    revision = "abcdef0123456789abcdef0123456789abcdef01"
    install_metadata(
        monkeypatch,
        json.dumps({"url": "https://example.invalid/source", "vcs_info": {"vcs": "git", "commit_id": revision}}),
    )
    assert server_info_module.server_info() == {"product": "netaudio", "version": "1.2.3", "git_revision": revision}


@pytest.mark.parametrize(
    "source",
    [
        "{invalid",
        "[]",
        '{"vcs_info": {"vcs": "git", "commit_id": "not-a-revision"}}',
        '{"dir_info": {"editable": true}}',
    ],
)
def test_unavailable_source_metadata_does_not_invent_revision(monkeypatch, source):
    install_metadata(monkeypatch, source)
    assert server_info_module.server_info() == {"product": "netaudio", "version": "1.2.3"}


def test_missing_distribution_does_not_invent_release(monkeypatch):
    def missing(_):
        raise PackageNotFoundError("netaudio")

    monkeypatch.setattr(server_info_module, "distribution", missing)
    assert server_info_module.server_info() == {"product": "netaudio"}


def test_unreadable_source_metadata_keeps_release_available(monkeypatch):
    def unreadable(_):
        raise OSError("metadata is unavailable")

    installed = SimpleNamespace(version="1.2.3", read_text=unreadable)
    monkeypatch.setattr(server_info_module, "distribution", lambda _: installed)
    assert server_info_module.server_info() == {"product": "netaudio", "version": "1.2.3"}


def test_invalid_release_is_not_advertised(monkeypatch):
    installed = SimpleNamespace(version="invalid\nversion", read_text=lambda _: None)
    monkeypatch.setattr(server_info_module, "distribution", lambda _: installed)
    assert server_info_module.server_info() == {"product": "netaudio"}


@pytest.mark.asyncio
async def test_http_and_discovery_report_the_same_backend_version(monkeypatch):
    revision = "abcdef0123456789abcdef0123456789abcdef01"
    install_metadata(monkeypatch, json.dumps({"vcs_info": {"vcs": "git", "commit_id": revision}}))
    server = make_http_server()
    status, body = await get(server, "/server-info")
    assert status == 200
    assert body == {"product": "netaudio", "version": "1.2.3", "git_revision": revision}
    advertisement = server._build_service_info(("192.0.2.10",))
    assert advertisement.properties[b"version"] == b"1"
    assert advertisement.properties[b"server_version"] == b"1.2.3"
    assert advertisement.properties[b"git_revision"] == revision.encode()
    server.application.identify.assert_not_called()


def test_discovery_omits_unavailable_release_and_revision(monkeypatch):
    def missing(_):
        raise PackageNotFoundError("netaudio")

    monkeypatch.setattr(server_info_module, "distribution", missing)
    server = make_http_server()
    advertisement = server._build_service_info(("192.0.2.10",))
    assert advertisement.properties == {b"version": b"1"}
