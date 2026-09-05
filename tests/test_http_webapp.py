import pytest

from netaudio.daemon.http.web import WEBAPP_ROOT, is_application_route, prefers_web_page, resolve_web_asset
from tests.http_api_test_support import FakeWriter, get, make_http_server


def raw_response(writer):
    header, _, body = bytes(writer.data).partition(b"\r\n\r\n")
    decoded_header = header.decode()
    status = int(decoded_header.split(" ")[1])
    headers = {
        name.strip().lower(): value.strip()
        for line in decoded_header.split("\r\n")[1:]
        for name, value in [line.split(":", 1)]
    }
    return status, headers, body


BROWSER_HEADERS = {"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}


async def fetch(http_server, path, headers=None):
    writer = FakeWriter()
    await http_server._dispatch("GET", path, None, writer, headers)
    return raw_response(writer)


class TestAssetResolution:
    def test_root_resolves_to_index(self):
        assert resolve_web_asset("/") == WEBAPP_ROOT.resolve() / "index.html"

    def test_nested_asset_resolves(self):
        assert resolve_web_asset("/views/devices.js") == WEBAPP_ROOT.resolve() / "views" / "devices.js"

    def test_parent_traversal_is_rejected(self):
        assert resolve_web_asset("/../api.py") is None

    def test_encoded_parent_traversal_is_rejected(self):
        assert resolve_web_asset("/views/../../web.py") is None

    def test_unknown_suffix_is_rejected(self):
        assert resolve_web_asset("/web.py") is None

    def test_missing_asset_is_rejected(self):
        assert resolve_web_asset("/missing.js") is None


class TestContentNegotiation:
    def test_browser_accept_prefers_a_web_page(self):
        assert prefers_web_page({"accept": "text/html,application/xhtml+xml"}) is True

    def test_json_client_does_not_prefer_a_web_page(self):
        assert prefers_web_page({"accept": "application/json"}) is False

    def test_missing_headers_do_not_prefer_a_web_page(self):
        assert prefers_web_page(None) is False


class TestApplicationRoutes:
    def test_extensionless_route_is_an_application_route(self):
        assert is_application_route("/devices/avio-usb-1") is True

    def test_root_is_an_application_route(self):
        assert is_application_route("/") is True

    def test_file_route_is_not_an_application_route(self):
        assert is_application_route("/vendor/preact.module.js") is False


class TestAssetServing:
    @pytest.mark.asyncio
    async def test_index_is_served_at_root(self):
        status, headers, body = await fetch(make_http_server(), "/")
        assert status == 200
        assert headers["content-type"] == "text/html; charset=utf-8"
        assert int(headers["content-length"]) == len(body)
        assert b"<title>netaudio</title>" in body

    @pytest.mark.asyncio
    async def test_module_is_served_with_javascript_content_type(self):
        status, headers, body = await fetch(make_http_server(), "/app.js")
        assert status == 200
        assert headers["content-type"] == "text/javascript; charset=utf-8"
        assert b"export" in body or b"import" in body

    @pytest.mark.asyncio
    async def test_stylesheet_is_served(self):
        status, headers, _ = await fetch(make_http_server(), "/app.css")
        assert status == 200
        assert headers["content-type"] == "text/css; charset=utf-8"

    @pytest.mark.asyncio
    async def test_missing_file_returns_json_not_found(self):
        status, body = await get(make_http_server(), "/does-not-exist.js")
        assert status == 404
        assert body == {"error": "not found"}

    @pytest.mark.asyncio
    async def test_vendored_module_is_served(self):
        status, headers, _ = await fetch(make_http_server(), "/vendor/preact.module.js")
        assert status == 200
        assert headers["content-type"] == "text/javascript; charset=utf-8"

    @pytest.mark.asyncio
    async def test_client_route_falls_back_to_index(self):
        status, headers, body = await fetch(make_http_server(), "/routing/studio-media-b", BROWSER_HEADERS)
        assert status == 200
        assert headers["content-type"] == "text/html; charset=utf-8"
        assert b"<title>netaudio</title>" in body

    @pytest.mark.asyncio
    async def test_deep_client_route_falls_back_to_index(self):
        status, _, body = await fetch(make_http_server(), "/devices/avio-usb-1/channels", BROWSER_HEADERS)
        assert status == 200
        assert b"<title>netaudio</title>" in body

    @pytest.mark.asyncio
    async def test_api_routes_are_not_shadowed_by_assets(self):
        status, body = await get(make_http_server(), "/devices")
        assert status == 200
        assert body == {}

    @pytest.mark.asyncio
    async def test_browser_navigation_to_a_device_serves_the_application(self):
        status, headers, body = await fetch(make_http_server(), "/devices/avio-usb-1", BROWSER_HEADERS)
        assert status == 200
        assert headers["content-type"] == "text/html; charset=utf-8"
        assert b"<title>netaudio</title>" in body

    @pytest.mark.asyncio
    async def test_json_client_still_reads_the_device_endpoint(self):
        status, _ = await get(make_http_server(), "/devices/avio-usb-1")
        assert status == 404

    @pytest.mark.asyncio
    async def test_browser_request_for_an_asset_is_still_the_asset(self):
        status, headers, _ = await fetch(make_http_server(), "/app.css", BROWSER_HEADERS)
        assert status == 200
        assert headers["content-type"] == "text/css; charset=utf-8"
