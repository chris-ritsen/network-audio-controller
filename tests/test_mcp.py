import json

import pytest

from netaudio.daemon.http.mcp import MCP_PROTOCOL_VERSION, RESOURCES, TOOLS
from netaudio.daemon.mcp_access import authorization_matches
from tests.http_api_test_support import FakeWriter, make_device, make_http_server

TOKEN = "test-token"
AUTHORIZED = {"authorization": f"Bearer {TOKEN}", "content-type": "application/json"}


async def rpc(server, method, params=None, *, request_id=1, headers=AUTHORIZED):
    writer = FakeWriter()
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    await server._route("POST", "/mcp", json.dumps(message).encode(), writer, None, headers)
    return writer.response()


def make_server():
    device = make_device(server_name="lx-dante.local.", name="lx-dante")
    return make_http_server(devices={device.server_name: device})


def test_catalog_is_sorted_and_confirmation_tools_are_destructive():
    assert [tool.name for tool in TOOLS] == sorted(tool.name for tool in TOOLS)
    assert [resource.uri for resource in RESOURCES] == sorted(resource.uri for resource in RESOURCES)
    for tool in TOOLS:
        assert set(tool.input_schema["required"]) <= set(tool.input_schema["properties"])
        if tool.requires_confirmation:
            assert tool.destructive
            assert "confirmed" in tool.input_schema["required"]


def test_bearer_token_comparison():
    assert authorization_matches("Bearer abc", "abc")
    assert authorization_matches("bearer abc", "abc")
    assert not authorization_matches("Bearer abd", "abc")
    assert not authorization_matches("Basic abc", "abc")
    assert not authorization_matches(None, "abc")
    assert not authorization_matches("Bearer abc", None)


@pytest.mark.asyncio
async def test_requests_without_the_token_are_rejected():
    server = make_server()
    status, body = await rpc(server, "tools/list", headers={"content-type": "application/json"})
    assert status == 401
    assert "mcp-token" in body["error"]
    status, _ = await rpc(server, "tools/list", headers={"authorization": "Bearer wrong"})
    assert status == 401


@pytest.mark.asyncio
async def test_initialize_lists_capabilities_and_tools():
    server = make_server()
    status, body = await rpc(
        server,
        "initialize",
        {"protocolVersion": MCP_PROTOCOL_VERSION, "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}},
    )
    assert status == 200
    assert body["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION
    assert body["result"]["serverInfo"]["name"] == "netaudio"
    assert "tools" in body["result"]["capabilities"]

    status, body = await rpc(server, "tools/list")
    names = {tool["name"]: tool for tool in body["result"]["tools"]}
    assert "subscribe" in names and "reboot" in names
    assert names["reboot"]["annotations"]["destructiveHint"] is True
    assert names["identify"]["annotations"]["destructiveHint"] is False
    assert names["subscribe"]["inputSchema"]["required"] == ["rx_channel", "rx_device", "tx_channel", "tx_device"]


@pytest.mark.asyncio
async def test_notifications_get_202_and_unknown_methods_are_errors():
    server = make_server()
    writer = FakeWriter()
    await server._route(
        "POST",
        "/mcp",
        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode(),
        writer,
        None,
        AUTHORIZED,
    )
    assert writer.response()[0] == 202
    status, body = await rpc(server, "nope")
    assert status == 200
    assert body["error"]["code"] == -32601


@pytest.mark.asyncio
async def test_tool_call_runs_the_http_handler():
    server = make_server()
    status, body = await rpc(server, "tools/call", {"name": "identify", "arguments": {"device": "lx-dante"}})
    assert status == 200
    result = body["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["accepted"] is True
    server.application.identify.assert_awaited_once()


@pytest.mark.asyncio
async def test_tool_call_reports_handler_errors_and_validates_arguments():
    server = make_server()
    status, body = await rpc(server, "tools/call", {"name": "identify", "arguments": {"device": "missing"}})
    assert body["result"]["isError"] is True
    assert "not found" in body["result"]["structuredContent"]["error"]

    status, body = await rpc(server, "tools/call", {"name": "identify", "arguments": {}})
    assert body["error"]["code"] == -32602
    assert "device" in body["error"]["message"]

    status, body = await rpc(
        server, "tools/call", {"name": "identify", "arguments": {"device": "lx-dante", "extra": 1}}
    )
    assert body["error"]["code"] == -32602

    status, body = await rpc(server, "tools/call", {"name": "explode", "arguments": {}})
    assert body["error"]["code"] == -32602


@pytest.mark.asyncio
async def test_destructive_tools_require_confirmation_before_reaching_the_handler():
    server = make_server()
    status, body = await rpc(
        server, "tools/call", {"name": "reboot", "arguments": {"device": "lx-dante", "confirmed": False}}
    )
    assert body["result"]["isError"] is True
    assert "confirm" in body["result"]["structuredContent"]["error"]
    server.application.reboot.assert_not_awaited()

    status, body = await rpc(
        server, "tools/call", {"name": "reboot", "arguments": {"device": "lx-dante", "confirmed": True}}
    )
    assert body["result"]["isError"] is False
    server.application.reboot.assert_awaited_once()


@pytest.mark.asyncio
async def test_resources_read_the_get_routes():
    from netaudio.dante.device import DanteDevice

    device = DanteDevice(server_name="lx-dante.local.")
    device.name = "lx-dante"
    server = make_http_server(devices={device.server_name: device})
    status, body = await rpc(server, "resources/list")
    uris = [resource["uri"] for resource in body["result"]["resources"]]
    assert "netaudio://devices" in uris

    status, body = await rpc(server, "resources/read", {"uri": "netaudio://devices"})
    contents = body["result"]["contents"][0]
    assert contents["mimeType"] == "application/json"
    assert "lx-dante.local." in json.loads(contents["text"])

    status, body = await rpc(server, "resources/read", {"uri": "netaudio://devices/lx-dante"})
    assert json.loads(body["result"]["contents"][0]["text"])["name"] == "lx-dante"

    status, body = await rpc(server, "resources/read", {"uri": "netaudio://devices/missing"})
    assert body["error"]["code"] == -32602

    status, body = await rpc(server, "resources/read", {"uri": "netaudio://nothing"})
    assert body["error"]["code"] == -32602

    status, body = await rpc(server, "tools/call", {"name": "list_devices", "arguments": {}})
    assert body["result"]["isError"] is False
    summary = body["result"]["structuredContent"]["lx-dante.local."]
    assert summary["name"] == "lx-dante"
    assert summary["subscription_count"] == 0 and summary["subscription_problems"] == []
    assert "channels" not in summary

    status, body = await rpc(server, "tools/call", {"name": "get_device", "arguments": {"device": "lx-dante"}})
    assert body["result"]["structuredContent"]["name"] == "lx-dante"
    assert "channels" not in body["result"]["structuredContent"]

    status, body = await rpc(
        server,
        "tools/call",
        {"name": "get_device", "arguments": {"device": "lx-dante", "sections": ["channels", "full"]}},
    )
    assert "channels" in body["result"]["structuredContent"]
    assert "server_name" in body["result"]["structuredContent"]

    status, body = await rpc(server, "tools/call", {"name": "get_device", "arguments": {"device": "missing"}})
    assert body["result"]["isError"] is True


@pytest.mark.asyncio
async def test_batches_and_unsupported_http_methods():
    server = make_server()
    writer = FakeWriter()
    batch = [
        {"jsonrpc": "2.0", "id": 1, "method": "ping"},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
    ]
    await server._route("POST", "/mcp", json.dumps(batch).encode(), writer, None, AUTHORIZED)
    status, body = writer.response()
    assert status == 200
    assert body == [{"jsonrpc": "2.0", "id": 1, "result": {}}]

    writer = FakeWriter()
    await server._route("GET", "/mcp", None, writer, None, AUTHORIZED)
    assert writer.response()[0] == 405


@pytest.mark.asyncio
async def test_server_info_and_discovery_advertise_mcp():
    server = make_server()
    assert server.server_info["mcp"]["path"] == "/mcp"
    advertisement = server._build_service_info(("192.0.2.10",))
    assert advertisement.properties[b"mcp_path"] == b"/mcp"
