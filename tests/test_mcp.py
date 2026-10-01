import asyncio
import json

import pytest

from netaudio.daemon.http.mcp import MCP_PROTOCOL_VERSION, RESOURCES, TOOLS
from netaudio.daemon.mcp_access import authorization_matches
from netaudio.dante.device import DanteDevice
from tests.http_api_test_support import FakeWriter, make_http_server

TOKEN = "test-token"
AUTHORIZED = {"authorization": f"Bearer {TOKEN}", "content-type": "application/json"}


async def rpc(server, method, params=None, *, request_id=1, headers=AUTHORIZED):
    writer = FakeWriter()
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    await server._route("POST", "/mcp", json.dumps(message).encode(), writer, None, headers)
    return writer.response()


def warmed_up(server):
    server.server_info["started_at"] = "2000-01-01T00:00:00Z"
    return server


def make_server():
    device = DanteDevice(server_name="lx-dante.local.")
    device.name = "lx-dante"
    return warmed_up(make_http_server(devices={device.server_name: device}))


def test_catalog_is_sorted_and_confirmation_tools_are_destructive():
    assert [tool.name for tool in TOOLS] == sorted(tool.name for tool in TOOLS)
    assert [resource.uri for resource in RESOURCES] == sorted(resource.uri for resource in RESOURCES)
    for tool in TOOLS:
        assert set(tool.input_schema["required"]) <= set(tool.input_schema["properties"])
        if not tool.read_only and tool.name not in {"apply_to_devices", "invoke_tool", "save_preset"}:
            assert tool.requires_confirmation
            assert "confirmed" in tool.input_schema["properties"]


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
async def test_mcp_http_response_advertises_its_one_request_connection_lifetime():
    server = make_server()
    listener = await asyncio.start_server(server.handle_connection, "127.0.0.1", 0)
    try:
        port = listener.sockets[0].getsockname()[1]
        for request, expected in (
            (
                b"POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-token\r\n"
                b'Content-Length: 40\r\n\r\n{"jsonrpc":"2.0","id":1,"method":"ping"}',
                "result",
            ),
            (b"GET /server-info HTTP/1.1\r\nHost: localhost\r\n\r\n", "version"),
        ):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(request)
            await writer.drain()
            response = await asyncio.wait_for(reader.read(), timeout=3)
            writer.close()
            await writer.wait_closed()
            headers, _, payload = response.partition(b"\r\n\r\n")
            assert b"HTTP/1.1 200 OK" in headers
            assert b"Connection: close\r\n" in headers + b"\r\n"
            assert expected in json.loads(payload)
    finally:
        listener.close()
        await listener.wait_closed()


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
    assert "set_subscriptions" in names and "invoke_tool" in names
    assert "get_routing" in names
    assert "get_network_overview" in names
    assert names["get_routing"]["annotations"]["readOnlyHint"] is True
    assert "detail" in names["get_clock_status"]["inputSchema"]["properties"]
    assert names["set_subscriptions"]["annotations"]["destructiveHint"] is True
    assert names["set_subscriptions"]["inputSchema"]["required"] == ["routes"]


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
    status, body = await rpc(
        server, "tools/call", {"name": "identify", "arguments": {"device": "lx-dante", "confirmed": True}}
    )
    assert status == 200
    result = body["result"]
    assert result["isError"] is False
    assert json.loads(result["content"][0]["text"])["accepted"] is True
    server.application.identify.assert_awaited_once()


@pytest.mark.asyncio
async def test_public_mcp_views_keep_debug_evidence_opt_in():
    server = make_server()
    device = {
        "name": "lx-dante",
        "server_name": "lx-dante.local.",
        "clock_role": "Leader",
        "clock_status": {"raw_record": [1] * 4000, "synchronization": "synchronized"},
        "channels": {"receivers": {"15": {"name": "vrroom:left"}}},
        "subscriptions": [
            {
                "rx_channel": "vrroom:left",
                "tx_device": "sender",
                "tx_channel": "01",
                "status": {"label": "Subscribed", "severity": "ok"},
            }
        ],
    }

    async def dispatch(method, path, body, writer, headers):
        payload = json.dumps({"lx-dante.local.": device}).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
            + str(len(payload)).encode()
            + b"\r\n\r\n"
            + payload
        )

    server._dispatch = dispatch
    _, body = await rpc(server, "tools/call", {"name": "get_clock_status", "arguments": {}})
    clock = json.loads(body["result"]["content"][0]["text"])
    assert "raw_record" not in json.dumps(clock)
    _, body = await rpc(server, "tools/call", {"name": "get_clock_status", "arguments": {"detail": "debug"}})
    debug = json.loads(body["result"]["content"][0]["text"])
    assert debug["raw_clock_records"]["lx-dante.local."]["clock_status"]["raw_record_hexadecimal"]
    _, body = await rpc(server, "tools/call", {"name": "get_routing", "arguments": {}})
    routes = json.loads(body["result"]["content"][0]["text"])["routes"]
    assert routes == {"lx-dante": ["15 vrroom:left ← 01 on sender"]}


@pytest.mark.asyncio
async def test_route_tool_resolves_labels_and_inventory_ids_before_dispatch():
    server = make_server()
    receiver = {
        "server_name": "lx-dante.local.",
        "name": "lx-dante",
        "inventory_id": "001dc1081258",
        "channels": {"receivers": {"15": {"name": "vrroom:left"}}},
    }
    transmitter = {
        "server_name": "sender.local.",
        "name": "sender",
        "inventory_id": "aabbccddeeff",
        "channels": {"transmitters": {"1": {"name": "01"}}},
    }
    server._serialized_devices = lambda: {"lx-dante.local.": receiver, "sender.local.": transmitter}
    dispatched = []

    async def capture(writer, params):
        dispatched.append(params)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")

    server.post_handlers["/subscriptions/apply"] = capture
    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "set_subscriptions",
            "arguments": {
                "confirmed": True,
                "routes": [
                    {
                        "rx_device": "001dc1081258",
                        "rx_channel": "vrroom:left",
                        "tx_device": "aabbccddeeff",
                        "tx_channel": 1,
                    }
                ],
            },
        },
    )
    assert body["result"]["isError"] is False
    assert dispatched[0]["routes"] == [
        {"rx_device": "lx-dante.local.", "rx_channel": 15, "tx_device": "sender", "tx_channel": "01"}
    ]

    receiver["channels"]["receivers"]["16"] = {"name": "vrroom:left"}
    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "set_subscriptions",
            "arguments": {
                "confirmed": True,
                "routes": [
                    {"rx_device": "lx-dante", "rx_channel": "vrroom:left", "tx_device": "sender", "tx_channel": "01"}
                ],
            },
        },
    )
    assert body["result"]["isError"] is True
    assert "ambiguous" in json.loads(body["result"]["content"][0]["text"])["error"]
    assert len(dispatched) == 1


@pytest.mark.asyncio
async def test_advanced_controls_are_typed_and_dispatch_to_existing_handlers():
    server = make_server()
    calls = []

    def capture(path):
        async def handler(writer, params):
            calls.append((path, params))
            writer.write(
                b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{"success":true}'
            )

        return handler

    for path in ("/device-controls", "/set-unicast-performance", "/set-clock-configuration"):
        server.post_handlers[path] = capture(path)

    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "plan_device_control",
            "arguments": {"device": "lx-dante", "category": "hdcp", "requested": {"mode": 1}},
        },
    )
    assert body["result"]["isError"] is False
    assert calls[-1] == (
        "/device-controls",
        {"action": "plan", "device": "lx-dante.local.", "category": "hdcp", "requested": {"mode": 1}},
    )

    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "apply_device_control",
            "arguments": {"device": "lx-dante", "category": "hdcp", "requested": {"mode": 1}, "confirmed": False},
        },
    )
    assert json.loads(body["result"]["content"][0]["text"])["changed"] is False
    assert [call[1]["action"] for call in calls] == ["plan", "plan"]

    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "apply_device_control",
            "arguments": {"device": "lx-dante", "category": "hdcp", "requested": {"mode": 1}, "confirmed": True},
        },
    )
    assert body["result"]["isError"] is False
    assert calls[-1][1]["action"] == "apply"

    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "configure_flow_performance",
            "arguments": {
                "device": "lx-dante",
                "kind": "unicast",
                "latency_microseconds": 1000,
                "frames_per_packet": 4,
                "confirmed": True,
            },
        },
    )
    assert body["result"]["isError"] is False
    assert calls[-1] == (
        "/set-unicast-performance",
        {"device": "lx-dante.local.", "latency_microseconds": 1000, "frames_per_packet": 4, "confirmed": True},
    )

    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "configure_clock",
            "arguments": {"device": "lx-dante", "changes": {"preferred_leader": True}, "confirmed": True},
        },
    )
    assert body["result"]["isError"] is False
    assert calls[-1][0] == "/set-clock-configuration"


@pytest.mark.asyncio
async def test_inventory_id_selects_a_device_for_a_regular_control():
    server = make_server()
    real_serialize = server._serialized_devices

    def serialized():
        records = real_serialize()
        records["lx-dante.local."]["inventory_id"] = "001dc1081258"
        return records

    server._serialized_devices = serialized
    _, body = await rpc(
        server, "tools/call", {"name": "identify", "arguments": {"device": "001dc1081258", "confirmed": True}}
    )
    assert body["result"]["isError"] is False
    server.application.identify.assert_awaited_once()


@pytest.mark.asyncio
async def test_metering_session_is_scoped_to_the_mcp_client():
    server = make_server()
    calls = []

    async def capture(writer, params):
        calls.append(params)
        writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{"success":true}')

    server.post_handlers["/metering/start"] = capture
    server.post_handlers["/metering/stop"] = capture
    for name in ("start_metering", "stop_metering"):
        _, body = await rpc(
            server, "tools/call", {"name": name, "arguments": {"device": "lx-dante", "confirmed": True}}
        )
        assert body["result"]["isError"] is False
    assert [call["client_id"] for call in calls] == ["mcp:local-token", "mcp:local-token"]
    assert all(call["device"] == "lx-dante.local." for call in calls)


class RecordingMetering:
    def __init__(self):
        self.snapshots = []

    async def snapshot(self, server_name, timeout=3.0):
        self.snapshots.append(server_name)

    def get_cached_levels_by_server(self):
        return {"lx-dante.local.": {"metering_source": "detailed", "tx": {1: 40}, "rx": {}}}


@pytest.mark.asyncio
async def test_signal_levels_request_detailed_metering_from_devices_without_passive_levels():
    devices = {}
    for server_name, passive in (("lx-dante.local.", False), ("avio-usb-1.local.", True), ("mixer.local.", None)):
        device = DanteDevice(server_name=server_name)
        device.name = server_name.removesuffix(".local.")
        device.ipv4 = "192.168.1.10"
        device.detailed_metering_supported = True
        device.per_channel_signal_presence_supported = passive
        devices[server_name] = device
    metering = RecordingMetering()
    server = warmed_up(make_http_server(devices=devices, metering=metering))

    _, body = await rpc(server, "tools/call", {"name": "get_signal_levels", "arguments": {}})
    assert body["result"]["isError"] is False
    assert "lx-dante" in json.loads(body["result"]["content"][0]["text"])["devices"]
    assert sorted(metering.snapshots) == ["lx-dante.local.", "mixer.local."]

    metering.snapshots.clear()
    await rpc(server, "tools/call", {"name": "get_signal_levels", "arguments": {"device": "lx-dante"}})
    assert metering.snapshots == ["lx-dante.local."]


@pytest.mark.asyncio
async def test_ddm_credential_arguments_are_dispatched_without_logging_them(caplog):
    server = make_server()
    calls = []

    async def capture(writer, params):
        calls.append(params)
        writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{"success":true}')

    server.post_handlers["/ddm/login"] = capture
    secret = "credential-value-in-test"
    _, body = await rpc(
        server,
        "tools/call",
        {
            "name": "login_ddm",
            "arguments": {"server": "lab", "method": "api_key", "api_key": secret, "confirmed": True},
        },
    )
    assert body["result"]["isError"] is False
    assert calls[0]["api_key"] == secret
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_network_overview_combines_three_read_only_api_snapshots():
    server = make_server()
    snapshots = {
        "/devices": {
            "lx-dante.local.": {
                "name": "lx-dante",
                "online": True,
                "clock_role": "Leader",
                "sample_rate_hz": 48000,
                "subscriptions": [],
            }
        },
        "/issues": {
            "count": 1,
            "active_count": 1,
            "issues": [{"issue_id": "one", "state": "open", "summary": "Missing source"}],
        },
        "/ddm/status": {"enabled": True, "state": "degraded", "server_count": 1, "domain_count": 0},
        "/event-journal": {"count": 0, "events": []},
    }
    paths = []

    async def dispatch(method, path, body, writer, headers):
        paths.append(path)
        payload = json.dumps(snapshots[path]).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
            + str(len(payload)).encode()
            + b"\r\n\r\n"
            + payload
        )

    server._dispatch = dispatch
    _, body = await rpc(server, "tools/call", {"name": "get_network_overview", "arguments": {}})
    overview = json.loads(body["result"]["content"][0]["text"])
    assert paths == ["/devices", "/issues", "/ddm/status", "/event-journal"]
    assert overview["clock_leaders"] == {"unmanaged": ["lx-dante"]}
    assert overview["open_issue_count"] == 1
    assert overview["ddm"]["state"] == "degraded"
    assert overview["devices"][0]["sample_rate_hz"] == 48000


@pytest.mark.asyncio
async def test_tool_call_reports_handler_errors_and_validates_arguments():
    server = make_server()
    status, body = await rpc(
        server, "tools/call", {"name": "identify", "arguments": {"device": "missing", "confirmed": True}}
    )
    assert body["result"]["isError"] is True
    assert "no device named 'missing'" in json.loads(body["result"]["content"][0]["text"])["error"]

    status, body = await rpc(server, "tools/call", {"name": "identify", "arguments": {}})
    assert body["result"]["isError"] is True
    assert "device" in json.loads(body["result"]["content"][0]["text"])["problems"][0]

    status, body = await rpc(
        server, "tools/call", {"name": "identify", "arguments": {"device": "lx-dante", "extra": 1}}
    )
    assert body["result"]["isError"] is True

    status, body = await rpc(server, "tools/call", {"name": "explode", "arguments": {}})
    assert body["error"]["code"] == -32602


@pytest.mark.asyncio
async def test_destructive_tools_require_confirmation_before_reaching_the_handler():
    server = make_server()
    status, body = await rpc(
        server, "tools/call", {"name": "reboot", "arguments": {"device": "lx-dante", "confirmed": False}}
    )
    assert body["result"]["isError"] is False
    assert json.loads(body["result"]["content"][0]["text"])["confirmation_required"] is True
    server.application.reboot.assert_not_awaited()

    status, body = await rpc(
        server, "tools/call", {"name": "reboot", "arguments": {"device": "lx-dante", "confirmed": True}}
    )
    assert body["result"]["isError"] is False
    server.application.reboot.assert_awaited_once()

    status, body = await rpc(
        server, "tools/call", {"name": "identify", "arguments": {"device": "lx-dante", "confirmed": False}}
    )
    assert json.loads(body["result"]["content"][0]["text"])["confirmation_required"] is True
    server.application.identify.assert_not_awaited()


@pytest.mark.asyncio
async def test_resources_read_the_get_routes():
    from netaudio.dante.device import DanteDevice

    device = DanteDevice(server_name="lx-dante.local.")
    device.name = "lx-dante"
    server = warmed_up(make_http_server(devices={device.server_name: device}))
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
    summary = json.loads(body["result"]["content"][0]["text"])["lx-dante"]
    assert "routes" not in summary and "route_problems" not in summary
    assert "channels" not in summary

    status, body = await rpc(server, "tools/call", {"name": "get_device", "arguments": {"device": "lx-dante"}})
    device_payload = json.loads(body["result"]["content"][0]["text"])
    assert device_payload["name"] == "lx-dante"
    assert "channels" not in device_payload

    status, body = await rpc(
        server,
        "tools/call",
        {"name": "get_device", "arguments": {"device": "lx-dante", "sections": ["channels", "full"]}},
    )
    full_payload = json.loads(body["result"]["content"][0]["text"])
    assert "channels" in full_payload
    assert "server_name" in full_payload

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
