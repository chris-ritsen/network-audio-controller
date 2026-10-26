from __future__ import annotations

import json
import urllib.error
import urllib.request

from netaudio.daemon.mcp_access import ensure_mcp_token

UI_EXTENSION = "io.modelcontextprotocol/ui"
APP_MIME_TYPE = "text/html;profile=mcp-app"
META_CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities"
META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
MODERN_PROTOCOL_VERSION = "2026-07-28"
DAEMON_HOST = "127.0.0.1"
META_CLIENT_INFO = "io.modelcontextprotocol/clientInfo"
CLIENT_INFO = {"name": "netaudio daemon mcp-call", "version": "1"}
APPS_CAPABILITIES = {"extensions": {UI_EXTENSION: {"mimeTypes": [APP_MIME_TYPE]}}}


class McpCallError(Exception):
    pass


def _request(
    port: int, method: str, params: dict, name: str | None = None, timeout: float = 60.0, apps: bool = False
) -> dict:
    meta = {
        META_PROTOCOL_VERSION: MODERN_PROTOCOL_VERSION,
        META_CLIENT_CAPABILITIES: APPS_CAPABILITIES if apps else {},
        META_CLIENT_INFO: CLIENT_INFO,
    }
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": {**params, "_meta": meta}}
    headers = {
        "Authorization": f"Bearer {ensure_mcp_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": MODERN_PROTOCOL_VERSION,
        "Mcp-Method": method,
    }
    if name is not None:
        headers["Mcp-Name"] = name
    request = urllib.request.Request(
        f"http://{DAEMON_HOST}:{port}/mcp", data=json.dumps(body).encode(), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            payload = json.loads(error.read() or b"{}")
        except ValueError:
            payload = {}
        message = (
            (payload.get("error") or {}).get("message")
            if isinstance(payload.get("error"), dict)
            else payload.get("error")
        )
        raise McpCallError(message or f"the daemon answered HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise McpCallError(f"the daemon is not reachable on port {port}: {error.reason}") from error
    if "error" in payload:
        raise McpCallError(payload["error"].get("message", "the MCP request failed"))
    return payload["result"]


def list_tools(port: int, apps: bool = False) -> list[dict]:
    return _request(port, "tools/list", {}, apps=apps)["tools"]


def call_tool(port: int, name: str, arguments: dict, apps: bool = False) -> dict:
    return _request(port, "tools/call", {"name": name, "arguments": arguments}, name=name, apps=apps)


def read_resource(port: int, uri: str, apps: bool = False) -> dict:
    return _request(port, "resources/read", {"uri": uri}, name=uri, apps=apps)
