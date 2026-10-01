from __future__ import annotations

import json
import urllib.error
import urllib.request

from netaudio.daemon.http.mcp_protocol import (
    META_CLIENT_CAPABILITIES,
    META_PROTOCOL_VERSION,
    MODERN_PROTOCOL_VERSION,
)
from netaudio.daemon.mcp_access import ensure_mcp_token

DAEMON_HOST = "127.0.0.1"
META_CLIENT_INFO = "io.modelcontextprotocol/clientInfo"
CLIENT_INFO = {"name": "netaudio daemon mcp-call", "version": "1"}


class McpCallError(Exception):
    pass


def _request(port: int, method: str, params: dict, name: str | None = None, timeout: float = 60.0) -> dict:
    meta = {META_PROTOCOL_VERSION: MODERN_PROTOCOL_VERSION, META_CLIENT_CAPABILITIES: {}, META_CLIENT_INFO: CLIENT_INFO}
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


def list_tools(port: int) -> list[dict]:
    return _request(port, "tools/list", {})["tools"]


def call_tool(port: int, name: str, arguments: dict) -> dict:
    return _request(port, "tools/call", {"name": name, "arguments": arguments}, name=name)
