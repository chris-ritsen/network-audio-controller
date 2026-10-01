from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass

MODERN_PROTOCOL_VERSION = "2026-07-28"
MODERN_PROTOCOL_VERSIONS = (MODERN_PROTOCOL_VERSION,)
LEGACY_PROTOCOL_VERSION = "2025-06-18"
SUPPORTED_PROTOCOL_VERSIONS = (*MODERN_PROTOCOL_VERSIONS, LEGACY_PROTOCOL_VERSION)

META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
META_CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities"
META_SERVER_INFO = "io.modelcontextprotocol/serverInfo"
META_SUBSCRIPTION_ID = "io.modelcontextprotocol/subscriptionId"

HEADER_MISMATCH = -32020
UNSUPPORTED_PROTOCOL_VERSION = -32022
INVALID_PARAMS = -32602
INVALID_REQUEST = -32600

NAMED_METHODS = {"tools/call": "name", "prompts/get": "name", "resources/read": "uri"}
LIST_TIME_TO_LIVE_MILLISECONDS = 60_000
CACHEABLE_METHODS = {
    "server/discover": LIST_TIME_TO_LIVE_MILLISECONDS,
    "tools/list": LIST_TIME_TO_LIVE_MILLISECONDS,
    "prompts/list": LIST_TIME_TO_LIVE_MILLISECONDS,
    "resources/list": LIST_TIME_TO_LIVE_MILLISECONDS,
    "resources/templates/list": LIST_TIME_TO_LIVE_MILLISECONDS,
    "resources/read": 0,
}


@dataclass(frozen=True)
class ModernRejection:
    status: int
    error: dict


def _meta(message: dict) -> dict | None:
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    meta = params.get("_meta")
    return meta if isinstance(meta, dict) else None


def is_modern_request(message, headers: dict) -> bool:
    if isinstance(message, list):
        return headers.get("mcp-protocol-version") in MODERN_PROTOCOL_VERSIONS
    if not isinstance(message, dict):
        return False
    meta = _meta(message)
    if meta is not None and META_PROTOCOL_VERSION in meta:
        return True
    return headers.get("mcp-protocol-version") in MODERN_PROTOCOL_VERSIONS


def _decode_header_value(value: str) -> str | None:
    if value.startswith("=?base64?") and value.endswith("?="):
        try:
            return base64.b64decode(value[len("=?base64?") : -len("?=")], validate=True).decode()
        except (binascii.Error, UnicodeDecodeError):
            return None
    return value


def _rejection(status: int, request_id, code: int, message: str, data: dict | None = None) -> ModernRejection:
    error = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return ModernRejection(status, {"jsonrpc": "2.0", "id": request_id, "error": error})


def validate_modern_request(message, headers: dict) -> ModernRejection | None:
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
        request_id = message.get("id") if isinstance(message, dict) else None
        return _rejection(400, request_id, INVALID_REQUEST, "the body must be a single JSON-RPC request")
    request_id = message.get("id")
    method = message["method"]
    meta = _meta(message) or {}
    body_version = meta.get(META_PROTOCOL_VERSION)
    header_version = headers.get("mcp-protocol-version")
    if not isinstance(body_version, str):
        return _rejection(400, request_id, INVALID_PARAMS, f"_meta.{META_PROTOCOL_VERSION} is required")
    if header_version is None:
        return _rejection(400, request_id, HEADER_MISMATCH, "MCP-Protocol-Version header is required")
    if header_version != body_version:
        return _rejection(
            400,
            request_id,
            HEADER_MISMATCH,
            f"MCP-Protocol-Version header {header_version!r} does not match body version {body_version!r}",
        )
    if body_version not in MODERN_PROTOCOL_VERSIONS:
        return _rejection(
            400,
            request_id,
            UNSUPPORTED_PROTOCOL_VERSION,
            "Unsupported protocol version",
            {"supported": list(SUPPORTED_PROTOCOL_VERSIONS), "requested": body_version},
        )
    if not isinstance(meta.get(META_CLIENT_CAPABILITIES), dict):
        return _rejection(400, request_id, INVALID_PARAMS, f"_meta.{META_CLIENT_CAPABILITIES} is required")
    header_method = headers.get("mcp-method")
    if header_method != method:
        return _rejection(
            400,
            request_id,
            HEADER_MISMATCH,
            f"Mcp-Method header {header_method!r} does not match body method {method!r}",
        )
    name_field = NAMED_METHODS.get(method)
    if name_field is not None:
        body_name = (message.get("params") or {}).get(name_field)
        raw_header_name = headers.get("mcp-name")
        header_name = _decode_header_value(raw_header_name) if isinstance(raw_header_name, str) else None
        if header_name is None or header_name != body_name:
            return _rejection(
                400,
                request_id,
                HEADER_MISMATCH,
                f"Mcp-Name header {raw_header_name!r} does not match body {name_field} {body_name!r}",
            )
    return None


def complete_result(method: str, result: dict, server_info: dict) -> dict:
    completed = {**result, "resultType": "complete"}
    completed["_meta"] = {**(result.get("_meta") or {}), META_SERVER_INFO: server_info}
    time_to_live = CACHEABLE_METHODS.get(method)
    if time_to_live is not None:
        completed["ttlMs"] = time_to_live
        completed["cacheScope"] = "private"
    return completed


def discover_result(instructions: str) -> dict:
    return {
        "supportedVersions": list(MODERN_PROTOCOL_VERSIONS),
        "capabilities": {
            "tools": {"listChanged": True},
            "resources": {"listChanged": True},
            "prompts": {"listChanged": True},
        },
        "instructions": instructions,
    }
