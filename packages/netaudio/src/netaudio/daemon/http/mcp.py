from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from netaudio.daemon.http.mcp_views import (
    DEVICE_SECTIONS,
    clock_status_view,
    compact,
    device_summary,
    device_view,
    events_view,
    issues_view,
    signal_levels_view,
)
from netaudio.daemon.mcp_access import authorization_matches
from netaudio.daemon.mcp_oauth import SCOPE_WRITE, Grant, OAuthStore

logger = logging.getLogger("netaudio.mcp")
logger.setLevel(logging.INFO)

MCP_PATH = "/mcp"
MCP_PROTOCOL_VERSION = "2025-06-18"
JSON_RPC_INVALID_REQUEST = -32600
JSON_RPC_METHOD_NOT_FOUND = -32601
JSON_RPC_INVALID_PARAMS = -32602
JSON_RPC_PARSE_ERROR = -32700

DEVICE_DESCRIPTION = "Device name or server name as returned by list_devices."
CONFIRMATION_DESCRIPTION = "Must be true to perform this change. Ask the user before setting it."


def _device_property() -> dict:
    return {"type": "string", "description": DEVICE_DESCRIPTION}


def _schema(properties: dict, required: list[str]) -> dict:
    return {"type": "object", "properties": properties, "required": required, "additionalProperties": False}


@dataclass(frozen=True)
class McpTool:
    name: str
    path: str
    description: str
    input_schema: dict
    destructive: bool = False
    method: str = "POST"
    requires_confirmation: bool = False
    read_only: bool = False
    defaults: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
            "annotations": {
                "destructiveHint": self.destructive,
                "idempotentHint": not self.destructive,
                "openWorldHint": False,
                "readOnlyHint": self.read_only,
                "title": self.name.replace("_", " ").capitalize(),
            },
        }


@dataclass(frozen=True)
class McpResource:
    uri: str
    path: str
    name: str
    description: str
    tool_name: str
    tool_description: str | None = None
    tool_properties: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"uri": self.uri, "name": self.name, "description": self.description, "mimeType": "application/json"}

    def to_tool(self) -> McpTool:
        return McpTool(
            name=self.tool_name,
            path=self.path,
            description=self.tool_description or self.description,
            input_schema=_schema(self.tool_properties, []),
            method="GET",
            read_only=True,
        )


LIMIT_PROPERTY = {"type": "integer", "minimum": 1, "maximum": 500, "default": 50}

ACTION_TOOLS: tuple[McpTool, ...] = (
    McpTool(
        name="get_clock_status",
        path="/devices",
        description=(
            "Clock synchronisation across the whole network in one call: every device's role, leader, lock state, "
            "preferred-leader flag and frequency offset, grouped by clock domain (each Dante Domain Manager domain "
            "and the unmanaged devices form separate domains with their own leader)."
        ),
        input_schema=_schema({}, []),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="get_device",
        path="/devices/{device}",
        description=(
            "Details for one device. Pick sections to keep the answer small: summary (identity, address, rate, "
            "latency, clock, subscription problems), channels (rx and tx channel names by number), subscriptions "
            "(only receive channels that have a route, with its status; a receive channel absent here is "
            "unsubscribed), network (interfaces and redundancy), availability (which operations are "
            "writable and why not), flows (transmit and receive flows), or full for the raw record."
        ),
        input_schema=_schema(
            {
                "device": _device_property(),
                "sections": {
                    "type": "array",
                    "items": {"type": "string", "enum": list(DEVICE_SECTIONS)},
                    "default": ["summary"],
                },
            },
            ["device"],
        ),
        method="GET",
        read_only=True,
        defaults={"sections": ["summary"]},
    ),
    McpTool(
        name="apply_preset",
        path="/presets/load",
        description=(
            "Apply a saved Dante Controller preset to matching devices. Preview it first with preview_preset and pass "
            "its digest. Writes routing, audio settings and network configuration; network changes may require a reboot."
        ),
        input_schema=_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "digest": {"type": "string", "description": "Digest returned by preview_preset."},
                "excluded": {"type": "array", "items": {"type": "string"}, "description": "Inventory IDs to skip."},
                "targets": {"type": "array", "items": {"type": "string"}, "description": "Inventory IDs to apply to."},
                "xml": {"type": "string", "description": "Preset XML as exported by Dante Controller or netaudio."},
            },
            ["confirmed", "digest", "xml"],
        ),
        destructive=True,
        requires_confirmation=True,
    ),
    McpTool(
        name="create_transmit_flow",
        path="/transmit-flows/create",
        description="Create a multicast transmit flow from a specification produced by plan_transmit_flow.",
        input_schema=_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": _device_property(),
                "specification": {
                    "type": "object",
                    "description": "Transmit flow specification (see plan_transmit_flow).",
                },
            },
            ["confirmed", "device", "specification"],
        ),
        destructive=True,
        requires_confirmation=True,
    ),
    McpTool(
        name="delete_transmit_flow",
        path="/transmit-flows/delete",
        description="Delete a multicast transmit flow by its global flow identifier.",
        input_schema=_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": _device_property(),
                "flow_id": {"type": "integer", "minimum": 1, "maximum": 32},
            },
            ["confirmed", "device", "flow_id"],
        ),
        destructive=True,
        requires_confirmation=True,
    ),
    McpTool(
        name="identify",
        path="/identify",
        description="Flash the device's identify LED so someone can find it physically.",
        input_schema=_schema({"device": _device_property()}, ["device"]),
    ),
    McpTool(
        name="lock_device",
        path="/lock",
        description="Lock a device against configuration changes with a four digit PIN. Requires a device lock key on the server.",
        input_schema=_schema(
            {"device": _device_property(), "pin": {"type": "string", "pattern": "^[0-9]{4}$"}},
            ["device", "pin"],
        ),
        destructive=True,
    ),
    McpTool(
        name="plan_transmit_flow",
        path="/transmit-flows/plan",
        description=(
            "Check whether a multicast transmit flow can be created on a device and how, without changing anything. "
            "The specification needs media_mode native_dante, flow_type multicast and channel_slots, a list of "
            "{slot, transmitter_channel} objects."
        ),
        input_schema=_schema(
            {"device": _device_property(), "specification": {"type": "object"}},
            ["device", "specification"],
        ),
        read_only=True,
    ),
    McpTool(
        name="preview_preset",
        path="/presets/preview",
        description="Show what applying a preset would change on the current network, per device, without changing anything.",
        input_schema=_schema(
            {
                "devices": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Inventory IDs to limit the preview to.",
                },
                "xml": {"type": "string", "description": "Preset XML as exported by Dante Controller or netaudio."},
            },
            ["xml"],
        ),
        read_only=True,
    ),
    McpTool(
        name="reboot",
        path="/reboot",
        description="Reboot a device. Audio stops while it restarts and it may come back with a different address.",
        input_schema=_schema(
            {"confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION}, "device": _device_property()},
            ["confirmed", "device"],
        ),
        destructive=True,
        requires_confirmation=True,
    ),
    McpTool(
        name="refresh",
        path="/refresh",
        description="Re-read one device, or every device when no device is given.",
        input_schema=_schema({"device": _device_property()}, []),
        read_only=True,
    ),
    McpTool(
        name="rename_channel",
        path="/rename-channel",
        description="Set a transmit or receive channel's label. An empty name resets it to the device default.",
        input_schema=_schema(
            {
                "channel_number": {"type": "integer", "minimum": 1},
                "channel_type": {"type": "string", "enum": ["rx", "tx"]},
                "device": _device_property(),
                "name": {"type": "string", "maxLength": 31},
            },
            ["channel_number", "channel_type", "device", "name"],
        ),
    ),
    McpTool(
        name="rename_device",
        path="/rename-device",
        description="Set a device's name. Other devices route to it by name, so renaming can break their subscriptions.",
        input_schema=_schema(
            {"device": _device_property(), "name": {"type": "string", "maxLength": 31}},
            ["device", "name"],
        ),
        destructive=True,
    ),
    McpTool(
        name="save_preset",
        path="/presets/save",
        description="Export the current configuration of devices as Dante Controller preset XML.",
        input_schema=_schema(
            {
                "devices": {"type": "array", "items": {"type": "string"}, "description": "Inventory IDs to include."},
                "name": {"type": "string"},
                "sections": {"type": "array", "items": {"type": "string", "enum": ["audio", "network", "routing"]}},
            },
            ["devices", "name"],
        ),
        read_only=True,
    ),
    McpTool(
        name="set_aes67",
        path="/set-aes67",
        description="Enable or disable AES67 mode on a device. Takes effect after a reboot.",
        input_schema=_schema({"device": _device_property(), "enabled": {"type": "boolean"}}, ["device", "enabled"]),
    ),
    McpTool(
        name="set_encoding",
        path="/set-encoding",
        description="Set a device's audio encoding in bits: 16, 24 or 32. Only values in the device's supported_encodings are accepted.",
        input_schema=_schema(
            {"device": _device_property(), "encoding": {"type": "integer", "enum": [16, 24, 32]}},
            ["device", "encoding"],
        ),
    ),
    McpTool(
        name="set_gain",
        path="/set-gain",
        description="Set the analog gain level of an AVIO input or output channel. Levels 1 through 5 map to the labels in the device's gain_level_choices.",
        input_schema=_schema(
            {
                "channel_number": {"type": "integer", "minimum": 1},
                "device": _device_property(),
                "device_type": {"type": "string", "enum": ["input", "output"]},
                "gain_level": {"type": "integer", "minimum": 1, "maximum": 5},
            },
            ["channel_number", "device", "device_type", "gain_level"],
        ),
    ),
    McpTool(
        name="set_interface",
        path="/interface",
        description="Configure a network interface for DHCP or a static address. Takes effect after a reboot; a wrong static address can make the device unreachable.",
        input_schema=_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": _device_property(),
                "dns": {"type": "string"},
                "gateway": {"type": "string"},
                "interface": {"type": "string", "enum": ["primary", "secondary"]},
                "ip": {"type": "string"},
                "mode": {"type": "string", "enum": ["dhcp", "static"]},
                "netmask": {"type": "string"},
            },
            ["confirmed", "device", "interface", "mode"],
        ),
        destructive=True,
        requires_confirmation=True,
    ),
    McpTool(
        name="set_latency",
        path="/set-latency",
        description="Set a device's receive latency in milliseconds. Use one of the device's standard_latency_choices_ms.",
        input_schema=_schema(
            {"device": _device_property(), "latency": {"type": "number", "exclusiveMinimum": 0}},
            ["device", "latency"],
        ),
    ),
    McpTool(
        name="set_preferred_leader",
        path="/set-preferred-leader",
        description="Mark a device as preferred clock leader or clear that preference.",
        input_schema=_schema({"device": _device_property(), "preferred": {"type": "boolean"}}, ["device", "preferred"]),
    ),
    McpTool(
        name="set_redundancy",
        path="/redundancy",
        description="Set a device's Dante redundancy mode. Only modes listed in the device's network_redundancy.available_modes are accepted; takes effect after a reboot.",
        input_schema=_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": _device_property(),
                "mode": {"type": "string", "enum": ["redundant", "split_redundant", "switched"]},
            },
            ["confirmed", "device", "mode"],
        ),
        destructive=True,
        requires_confirmation=True,
    ),
    McpTool(
        name="set_sample_rate",
        path="/set-sample-rate",
        description=(
            "Set a device's sample rate in hertz. Only values in the device's supported_sample_rates_hz are accepted. "
            "Devices subscribed to each other must share a sample rate, so changing one may break routes on others."
        ),
        input_schema=_schema(
            {
                "device": _device_property(),
                "sample_rate": {"type": "integer", "enum": [44100, 48000, 88200, 96000, 176400, 192000]},
            },
            ["device", "sample_rate"],
        ),
        destructive=True,
    ),
    McpTool(
        name="set_subscriptions",
        path="/subscriptions/apply",
        description=(
            "Set or clear any number of routes across any number of devices in one call. The server groups routes by "
            "receiving device and sends them in the largest batches each device accepts (16 per request for direct "
            "Dante control, 32 for newer firmware and managed devices), running devices in parallel. Give tx_device "
            "and tx_channel to route audio, or omit both to clear the receive channel. Prefer this over subscribe and "
            "unsubscribe for more than one route. The reply lists every route with ok true or an error."
        ),
        input_schema=_schema(
            {
                "routes": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {
                            "rx_channel": {"type": "integer", "minimum": 1},
                            "rx_device": {"type": "string"},
                            "tx_channel": {"type": "string"},
                            "tx_device": {"type": "string"},
                        },
                        "required": ["rx_channel", "rx_device"],
                        "additionalProperties": False,
                    },
                }
            },
            ["routes"],
        ),
        destructive=True,
    ),
    McpTool(
        name="subscribe",
        path="/subscribe",
        description="Route a transmit channel from one device to a receive channel on another. Channels are named as shown in the devices resource.",
        input_schema=_schema(
            {
                "rx_channel": {"type": "integer", "minimum": 1, "description": "Receive channel number on rx_device."},
                "rx_device": {"type": "string", "description": "Receiving device name."},
                "tx_channel": {"type": "string", "description": "Transmit channel name on tx_device."},
                "tx_device": {"type": "string", "description": "Transmitting device name."},
            },
            ["rx_channel", "rx_device", "tx_channel", "tx_device"],
        ),
    ),
    McpTool(
        name="unlock_device",
        path="/unlock",
        description="Unlock a locked device using its PIN.",
        input_schema=_schema(
            {"device": _device_property(), "pin": {"type": "string", "pattern": "^[0-9]{4}$"}},
            ["device", "pin"],
        ),
    ),
    McpTool(
        name="unsubscribe",
        path="/unsubscribe",
        description="Remove the route feeding a receive channel.",
        input_schema=_schema(
            {"rx_channel": {"type": "integer", "minimum": 1}, "rx_device": {"type": "string"}},
            ["rx_channel", "rx_device"],
        ),
        destructive=True,
    ),
)

RESOURCES: tuple[McpResource, ...] = (
    McpResource(
        uri="netaudio://ddm/status",
        path="/ddm/status",
        name="Dante Domain Manager status",
        description="Connection state of configured Dante Domain Manager servers and their domains.",
        tool_name="get_ddm_status",
    ),
    McpResource(
        uri="netaudio://devices",
        path="/devices",
        name="Devices",
        description=(
            "Every known Dante device keyed by server name: online state, channels, subscriptions with their status, "
            "sample rate, latency, encoding, clock role, network interfaces, redundancy, and operation_availability "
            "which says which settings can currently be changed and why not."
        ),
        tool_name="list_devices",
        tool_description=(
            "Every known device as a compact summary keyed by server name: name, model, address, online state, "
            "management state, sample rate, latency, channel counts, subscription count and any failing "
            "subscriptions. Use get_device for channels, routes, network and operation availability."
        ),
    ),
    McpResource(
        uri="netaudio://event-journal",
        path="/event-journal",
        name="Event journal",
        description="Recent monitoring events: latency, late packets, issue lifecycle and configuration changes.",
        tool_name="get_event_journal",
        tool_description=(
            "Recent monitoring events, newest first: latency, late packets, issue lifecycle and configuration "
            "changes. Filter by device or kind and page with limit."
        ),
        tool_properties={
            "device": _device_property(),
            "kind": {
                "type": "string",
                "description": "Only events of this kind, for example receiver_flow_latency_high.",
            },
            "limit": LIMIT_PROPERTY,
        },
    ),
    McpResource(
        uri="netaudio://external-flows",
        path="/external-flows",
        name="External AES67 streams",
        description="AES67 and RTP streams announced on the network by SAP.",
        tool_name="get_external_flows",
    ),
    McpResource(
        uri="netaudio://issues",
        path="/issues",
        name="Issues",
        description="Open and recently resolved problems detected on the network, with suggested remediation.",
        tool_name="get_issues",
        tool_description=(
            "Problems detected on the network, most recently seen first. Defaults to open issues; set state to "
            "resolved or all for history. Filter by device and page with limit."
        ),
        tool_properties={
            "device": _device_property(),
            "limit": LIMIT_PROPERTY,
            "state": {"type": "string", "enum": ["all", "open", "resolved"], "default": "open"},
        },
    ),
    McpResource(
        uri="netaudio://metering",
        path="/metering/cache",
        name="Signal levels",
        description="Latest signal level per channel for devices that are being metered.",
        tool_name="get_signal_levels",
        tool_description="Latest signal level per tx and rx channel for devices that are being metered, optionally for one device.",
        tool_properties={"device": _device_property()},
    ),
    McpResource(
        uri="netaudio://server",
        path="/server-info",
        name="Server",
        description="This netaudio server's host, version and start time.",
        tool_name="get_server_info",
    ),
)

TOOLS: tuple[McpTool, ...] = tuple(
    sorted((*ACTION_TOOLS, *(resource.to_tool() for resource in RESOURCES)), key=lambda tool: tool.name)
)

DEVICE_RESOURCE_TEMPLATE = {
    "uriTemplate": "netaudio://devices/{name}",
    "name": "Device",
    "description": "One device by name, with the same fields as the devices resource.",
    "mimeType": "application/json",
}

TOOL_VIEWS = {
    "get_clock_status": clock_status_view,
    "get_device": lambda payload, arguments: device_view(payload, arguments["sections"]),
    "get_event_journal": events_view,
    "get_issues": issues_view,
    "get_signal_levels": signal_levels_view,
    "list_devices": lambda payload, arguments: {key: device_summary(value) for key, value in payload.items()},
}
TOOLS_BY_NAME = {tool.name: tool for tool in TOOLS}
RESOURCES_BY_URI = {resource.uri: resource for resource in RESOURCES}


class CapturedResponse:
    def __init__(self, peername=None):
        self.chunks = bytearray()
        self.peername = peername

    def get_extra_info(self, name):
        return self.peername if name == "peername" else None

    def write(self, payload):
        self.chunks.extend(payload)

    async def drain(self):
        pass

    def close(self):
        pass

    async def wait_closed(self):
        pass

    def result(self) -> tuple[int, Any]:
        raw = bytes(self.chunks)
        header, _, body = raw.partition(b"\r\n\r\n")
        try:
            status = int(header.split(b" ")[1])
        except (IndexError, ValueError):
            status = 500
        if not body:
            return status, None
        try:
            return status, json.loads(body)
        except ValueError:
            return status, body.decode(errors="replace")


class McpError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


class DaemonMcpHandlers:
    mcp_token: str | None = None

    def mcp_server_info(self) -> dict:
        return {"path": MCP_PATH, "protocol_version": MCP_PROTOCOL_VERSION, "transport": "streamable-http"}

    def mcp_grant(self, headers) -> Grant | None:
        header = (headers or {}).get("authorization")
        if authorization_matches(header, self.mcp_token):
            return Grant(
                client_id="local-token",
                client_name="netaudio daemon mcp-token",
                scope=SCOPE_WRITE,
                expires_at=float("inf"),
            )
        if not isinstance(header, str):
            return None
        scheme, _, token = header.strip().partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            return None
        store: OAuthStore | None = getattr(self, "oauth_store", None)
        return store.grant_for(token.strip()) if store else None

    async def _handle_mcp(self, method: str, body, writer, headers) -> None:
        headers = headers or {}
        grant = self.mcp_grant(headers)
        if grant is None:
            self._write_mcp_raw(
                writer,
                401,
                {"error": "a bearer token is required; connect through OAuth or run `netaudio daemon mcp-token`"},
                resource_metadata=self.oauth_base_url(headers) + "/.well-known/oauth-protected-resource",
            )
            return
        self._mcp_current_grant = grant
        if method == "DELETE":
            self._write_mcp_raw(writer, 204, None)
            return
        if method != "POST":
            self._write_mcp_raw(writer, 405, {"error": "MCP requests are POST only"})
            return
        try:
            message = json.loads(body) if body else None
        except ValueError as exception:
            self._write_mcp_raw(writer, 200, _error_response(None, JSON_RPC_PARSE_ERROR, f"invalid json: {exception}"))
            return
        if isinstance(message, list):
            responses = [await self._mcp_message(item, writer) for item in message]
            responses = [response for response in responses if response is not None]
            self._write_mcp_raw(writer, 200 if responses else 202, responses or None)
            return
        response = await self._mcp_message(message, writer)
        self._write_mcp_raw(writer, 200 if response is not None else 202, response)

    async def _mcp_message(self, message, writer) -> dict | None:
        if (
            not isinstance(message, dict)
            or message.get("jsonrpc") != "2.0"
            or not isinstance(message.get("method"), str)
        ):
            return _error_response(
                message.get("id") if isinstance(message, dict) else None, JSON_RPC_INVALID_REQUEST, "invalid request"
            )
        method = message["method"]
        params = message.get("params") or {}
        request_id = message.get("id")
        if not isinstance(params, dict):
            return _error_response(request_id, JSON_RPC_INVALID_PARAMS, "params must be an object")
        if request_id is None:
            return None
        try:
            result = await self._mcp_call(method, params, writer)
        except McpError as exception:
            return _error_response(request_id, exception.code, str(exception))
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    async def _mcp_call(self, method: str, params: dict, writer) -> dict:
        if method == "initialize":
            return {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "prompts": {"listChanged": False},
                    "resources": {"listChanged": False},
                    "tools": {"listChanged": False},
                },
                "serverInfo": {"name": "netaudio", "version": str(self.server_info.get("version", "unknown"))},
                "instructions": (
                    "netaudio controls Dante audio devices. Call list_devices first (or read the netaudio://devices "
                    "resource): it lists every device with its channels, routes and operation_availability. Route "
                    "audio with subscribe using the "
                    "receiving device's channel number and the transmitting device's channel name. Tools that carry a "
                    "confirmed flag change network state or interrupt audio; ask the user before setting it."
                ),
            }
        if method == "ping":
            return {}
        if method == "tools/list":
            return {"tools": [tool.to_dict() for tool in TOOLS]}
        if method == "tools/call":
            return await self._mcp_tool_call(params, writer)
        if method == "resources/list":
            return {"resources": [resource.to_dict() for resource in RESOURCES]}
        if method == "resources/templates/list":
            return {"resourceTemplates": [DEVICE_RESOURCE_TEMPLATE]}
        if method == "resources/read":
            return await self._mcp_resource_read(params, writer)
        if method == "prompts/list":
            return {"prompts": []}
        raise McpError(JSON_RPC_METHOD_NOT_FOUND, f"unknown method {method}")

    async def _mcp_tool_call(self, params: dict, writer) -> dict:
        name = params.get("name")
        tool = TOOLS_BY_NAME.get(name) if isinstance(name, str) else None
        if tool is None:
            raise McpError(JSON_RPC_INVALID_PARAMS, f"unknown tool {name!r}")
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise McpError(JSON_RPC_INVALID_PARAMS, "arguments must be an object")
        unexpected = sorted(set(arguments) - set(tool.input_schema["properties"]))
        if unexpected:
            raise McpError(JSON_RPC_INVALID_PARAMS, f"unexpected arguments: {', '.join(unexpected)}")
        missing = [key for key in tool.input_schema["required"] if key not in arguments]
        if missing:
            raise McpError(JSON_RPC_INVALID_PARAMS, f"missing arguments: {', '.join(missing)}")
        grant: Grant | None = getattr(self, "_mcp_current_grant", None)
        if grant is not None and not grant.can_write and not tool.read_only:
            return _tool_result(
                {
                    "error": f"{grant.client_name} was granted read-only access; {tool.name} needs the {SCOPE_WRITE} scope."
                },
                is_error=True,
            )
        if tool.requires_confirmation and arguments.get("confirmed") is not True:
            return _tool_result(
                {
                    "error": f"{tool.name} changes device state; confirm with the user and call it again with confirmed set to true."
                },
                is_error=True,
            )
        request = {**tool.defaults, **arguments}
        logger.info(
            f"MCP {grant.client_name if grant else 'unknown client'} called {tool.name} "
            f"{json.dumps({key: value for key, value in arguments.items() if key not in {'pin', 'xml'}}, default=str)}"
        )
        captured = CapturedResponse(writer.get_extra_info("peername"))
        try:
            if tool.method == "GET":
                path = tool.path.format(**{key: quote(str(value), safe="") for key, value in request.items()})
                await self._dispatch("GET", path, None, captured, {"accept": "application/json"})
            else:
                await self.post_handlers[tool.path](captured, request)
        except TimeoutError:
            return _tool_result({"error": "device did not respond"}, is_error=True)
        except Exception as exception:
            logger.exception(f"MCP tool {tool.name} failed")
            return _tool_result({"error": str(exception)}, is_error=True)
        status, payload = captured.result()
        if payload is None:
            payload = {"status": status}
        elif status < 400 and tool.name in TOOL_VIEWS and isinstance(payload, dict):
            payload = TOOL_VIEWS[tool.name](payload, request)
        return _tool_result(compact(payload), is_error=status >= 400)

    async def _mcp_resource_read(self, params: dict, writer) -> dict:
        uri = params.get("uri")
        if not isinstance(uri, str):
            raise McpError(JSON_RPC_INVALID_PARAMS, "uri is required")
        resource = RESOURCES_BY_URI.get(uri)
        if resource is not None:
            path = resource.path
        elif uri.startswith("netaudio://devices/") and len(uri) > len("netaudio://devices/"):
            path = "/devices/" + uri[len("netaudio://devices/") :]
        else:
            raise McpError(JSON_RPC_INVALID_PARAMS, f"unknown resource {uri}")
        captured = CapturedResponse(writer.get_extra_info("peername"))
        try:
            await self._dispatch("GET", path, None, captured, {"accept": "application/json"})
        except TimeoutError:
            raise McpError(JSON_RPC_INVALID_PARAMS, "device did not respond") from None
        except Exception as exception:
            logger.exception(f"MCP resource {uri} failed")
            raise McpError(JSON_RPC_INVALID_PARAMS, str(exception)) from exception
        status, payload = captured.result()
        if status >= 400:
            message = payload.get("error") if isinstance(payload, dict) else str(payload)
            raise McpError(JSON_RPC_INVALID_PARAMS, message or f"resource returned HTTP {status}")
        return {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(payload, default=str)}]}

    def _write_mcp_raw(self, writer, status: int, payload, resource_metadata: str | None = None) -> None:
        body = b"" if payload is None else json.dumps(payload, default=str).encode()
        status_text = {
            200: "OK",
            202: "Accepted",
            204: "No Content",
            401: "Unauthorized",
            405: "Method Not Allowed",
        }.get(status, "Error")
        head = f"HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nCache-Control: no-store\r\n"
        if status == 401:
            challenge = 'Bearer realm="netaudio"'
            if resource_metadata:
                challenge += f', resource_metadata="{resource_metadata}"'
            head += f"WWW-Authenticate: {challenge}\r\n"
        writer.write(head.encode() + b"\r\n" + body)


def _error_response(request_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _tool_result(payload, *, is_error: bool) -> dict:
    result = {"content": [{"type": "text", "text": json.dumps(payload, default=str, indent=1)}], "isError": is_error}
    if isinstance(payload, dict):
        result["structuredContent"] = payload
    return result
