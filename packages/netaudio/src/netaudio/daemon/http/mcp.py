from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from netaudio.daemon.http.mcp_host_audio import HOST_AUDIO_TOOL_NAMES, HOST_AUDIO_TOOL_SPECS, McpHostAudioTools
from netaudio.daemon.http.mcp_planned import PLANNED_BY_NAME, PLANNED_OPERATIONS, PlannedOperation
from netaudio.daemon.http.mcp_presets import PRESET_TOOLS, McpPresetTools
from netaudio.daemon.http.mcp_preview import (
    NEXT_STEP,
    READBACK_TOOLS,
    ROUTE_TOOLS,
    write_outcome,
    write_preview,
    write_settled,
)
from netaudio.daemon.http.mcp_protocol import (
    complete_result,
    discover_result,
    is_modern_request,
    validate_modern_request,
)
from netaudio.daemon.http.host_audio import HISTORY_FLUSH_SECONDS
from netaudio.daemon.http.mcp_signal_history import PERIOD_PROPERTIES, signal_history_view, signal_period
from netaudio.daemon.http.mcp_schema import (
    CHANNEL_ARGUMENT,
    CONFIRMATION_DESCRIPTION,
    close_matches,
    device_property,
    object_schema,
    schema_errors,
    with_confirmation,
)
from netaudio.daemon.http.mcp_subscriptions import McpSubscriptions
from netaudio.daemon.http.mcp_views import (
    DEVICE_SECTIONS,
    _subscription_list,
    channel_label,
    clock_status_view,
    compact,
    compact_channel_levels,
    control_apply_view,
    control_plan_view,
    controls_view,
    ddm_domains_view,
    ddm_inventory_known,
    device_addresses,
    device_list_view,
    device_view,
    diagnostics_view,
    events_view,
    find_channels_view,
    flow_result_view,
    free_flow_ids,
    hexadecimal_byte_fields,
    issue_groups_view,
    management_state,
    missing_devices,
    name_meter_channels,
    network_levels_view,
    network_overview_view,
    last_seen_text,
    off_devices,
    receive_channels_text,
    routes_by_source,
    is_off,
    record_fields_view,
    renamed_devices,
    routing_view,
    signal_levels_view,
    transmit_flow_view,
    transmit_flows_view,
    utc_now,
    wireless_link,
    wireless_view,
    with_channel_signal,
)
from netaudio.daemon.http.request_scope import REQUEST_INVENTORY
from netaudio.daemon.mcp_oauth import SCOPE_WRITE, Grant, OAuthStore, grant_for_authorization
from netaudio.dante.events import EventType
from netaudio.dante.metering import detailed_metering_targets

logger = logging.getLogger("netaudio.mcp")
logger.setLevel(logging.INFO)

MCP_PATH = "/mcp"
MCP_PROTOCOL_VERSION = "2025-06-18"
JSON_RPC_INVALID_REQUEST = -32600
JSON_RPC_METHOD_NOT_FOUND = -32601
JSON_RPC_INVALID_PARAMS = -32602
JSON_RPC_PARSE_ERROR = -32700
MODERN_METHODS = frozenset(
    {"tools/list", "tools/call", "resources/list", "resources/templates/list", "resources/read", "prompts/list"}
)
MCP_INSTRUCTIONS = (
    "Use get_network_overview for health (devices, issues, missing devices, clock leaders and stream latency), "
    "get_routing for the patch, find_channels to locate a channel by its label, and get_device for focused "
    "details. Use discover_tools to find other operations and their exact schemas, then invoke_tool to run them: "
    "get_device_diagnostics for per-flow latency and late packets, get_signal_levels for whether audio is "
    "present (with over_last_seconds or listen_seconds for sound that comes and goes, such as speech, since one "
    "sample can miss it), trace_signal to follow audio from any point through Shure wireless, Dante, this computer's sound "
    "cards, JACK and PulseAudio with levels at each step, get_host_audio for this computer's JACK and PulseAudio, "
    "get_wireless_devices for Shure mics, inspect_device_controls for a device's own panel (AVIO "
    "Bluetooth pairing and name, analog levels, Dante AV video), and save_preset/apply_preset to keep and restore "
    "setups by name. Reads are compact by default; detail=debug exposes raw evidence. Channels accept unique labels or "
    "numbers. Call a write without confirmed to see exactly what it would change; nothing changes until the "
    "user agrees and you call it again with confirmed=true, and the result reads the device back. Use "
    "apply_to_devices to make one change on several devices. A device that is off the network is not a problem, "
    "even when it is enrolled in Dante Domain Manager: it shows only when it was last seen, routes waiting for it "
    "are not failures, and neither is worth mentioning unless the user asks about that device or about what is "
    "off. forget_device removes devices that no longer matter. Operations with status planned are designed but not built; calling one explains what blocks it."
)


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
    confirmable: bool = True
    defaults: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.read_only or not self.confirmable:
            return
        object.__setattr__(self, "input_schema", with_confirmation(self.input_schema))
        object.__setattr__(self, "requires_confirmation", True)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
            "annotations": {
                "destructiveHint": self.destructive,
                "readOnlyHint": self.read_only,
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
            input_schema=object_schema(self.tool_properties, []),
            method="GET",
            read_only=True,
        )


LIMIT_PROPERTY = {"type": "integer", "minimum": 1, "maximum": 500, "default": 50}
EVENT_LIMIT_PROPERTY = {"type": "integer", "minimum": 1, "maximum": 500, "default": 20}

ACTION_TOOLS: tuple[McpTool, ...] = (
    McpTool(
        name="apply_to_devices",
        path="",
        description=(
            "Run one write operation on every device matching a filter, such as locking all unmanaged devices "
            "with one PIN. Call without token to preview which devices match and why others are skipped; then "
            "call again with the preview's token and confirmed=true. Each device reports its own result."
        ),
        input_schema=object_schema(
            {
                "operation": {
                    "type": "string",
                    "description": "A write operation that takes a device argument, such as lock_device or set_latency.",
                },
                "where": {
                    "type": "object",
                    "description": "Devices to include; every given condition must match.",
                    "properties": {
                        "devices": {"type": "array", "items": {"type": "string"}, "minItems": 1},
                        "management": {"type": "string", "enum": ["managed", "unmanaged"]},
                        "model": {
                            "type": "string",
                            "description": "Text found in the model or product name, ignoring case, such as avio or AD4D.",
                        },
                        "manufacturer": {"type": "string"},
                        "online": {"type": "boolean"},
                        "locked": {"type": "boolean"},
                    },
                    "additionalProperties": False,
                },
                "arguments": {"type": "object", "description": "The operation's arguments other than device."},
                "token": {"type": "string", "description": "Token returned by the preview; required to apply."},
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
            },
            ["operation", "where"],
        ),
        destructive=True,
        confirmable=False,
    ),
    McpTool(
        name="clear_configuration",
        path="/clear-configuration",
        description=(
            "Clear a device's stored configuration: everything, or everything except its network settings. "
            "Some devices apply the clear only when they reboot, so set reboot to apply it now."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "mode": {"type": "string", "enum": ["all", "keep_network"]},
                "reboot": {"type": "boolean", "default": False},
            },
            ["device", "mode"],
        ),
        destructive=True,
    ),
    McpTool(
        name="factory_reset",
        path="/factory-reset",
        description="Return a device to factory settings. It reboots and loses its name, routes and network settings.",
        input_schema=object_schema({"device": device_property()}, ["device"]),
        destructive=True,
    ),
    McpTool(
        name="get_wireless_devices",
        path="/shure/devices",
        description=(
            "Shure wireless receivers and transmitters the server tracks: each channel's name, whether a transmitter "
            "is being received, frequency, level, and for receivers with one matching Dante device, the Dante "
            "output it appears on and what that feeds. Settings use the names and values set_wireless_value "
            "takes; AD4D levels are dBFS, while the P10T reports its input meter in its own units."
        ),
        input_schema=object_schema({}, []),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="discover_tools",
        path="",
        description="Find NetAudio operations by name or purpose. Use include_schema for exact arguments before invoking one.",
        input_schema=object_schema(
            {
                "query": {
                    "type": "string",
                    "description": "Words in an operation name or description, a category name, or an exact operation name; omit to list every operation by category.",
                },
                "include_schema": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            [],
        ),
        read_only=True,
    ),
    McpTool(
        name="invoke_tool",
        path="",
        description="Run an operation found with discover_tools. The selected operation's write scope and confirmation rules apply.",
        input_schema=object_schema(
            {
                "name": {"type": "string", "description": "Exact operation name from discover_tools."},
                "arguments": {"type": "object", "description": "Arguments matching that operation's input schema."},
            },
            ["name"],
        ),
        destructive=True,
        confirmable=False,
    ),
    McpTool(
        name="inspect_device_controls",
        path="/device-controls",
        description=(
            "A device's own control panel, such as AVIO Bluetooth pairing and name or Dante AV video settings: "
            "current values and what can be changed. Devices without a panel say so."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "detail": {"type": "string", "enum": ["compact", "debug"], "default": "compact"},
            },
            ["device"],
        ),
        defaults={"action": "inspect"},
        read_only=True,
    ),
    McpTool(
        name="plan_device_control",
        path="/device-controls",
        description=(
            "Check a device panel change without applying it: an AVIO Bluetooth name, discoverability or clearing "
            "remembered phones, AVIO analog levels (category analog_level), or Dante AV video settings. "
            "inspect_device_controls lists each setting and what it accepts."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "category": {
                    "type": "string",
                    "description": (
                        "Setting from inspect_device_controls, such as bluetooth_identification, "
                        "bluetooth_discovery, bluetooth_pairing or analog_level."
                    ),
                },
                "requested": {
                    "description": (
                        'New value, for example "Studio BT" or "Dante device name" for the Bluetooth name, '
                        '"discoverable" or "not discoverable", "clear", or {"channel": 1, "level": 3}.'
                    )
                },
            },
            ["device", "category", "requested"],
        ),
        defaults={"action": "plan"},
        read_only=True,
    ),
    McpTool(
        name="apply_device_control",
        path="/device-controls",
        description=(
            "Apply a device panel change: an AVIO Bluetooth name, discoverability or clearing remembered phones, "
            "AVIO analog levels, or Dante AV video settings. Takes the same arguments as plan_device_control; "
            "clearing remembered phones also needs confirm_clear=true."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "category": {"type": "string", "description": "Setting from inspect_device_controls."},
                "requested": {"description": "New value, in the same forms plan_device_control accepts."},
                "confirm_clear": {
                    "type": "boolean",
                    "default": False,
                    "description": "Required to clear remembered Bluetooth phones, which cannot be undone.",
                },
            },
            ["device", "category", "requested"],
        ),
        defaults={"action": "apply"},
    ),
    McpTool(
        name="configure_clock",
        path="/set-clock-configuration",
        description="Set advertised clock controls with readback: source, preferred leader, subdomain, or delay requests. Inspect clock status first.",
        input_schema=object_schema(
            {
                "device": device_property(),
                "changes": {
                    "type": "object",
                    "minProperties": 1,
                    "description": "Clock control fields and values supported by this device.",
                },
            },
            ["device", "changes"],
        ),
        destructive=True,
    ),
    McpTool(
        name="configure_flow_performance",
        path="/set-receive-flow-performance",
        description="Set receive, transmit, or unicast flow latency and frames per packet on a device.",
        input_schema=object_schema(
            {
                "device": device_property(),
                "kind": {"type": "string", "enum": ["receive", "transmit", "unicast"]},
                "latency_microseconds": {"type": "integer", "minimum": 1},
                "frames_per_packet": {"type": "integer", "minimum": 1},
            },
            ["device", "kind", "latency_microseconds", "frames_per_packet"],
        ),
        destructive=True,
    ),
    McpTool(
        name="set_receive_flow_default_slots",
        path="/set-receive-flow-default-slots",
        description="Set the default receive flow slot count supported by the device.",
        input_schema=object_schema(
            {"device": device_property(), "default_slots": {"type": "integer", "minimum": 1}},
            ["device", "default_slots"],
        ),
        destructive=True,
    ),
    McpTool(
        name="store_current_configuration",
        path="/store-current-configuration",
        description="Store the device's current settings in its persistent configuration.",
        input_schema=object_schema({"device": device_property()}, ["device"]),
        destructive=True,
    ),
    McpTool(
        name="set_sample_rate_pullup",
        path="/set-sample-rate-pullup",
        description="Set sample rate pull-up or pull-down. Use a value advertised by the device.",
        input_schema=object_schema(
            {
                "device": device_property(),
                "value": {"type": "string", "enum": ["none", "+4.1667%", "+0.1%", "-0.1%", "-4.0%"]},
            },
            ["device", "value"],
        ),
        destructive=True,
    ),
    McpTool(
        name="set_aes67_multicast_prefix",
        path="/set-aes67-multicast-prefix",
        description="Set the advertised AES67 multicast IPv4 prefix on a supporting device.",
        input_schema=object_schema({"device": device_property(), "prefix": {"type": "string"}}, ["device", "prefix"]),
        destructive=True,
    ),
    McpTool(
        name="subscribe_external_flow",
        path="/external-flows/subscribe",
        description="Subscribe a receiver to a fresh SAP-announced AES67 flow. Use get_external_flows for its source, session, and content digest.",
        input_schema=object_schema(
            {
                "rx_device": device_property(),
                "source_ipv4": {"type": "string"},
                "session_id": {"type": "string", "pattern": "^[0-9]+$"},
                "content_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "receiver_channel_ids": {"type": "array", "items": {"type": "integer", "minimum": 1}},
                "flow_slot_assignments": {"type": "array", "items": {"type": "integer", "minimum": 0}},
            },
            ["rx_device", "source_ipv4", "session_id", "content_sha256"],
        ),
        destructive=True,
    ),
    McpTool(
        name="start_metering",
        path="/metering/start",
        description="Start detailed metering for one device for this MCP client; then read get_signal_levels.",
        input_schema=object_schema({"device": device_property()}, ["device"]),
    ),
    McpTool(
        name="stop_metering",
        path="/metering/stop",
        description="Stop detailed metering started by this MCP client for one device.",
        input_schema=object_schema({"device": device_property()}, ["device"]),
    ),
    McpTool(
        name="get_transmit_flows",
        path="/transmit-flows/{device}",
        description=(
            "One device's transmit flows: each flow's ID, unicast or multicast, where it goes and which channels it "
            "carries, plus the next free flow ID. Debug returns the raw inventory."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "detail": {"type": "string", "enum": ["compact", "debug"], "default": "compact"},
            },
            ["device"],
        ),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="refresh_ddm",
        path="/ddm/refresh",
        description="Refresh a DDM context or the managed inventory without changing devices.",
        input_schema=object_schema({"context": {"type": "string"}}, []),
        read_only=True,
    ),
    McpTool(
        name="login_ddm",
        path="/ddm/login",
        description=(
            "Log in to a DDM server and save the login under a profile name. An API key is the usual method; a "
            "password login needs the DDM user's email address, not a plain username. A failed login keeps the "
            "previously saved one."
        ),
        input_schema=object_schema(
            {
                "server": {"type": "string", "description": "Profile name, such as lab."},
                "url": {
                    "type": "string",
                    "description": "Managed API URL, such as http://host/graphql; needed for a new profile.",
                },
                "method": {"type": "string", "enum": ["saved", "password", "api_key"]},
                "username": {"type": "string", "description": "The DDM user's email address."},
                "password": {"type": "string"},
                "api_key": {"type": "string"},
            },
            ["server", "method"],
        ),
    ),
    McpTool(
        name="logout_ddm",
        path="/ddm/logout",
        description="Remove a DDM server's saved login and disconnect its managed inventory.",
        input_schema=object_schema({"server": {"type": "string"}}, ["server"]),
        destructive=True,
    ),
    McpTool(
        name="create_ddm_domain",
        path="/ddm/domains",
        description="Create a domain on a logged-in DDM server.",
        input_schema=object_schema({"server": {"type": "string"}, "name": {"type": "string"}}, ["server", "name"]),
    ),
    McpTool(
        name="update_ddm_domain",
        path="/ddm/domains/update",
        description="Rename or remove a Dante Domain Manager domain, named by its name or ID.",
        input_schema=object_schema(
            {
                "domain": {"type": "string", "description": "Domain name or ID."},
                "action": {"type": "string", "enum": ["rename", "remove"]},
                "name": {"type": "string", "description": "New name; needed to rename."},
                "server": {"type": "string", "description": "DDM server profile, when the domain name is ambiguous."},
                "domain_id": {"type": "string", "description": "Domain ID, instead of domain."},
            },
            ["action"],
        ),
        destructive=True,
    ),
    McpTool(
        name="set_ddm_enrollment",
        path="/ddm/enrollment",
        description=(
            "Enroll a device in a Dante Domain Manager domain, or unenroll it. Name the device and domain; "
            "the server finds the DDM server, device ID and domain ID."
        ),
        input_schema=object_schema(
            {
                "device": {"type": "string", "description": "Device name as returned by list_devices."},
                "domain": {"type": "string", "description": "Domain name or ID; needed to enroll."},
                "action": {"type": "string", "enum": ["enroll", "unenroll"]},
                "server": {"type": "string", "description": "DDM server profile, when the device alone is ambiguous."},
                "device_id": {"type": "string", "description": "DDM device ID, instead of device."},
                "domain_id": {"type": "string", "description": "DDM domain ID, instead of domain."},
            },
            ["action"],
        ),
        destructive=True,
    ),
    McpTool(
        name="select_ddm_context",
        path="/ddm/context",
        description="Select a saved DDM context, or create and select one for a domain.",
        input_schema=object_schema(
            {
                "context": {"type": "string", "description": "Saved context name."},
                "domain": {"type": "string", "description": "Domain name or ID, to create a context for it."},
                "server": {"type": "string"},
                "domain_id": {"type": "string"},
            },
            [],
        ),
    ),
    McpTool(
        name="edit_ddm_profile",
        path="/ddm/profile",
        description="Edit or remove a saved DDM server profile. Login credentials are managed separately.",
        input_schema=object_schema(
            {
                "server": {"type": "string"},
                "action": {"type": "string", "enum": ["edit", "remove"]},
                "name": {"type": "string"},
                "url": {"type": "string"},
            },
            ["server", "action"],
        ),
        destructive=True,
    ),
    McpTool(
        name="get_device_diagnostics",
        path="/diagnostics/{device}",
        description=(
            "Measured receive-flow latency against its budget, late packets, and clock state and offset, from what "
            "the server has observed since it started. Give a device for its flows, or leave it out for one line per "
            "device across the network. Counters are not reset."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "detail": {"type": "string", "enum": ["compact", "debug"], "default": "compact"},
            },
            [],
        ),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="reset_device_diagnostics",
        path="/diagnostics/reset",
        description="Reset the daemon's diagnostic counters for one device.",
        input_schema=object_schema({"device": device_property()}, ["device"]),
        destructive=True,
    ),
    McpTool(
        name="set_clock_warning_policy",
        path="/diagnostics/policy",
        description="Enable or disable clock variation warnings for one device.",
        input_schema=object_schema(
            {"device": device_property(), "clock_variation_warnings": {"type": "boolean"}},
            ["device", "clock_variation_warnings"],
        ),
    ),
    McpTool(
        name="refresh_discovery",
        path="/discovery/refresh",
        description="Request a fresh mDNS discovery scan, optionally targeting one address.",
        input_schema=object_schema({"address": {"type": "string"}}, []),
        read_only=True,
    ),
    McpTool(
        name="report_unresponsive",
        path="/report-unresponsive",
        description="Mark an online device as an offline candidate after an independent failed reachability check.",
        input_schema=object_schema({"device": device_property()}, ["device"]),
    ),
    McpTool(
        name="set_clock_source",
        path="/set-clock-source",
        description="Select an advertised clock source by its numeric code with readback.",
        input_schema=object_schema(
            {"device": device_property(), "clock_source": {"type": "integer", "minimum": 0, "maximum": 65535}},
            ["device", "clock_source"],
        ),
        destructive=True,
    ),
    McpTool(
        name="set_clock_subdomain",
        path="/set-clock-subdomain",
        description="Set a clock subdomain to an ASCII name, hex bytes, or 'unset' with readback.",
        input_schema=object_schema(
            {"device": device_property(), "subdomain": {"type": "string"}}, ["device", "subdomain"]
        ),
        destructive=True,
    ),
    McpTool(
        name="configure_monitoring_port",
        path="/settings/monitoring",
        description="Change the daemon's monitoring port; requires an available port and persists the setting.",
        input_schema=object_schema({"port": {"type": "integer", "minimum": 1, "maximum": 65535}}, ["port"]),
        destructive=True,
    ),
    McpTool(
        name="get_network_overview",
        path="/devices",
        description=(
            "One compact snapshot of the devices on the network, clock leaders, problems and Dante Domain Manager "
            "state. Devices that are off the network are not problems and are left out; list_devices shows them with "
            "when they were last seen. Use focused tools for channels, routes or forensic detail."
        ),
        input_schema=object_schema({}, []),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="get_clock_status",
        path="/devices",
        description=(
            "Clock synchronisation across the whole network in one call: every device's role, leader, lock state, "
            "preferred-leader flag and frequency offset, grouped by clock domain (each Dante Domain Manager domain "
            "and the unmanaged devices form separate domains with their own leader)."
        ),
        input_schema=object_schema(
            {
                "detail": {
                    "type": "string",
                    "enum": ["compact", "debug"],
                    "default": "compact",
                    "description": "Use debug only when raw clock records are needed.",
                }
            },
            [],
        ),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="find_channels",
        path="/devices",
        description=(
            "Find anything that carries audio by name, such as 'tv speakers', 'vrroom' or 'chromium': Dante channels "
            "across the network, plus Shure wireless channels and the JACK ports, PulseAudio inputs, outputs and "
            "application streams on every computer running netaudio. Each match shows what feeds it, where it goes "
            "and whether it has signal right now; a channel on an offline device says so."
        ),
        input_schema=object_schema(
            {
                "query": {"type": "string", "minLength": 1, "description": "Words in a channel label or device name."},
                "channel_type": {"type": "string", "enum": ["any", "rx", "tx"], "default": "any"},
            },
            ["query"],
        ),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="get_routing",
        path="/devices",
        description=(
            "All configured receive routes across the network, including unresolved ones, grouped by receiving "
            "device. Each line reads 'rx number rx label \u2190 tx channel on tx device'; multicast and self routes "
            "are marked, and failing routes give the reason. Channel numbers and labels both work in routing calls. "
            "Filter by receiving device, receive channel, or status."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "rx_channel": {"oneOf": [{"type": "string"}, {"type": "integer", "minimum": 1}]},
                "status": {
                    "type": "string",
                    "enum": ["all", "ok", "problem", "waiting"],
                    "default": "all",
                    "description": "waiting selects routes whose transmitting device is off the network; those are "
                    "not problems.",
                },
            },
            [],
        ),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="get_device",
        path="/devices/{device}",
        description=(
            "Details for one device. Pick sections to keep the answer small: summary (identity, address, rate, "
            "latency, clock, subscription problems), channels (custom rx and tx channel names by number; channels "
            "still on factory names are listed as number ranges), subscriptions "
            "(only receive channels that have a route, with its status; a receive channel absent here is "
            "unsubscribed), network (interfaces and redundancy), availability (which operations are "
            "writable and why not), flows (transmit and receive flows), or full for the raw record."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
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
        name="get_device_record",
        path="/devices/{device}",
        description=(
            "Raw fields of one device's record, such as receiver_flow_connection_health or clock_observations. "
            "Use it for fields get_device full lists as omitted, or to read a few raw fields without the rest."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": "Top-level field names from get_device full.",
                },
            },
            ["device", "fields"],
        ),
        method="GET",
        read_only=True,
    ),
    McpTool(
        name="apply_preset",
        path="/presets/load",
        description=(
            "Apply a Dante Controller or netaudio preset. Each preset device goes to the network device with the same "
            "name unless targets says otherwise; preset devices with no match are skipped. Writes routing, audio "
            "settings and network configuration; network changes may require a reboot."
        ),
        input_schema=object_schema(
            {
                "preset": {"type": "string", "description": "Name of a preset saved on the server (list_presets)."},
                "xml": {
                    "type": "string",
                    "description": "Preset XML as exported by Dante Controller, instead of preset.",
                },
                "targets": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                    "description": "Preset device name to the network device it should be applied to.",
                },
                "skip": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Preset device names to leave out.",
                },
                "digest": {
                    "type": "string",
                    "description": "Digest from preview_preset, to make sure the XML is unchanged.",
                },
                "confirm_destructive": {
                    "type": "boolean",
                    "description": "Set true only after the user accepted that a sample-rate change may remove transmit flow channels.",
                },
                "store_current_configuration": {
                    "type": "boolean",
                    "description": "Also store the result as each device's saved configuration.",
                },
            },
            [],
        ),
        destructive=True,
    ),
    McpTool(
        name="create_transmit_flow",
        path="/transmit-flows/create",
        description=(
            "Create a multicast transmit flow. The specification is the same as plan_transmit_flow's; call without "
            "confirmed to see the plan."
        ),
        input_schema=object_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": device_property(),
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
        input_schema=object_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": device_property(),
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
        input_schema=object_schema({"device": device_property()}, ["device"]),
    ),
    McpTool(
        name="forget_device",
        path="/devices/{device}",
        description=(
            "Forget a device that is off the network so it no longer appears in the device list. Use it when the "
            "user, or you, decide the device no longer matters, such as gear that was attached for a while and then "
            "removed. A forgotten device comes back by itself if it returns to the network. Routes on other devices "
            "that point at it stay as they are, and the reply lists them. every_device_off forgets every device "
            "that is off, including ones enrolled in Dante Domain Manager."
        ),
        input_schema=object_schema(
            {
                "device": device_property(),
                "every_device_off": {
                    "type": "boolean",
                    "description": "Forget every device that is off the network instead of one.",
                },
            },
            [],
        ),
        method="DELETE",
    ),
    McpTool(
        name="lock_device",
        path="/lock",
        description="Lock a device against configuration changes with a four digit PIN. Requires a device lock key on the server.",
        input_schema=object_schema(
            {"device": device_property(), "pin": {"type": "string", "pattern": "^[0-9]{4}$"}},
            ["device", "pin"],
        ),
        destructive=True,
    ),
    McpTool(
        name="plan_transmit_flow",
        path="/transmit-flows/plan",
        description=(
            "Check whether a multicast transmit flow can be created on a device and how, without changing anything. "
            "The specification needs channels, a list of transmit channel numbers or labels, or channel_slots, a "
            "list of {slot, transmitter_channel} objects. media_mode defaults to native_dante, flow_type to "
            "multicast, and devices that need a flow ID get the lowest free one unless identity.global_flow_id is "
            "given."
        ),
        input_schema=object_schema(
            {"device": device_property(), "specification": {"type": "object"}},
            ["device", "specification"],
        ),
        read_only=True,
    ),
    McpTool(
        name="preview_preset",
        path="/presets/preview",
        description="List a preset's devices, the settings it holds for each, and which network devices match them.",
        input_schema=object_schema(
            {
                "devices": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Network devices to match against; all devices when omitted.",
                },
                "preset": {"type": "string", "description": "Name of a preset saved on the server (list_presets)."},
                "xml": {
                    "type": "string",
                    "description": "Preset XML as exported by Dante Controller, instead of preset.",
                },
            },
            [],
        ),
        read_only=True,
    ),
    McpTool(
        name="reboot",
        path="/reboot",
        description="Reboot a device. Audio stops while it restarts and it may come back with a different address.",
        input_schema=object_schema(
            {"confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION}, "device": device_property()},
            ["confirmed", "device"],
        ),
        destructive=True,
        requires_confirmation=True,
    ),
    McpTool(
        name="refresh",
        path="/refresh",
        description="Re-read one device, or every device when no device is given.",
        input_schema=object_schema({"device": device_property()}, []),
        read_only=True,
    ),
    McpTool(
        name="rename_channel",
        path="/rename-channel",
        description=(
            "Set a transmit or receive channel's label. An empty name resets it to the device default. Renaming a "
            "transmit channel breaks routes on other devices that subscribe to it by its old label."
        ),
        input_schema=object_schema(
            {
                "channel": CHANNEL_ARGUMENT,
                "channel_type": {"type": "string", "enum": ["rx", "tx"]},
                "device": device_property(),
                "name": {"type": "string", "maxLength": 31},
            },
            ["channel", "channel_type", "device", "name"],
        ),
    ),
    McpTool(
        name="rename_device",
        path="/rename-device",
        description="Set a device's name. Other devices route to it by name, so renaming can break their subscriptions.",
        input_schema=object_schema(
            {"device": device_property(), "name": {"type": "string", "maxLength": 31}},
            ["device", "name"],
        ),
        destructive=True,
    ),
    McpTool(
        name="save_preset",
        path="/presets/save",
        description=(
            "Read devices' current configuration and save it as a named preset on the server, in the same preset "
            "directory the netaudio CLI uses. apply_preset restores it by name."
        ),
        input_schema=object_schema(
            {
                "devices": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": "Device names.",
                },
                "name": {"type": "string", "minLength": 1, "maxLength": 120},
                "sections": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["audio", "device_controls", "network", "routing"]},
                    "minItems": 1,
                    "default": ["audio", "routing"],
                },
                "replace": {"type": "boolean", "description": "Overwrite a saved preset with the same name."},
            },
            ["devices", "name"],
        ),
        confirmable=False,
    ),
    McpTool(
        name="list_presets",
        path="/presets",
        description="Presets saved on the server: name, devices and when each was saved.",
        input_schema=object_schema({}, []),
        read_only=True,
    ),
    McpTool(
        name="set_aes67",
        path="/set-aes67",
        description="Enable or disable AES67 mode on a device. Takes effect after a reboot.",
        input_schema=object_schema(
            {"device": device_property(), "enabled": {"type": "boolean"}}, ["device", "enabled"]
        ),
    ),
    McpTool(
        name="set_encoding",
        path="/set-encoding",
        description="Set a device's audio encoding in bits: 16, 24 or 32. Only values in the device's supported_encodings are accepted.",
        input_schema=object_schema(
            {"device": device_property(), "encoding": {"type": "integer", "enum": [16, 24, 32]}},
            ["device", "encoding"],
        ),
    ),
    McpTool(
        name="set_gain",
        path="/set-gain",
        description=(
            "Set the analog gain level of an AVIO input or output channel. Levels 1 through 5 map to the labels in "
            "the device's gain_level_choices. An input adapter's channels are its transmit channels; an output "
            "adapter's are its receive channels."
        ),
        input_schema=object_schema(
            {
                "channel": CHANNEL_ARGUMENT,
                "device": device_property(),
                "device_type": {"type": "string", "enum": ["input", "output"]},
                "gain_level": {"type": "integer", "minimum": 1, "maximum": 5},
            },
            ["channel", "device", "device_type", "gain_level"],
        ),
    ),
    McpTool(
        name="set_interface",
        path="/interface",
        description="Configure a network interface for DHCP or a static address. Takes effect after a reboot; a wrong static address can make the device unreachable.",
        input_schema=object_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": device_property(),
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
        input_schema=object_schema(
            {"device": device_property(), "latency": {"type": "number", "exclusiveMinimum": 0}},
            ["device", "latency"],
        ),
    ),
    McpTool(
        name="set_preferred_leader",
        path="/set-preferred-leader",
        description="Mark a device as preferred clock leader or clear that preference.",
        input_schema=object_schema(
            {"device": device_property(), "preferred": {"type": "boolean"}}, ["device", "preferred"]
        ),
    ),
    McpTool(
        name="set_redundancy",
        path="/redundancy",
        description="Set a device's Dante redundancy mode. Only modes listed in the device's network_redundancy.available_modes are accepted; takes effect after a reboot.",
        input_schema=object_schema(
            {
                "confirmed": {"type": "boolean", "description": CONFIRMATION_DESCRIPTION},
                "device": device_property(),
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
        input_schema=object_schema(
            {
                "device": device_property(),
                "sample_rate": {"type": "integer", "enum": [44100, 48000, 88200, 96000, 176400, 192000]},
                "confirm_destructive": {
                    "type": "boolean",
                    "description": (
                        "Set true only after the user accepted that the device's transmit flows may lose channels "
                        "at the new rate. Without it, the server refuses a change whose effect on those flows it "
                        "cannot predict."
                    ),
                },
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
        input_schema=object_schema(
            {
                "routes": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {
                            "rx_channel": {"oneOf": [{"type": "integer", "minimum": 1}, {"type": "string"}]},
                            "rx_device": {"type": "string"},
                            "tx_channel": {"oneOf": [{"type": "string"}, {"type": "integer", "minimum": 1}]},
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
        description=(
            "Route (patch) a transmit channel from one device into a receive channel on another, replacing any "
            "route already feeding that receive channel. Channels accept a label or number; get_device with the "
            "channels section lists them."
        ),
        input_schema=object_schema(
            {
                "rx_channel": {
                    "oneOf": [{"type": "integer", "minimum": 1}, {"type": "string"}],
                    "description": "Receive channel number or unique label on rx_device.",
                },
                "rx_device": {"type": "string", "description": "Receiving device name."},
                "tx_channel": {
                    "oneOf": [{"type": "string"}, {"type": "integer", "minimum": 1}],
                    "description": "Transmit channel name or number on tx_device.",
                },
                "tx_device": {"type": "string", "description": "Transmitting device name."},
            },
            ["rx_channel", "rx_device", "tx_channel", "tx_device"],
        ),
    ),
    McpTool(
        name="unlock_device",
        path="/unlock",
        description="Unlock a locked device using its PIN.",
        input_schema=object_schema(
            {"device": device_property(), "pin": {"type": "string", "pattern": "^[0-9]{4}$"}},
            ["device", "pin"],
        ),
    ),
    McpTool(
        name="unsubscribe",
        path="/unsubscribe",
        description="Remove the route feeding a receive channel, leaving it unsubscribed.",
        input_schema=object_schema(
            {
                "rx_channel": {
                    "oneOf": [{"type": "integer", "minimum": 1}, {"type": "string"}],
                    "description": "Receive channel number or unique label on rx_device.",
                },
                "rx_device": {"type": "string", "description": "Receiving device name."},
            },
            ["rx_channel", "rx_device"],
        ),
        destructive=True,
    ),
)

RESOURCES: tuple[McpResource, ...] = (
    McpResource(
        uri="netaudio://ddm/connections",
        path="/ddm/connections",
        name="Dante Domain Manager connections",
        description="Configured DDM server profiles, contexts, and default context without credentials.",
        tool_name="get_ddm_connections",
    ),
    McpResource(
        uri="netaudio://ddm/domains",
        path="/ddm/domains",
        name="Dante Domain Manager domains",
        description="Discovered managed domains and their identifiers.",
        tool_name="get_ddm_domains",
        tool_description=(
            "Dante Domain Manager domains with their health and enrolled devices: each device's product, "
            "enrollment, connection and any alert. Debug returns the raw DDM records."
        ),
        tool_properties={"detail": {"type": "string", "enum": ["compact", "debug"], "default": "compact"}},
    ),
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
            "Every known device keyed by its name: product, address, online state, management (and DDM domain), "
            "sample rate, latency, channel counts, route count and any failing routes. A device that is off the "
            "network shows only its product and when it was last seen; forget_device removes ones that no longer "
            "matter. Any tool's device argument takes these names. Use get_device for channels, routes, network and "
            "operation availability."
        ),
    ),
    McpResource(
        uri="netaudio://event-journal",
        path="/event-journal",
        name="Event journal",
        description="Recent monitoring events: latency, late packets, issue lifecycle and configuration changes.",
        tool_name="get_event_journal",
        tool_description=(
            "Recent monitoring events, newest first. Repeated observations are grouped by default; "
            "filter by device or kind, page with before_sequence, or request debug detail."
        ),
        tool_properties={
            "before_sequence": {
                "type": "integer",
                "minimum": 1,
                "description": "Return older events with sequence below this value.",
            },
            "collapse_repeats": {
                "type": "boolean",
                "default": True,
                "description": "Group repeated observations of the same event target; set false for every event.",
            },
            "device": device_property(),
            "detail": {
                "type": "string",
                "enum": ["compact", "debug"],
                "default": "compact",
                "description": "Debug includes complete forensic event records.",
            },
            "kind": {
                "type": "string",
                "description": "Only events of this kind, for example receiver_flow_latency_high.",
            },
            "limit": EVENT_LIMIT_PROPERTY,
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
            "Problems detected on the network, grouped by kind and device, worst first. A failing route group names "
            "the transmitting device behind each channel. Devices that are off the network and routes waiting for "
            "them are not problems and are left out; get_routing with status waiting lists those routes, and "
            "naming an off device here shows its last report. Defaults to open issues; set state to resolved or all "
            "for history. Filter by device and page with limit; debug lists every issue separately."
        ),
        tool_properties={
            "detail": {"type": "string", "enum": ["compact", "debug"], "default": "compact"},
            "device": device_property(),
            "limit": LIMIT_PROPERTY,
            "offset": {"type": "integer", "minimum": 0, "default": 0},
            "state": {"type": "string", "enum": ["all", "open", "resolved"], "default": "open"},
        },
    ),
    McpResource(
        uri="netaudio://metering",
        path="/metering/cache",
        name="Signal levels",
        description="Latest signal level per channel for devices that are being metered.",
        tool_name="get_signal_levels",
        tool_description=(
            "Whether audio is present. Without a device: for each device, the channels carrying signal with their "
            "dBFS level and how many are silent. With a device: the rx and tx channels with signal and their level, "
            "and the silent ones by name or number range. Devices with detailed metering are sampled on request. "
            "points measures named places anywhere in the chain instead: JACK ports, PulseAudio inputs, outputs "
            "and applications, Shure channels or Dante channels. Debug includes every channel and the raw meter codes. "
            "over_last_seconds, ending_at or listen_seconds report the recorded levels over a period instead of one "
            "sample, which is what to use for sound that comes and goes."
        ),
        tool_properties={
            "device": device_property(),
            "points": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "description": (
                    "Places to measure, such as system:capture_1, default input, Discord or AD4D-A ch1; put another "
                    "computer's name first for its points, such as workstation default input."
                ),
            },
            "detail": {"type": "string", "enum": ["compact", "debug"], "default": "compact"},
            **PERIOD_PROPERTIES,
        },
    ),
    McpResource(
        uri="netaudio://metering/status",
        path="/metering/status",
        name="Metering status",
        description="Whether detailed metering is available and which devices are being metered.",
        tool_name="get_metering_status",
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
    sorted(
        (
            *ACTION_TOOLS,
            *(resource.to_tool() for resource in RESOURCES),
            *(McpTool(**specification) for specification in HOST_AUDIO_TOOL_SPECS),
        ),
        key=lambda tool: tool.name,
    )
)
PUBLIC_TOOL_NAMES = frozenset(
    {
        "discover_tools",
        "invoke_tool",
        "find_channels",
        "get_network_overview",
        "get_routing",
        "get_device",
        "list_devices",
        "get_issues",
        "get_event_journal",
        "get_clock_status",
        "get_signal_levels",
        "get_server_info",
        "set_subscriptions",
        "get_device_diagnostics",
        "trace_signal",
    }
)
PUBLIC_TOOLS = tuple(tool for tool in TOOLS if tool.name in PUBLIC_TOOL_NAMES)

DEVICE_RESOURCE_TEMPLATE = {
    "uriTemplate": "netaudio://devices/{name}",
    "name": "Device",
    "description": "One device by name, with the same fields as the devices resource.",
    "mimeType": "application/json",
}

TOOL_VIEWS = {
    "get_clock_status": clock_status_view,
    "get_device": lambda payload, arguments: device_view(payload, arguments["sections"]),
    "get_device_record": record_fields_view,
    "find_channels": find_channels_view,
    "get_ddm_domains": ddm_domains_view,
    "get_event_journal": events_view,
    "get_routing": routing_view,
    "get_signal_levels": signal_levels_view,
}
TOOLS_BY_NAME = {tool.name: tool for tool in TOOLS}
HISTORICAL_DEVICE_TOOLS = frozenset({"get_issues", "get_event_journal"})
NAME_SELECTED_TOOLS = frozenset({"get_issues", "get_event_journal", "get_routing", "get_signal_levels"})
CHANNEL_SELECTED_TOOLS = frozenset({"rename_channel", "set_gain"})
WARMUP_SECONDS = 30.0
EMPTY_RESULTS = {
    "get_external_flows": "no AES67 or RTP streams are being announced on the network",
    "get_metering_status": "no detailed metering is running",
}
WARMUP_EXEMPT_TOOLS = frozenset(
    {
        "discover_tools",
        "get_ddm_connections",
        "get_ddm_status",
        "get_event_journal",
        "get_server_info",
        "invoke_tool",
        "list_presets",
        "login_ddm",
        "logout_ddm",
    }
)
RESOURCES_BY_URI = {resource.uri: resource for resource in RESOURCES}

DISCOVERY_STOP_WORDS = frozenset(
    {
        "a",
        "all",
        "an",
        "anything",
        "are",
        "can",
        "for",
        "i",
        "is",
        "me",
        "my",
        "of",
        "on",
        "show",
        "the",
        "to",
        "what",
        "why",
    }
)
DISCOVERY_TERMS = {
    "blink": "identify",
    "broken": "issue",
    "change": "set",
    "channels": "channel",
    "connect": "route",
    "devices": "device",
    "disconnect": "unsubscribe",
    "feed": "route",
    "flows": "flow",
    "label": "name",
    "patch": "route",
    "receiver": "receive",
    "rx": "receive",
    "send": "route",
    "subscriptions": "subscription",
    "tx": "transmit",
    "unpatch": "unsubscribe",
    "unroute": "unsubscribe",
    "enrol": "enroll",
    "enrolled": "enroll",
    "enrollment": "enroll",
    "flash": "identify",
    "join": "enroll",
    "locked": "lock",
    "pin": "lock",
    "restart": "reboot",
    "errors": "issue",
    "failed": "issue",
    "failure": "issue",
    "failures": "issue",
    "issues": "issue",
    "late": "latency",
    "levels": "level",
    "meters": "level",
    "missing": "issue",
    "modify": "set",
    "problems": "issue",
    "recent": "event",
    "recently": "event",
    "routed": "route",
    "routes": "route",
    "routing": "route",
    "transmitter": "subscription",
    "transmitters": "subscription",
    "unenrollment": "unenroll",
    "unlocked": "unlock",
    "unresolved": "issue",
    "wrong": "issue",
    "backup": "preset",
    "recall": "apply",
    "restore": "apply",
    "snapshot": "preset",
    "snapshots": "preset",
}
DISCOVERY_WRITE_WORDS = frozenset(
    {
        "apply",
        "change",
        "clear",
        "configure",
        "connect",
        "create",
        "delete",
        "disable",
        "disconnect",
        "enable",
        "enroll",
        "feed",
        "identify",
        "lock",
        "login",
        "logout",
        "make",
        "modify",
        "mute",
        "patch",
        "reboot",
        "remove",
        "rename",
        "reset",
        "route",
        "send",
        "set",
        "start",
        "stop",
        "subscribe",
        "unenroll",
        "unlock",
        "unmute",
        "unpatch",
        "unroute",
        "unsubscribe",
    }
)
DISCOVERY_HINTS = {
    "get_issues": {"latency", "subscription", "audio", "sound"},
    "get_event_journal": {"issue", "latency"},
    "get_network_overview": {"status", "health", "network"},
    "list_devices": {"network", "online", "offline"},
    "forget_device": {"forget", "remove", "dismiss", "offline", "off", "old", "stale", "clean", "gone", "inventory"},
    "get_routing": {"subscription"},
    "rename_device": {"name", "device"},
    "set_subscriptions": {"route", "subscription", "many", "several", "bulk"},
    "subscribe": {"subscription", "receive", "transmit"},
    "unsubscribe": {"route", "subscription", "remove", "clear"},
    "get_device_diagnostics": {
        "health",
        "packet",
        "packets",
        "dropped",
        "drop",
        "latency",
        "stream",
        "streams",
        "jitter",
    },
    "get_signal_levels": {
        "audio",
        "sound",
        "silent",
        "silence",
        "hear",
        "coming",
        "through",
        "signal",
        "history",
        "recorded",
        "last",
        "minute",
        "hour",
        "night",
        "period",
        "talking",
        "loudest",
    },
    "apply_preset": {"preset", "restore", "back"},
    "save_preset": {"preset", "save", "keep"},
    "list_presets": {"preset", "saved"},
    "create_transmit_flow": {"multicast", "stream"},
    "get_wireless_devices": {"mic", "microphone", "wireless", "battery", "transmitter", "receiver"},
    "trace_signal": {
        "audio",
        "sound",
        "silent",
        "silence",
        "hear",
        "chain",
        "path",
        "trace",
        "follow",
        "where",
        "mic",
        "microphone",
        "jack",
        "pulseaudio",
        "pulse",
        "discord",
        "application",
    },
    "get_host_audio": {"jack", "pulse", "pulseaudio", "computer", "host", "linux", "card", "alsa", "xrun", "xruns"},
    "connect_audio_ports": {"jack", "port", "connect", "patch", "route", "computer"},
    "disconnect_audio_ports": {"jack", "port", "disconnect", "unpatch", "computer"},
    "set_audio_volume": {"volume", "louder", "quieter", "pulseaudio", "pulse", "application", "computer"},
    "set_audio_mute": {"mute", "unmute", "pulseaudio", "pulse", "application", "computer"},
    "set_default_audio_device": {"default", "output", "input", "speakers", "microphone", "pulseaudio"},
    "move_audio_stream": {"move", "stream", "application", "output", "input", "pulseaudio"},
    "link_audio_card": {"card", "alsa", "pcie", "usb", "link", "computer"},
    "set_wireless_value": {"mic", "wireless", "gain", "frequency", "name", "mute", "shure", "transmitter"},
    "inspect_device_controls": {"bluetooth", "paired", "pairing", "panel", "video", "hdmi", "analog", "settings"},
    "plan_device_control": {"bluetooth", "name", "discoverable", "discovery", "pairing", "forget", "analog", "level"},
    "apply_device_control": {
        "bluetooth",
        "name",
        "rename",
        "discoverable",
        "discovery",
        "pairing",
        "pair",
        "forget",
        "clear",
        "analog",
        "level",
        "video",
        "hdcp",
    },
}
DISCOVERABLE_WORDS = frozenset({"discoverable", "visible", "on", "true", "yes", "enable", "enabled", "1"})
NOT_DISCOVERABLE_WORDS = frozenset(
    {"not discoverable", "undiscoverable", "hidden", "off", "false", "no", "disable", "disabled", "2"}
)
CLEAR_PAIRING_WORDS = frozenset({"clear", "clear all", "forget", "forget all", "forget phones", "unpair all"})
DANTE_NAME_WORDS = frozenset({"dante device name", "dante name", "device name", "use dante device name"})
DISCOVERY_ALIASES = {
    "subscribe": {"route"},
}
DISCOVERY_CATEGORIES = (
    (
        "this computer's audio",
        (
            "host_audio",
            "audio_ports",
            "audio_volume",
            "audio_mute",
            "audio_device",
            "audio_stream",
            "audio_card",
            "trace_signal",
        ),
    ),
    ("Dante Domain Manager", ("ddm",)),
    ("presets", ("preset",)),
    ("clock", ("clock", "leader", "signal_reference")),
    (
        "routing and flows",
        ("subscri", "route", "routing", "flow", "template", "multicast", "advertisement", "rtp", "igmp"),
    ),
    (
        "monitoring",
        ("issue", "event", "signal", "meter", "metering", "latency_history", "diagnostic", "errors", "unresponsive"),
    ),
    (
        "maintenance and security",
        ("reboot", "identify", "reset", "clear_configuration", "lock", "firmware", "recover", "store_current"),
    ),
    ("wireless and virtual devices", ("wireless", "virtual")),
    ("bulk changes", ("apply_to_devices",)),
    ("device settings", ("set_", "rename", "configure", "device_control", "channel", "interface")),
)
DISCOVERY_CATEGORY_OVERRIDES = {
    "configure_monitoring_port": "devices and server",
    "get_director_sites": "Dante Domain Manager",
    "set_discovery_interfaces": "devices and server",
}
DISCOVERY_SCHEMA_LIMIT = 6
DISCOVERY_DEFAULT_CATEGORY = "devices and server"


def _discovery_words(value: str) -> set[str]:
    return {
        DISCOVERY_TERMS.get(word, word)
        for word in re.findall(r"[a-z0-9]+", value.lower())
        if word not in DISCOVERY_STOP_WORDS
    }


def _discovery_candidates() -> list[McpTool | PlannedOperation]:
    return [
        *(tool for tool in TOOLS if tool.name not in {"discover_tools", "invoke_tool"}),
        *PLANNED_OPERATIONS,
    ]


def _discovery_category(name: str) -> str:
    if name in DISCOVERY_CATEGORY_OVERRIDES:
        return DISCOVERY_CATEGORY_OVERRIDES[name]
    for category, needles in DISCOVERY_CATEGORIES:
        if any(needle in name for needle in needles):
            return category
    return DISCOVERY_DEFAULT_CATEGORY


def _discovery_index() -> dict:
    categories: dict[str, dict[str, list[str]]] = {}
    for candidate in sorted(_discovery_candidates(), key=lambda item: item.name):
        status = "planned" if isinstance(candidate, PlannedOperation) else "available"
        category = categories.setdefault(_discovery_category(candidate.name), {"available": [], "planned": []})
        category[status].append(candidate.name)
    return {
        "categories": {
            name: {key: value for key, value in entry.items() if value} for name, entry in sorted(categories.items())
        },
        "next": (
            "Query a category name for descriptions, or an exact operation name with include_schema=true for its "
            "arguments. Planned operations are designed but not built."
        ),
    }


def _discover_matches(query: str) -> list[McpTool | PlannedOperation]:
    words = _discovery_words(query)
    candidates = _discovery_candidates()
    exact_name = query.strip().lower().replace(" ", "_")
    if exact_name in TOOLS_BY_NAME and exact_name not in {"discover_tools", "invoke_tool"}:
        return [TOOLS_BY_NAME[exact_name]]
    if exact_name in PLANNED_BY_NAME:
        return [PLANNED_BY_NAME[exact_name]]
    category = query.strip().casefold()
    in_category = [candidate for candidate in candidates if _discovery_category(candidate.name).casefold() == category]
    if in_category:
        return sorted(in_category, key=lambda item: (isinstance(item, PlannedOperation), item.name))
    if not words:
        return candidates
    wants_write = bool(set(re.findall(r"[a-z0-9]+", query.lower())) & DISCOVERY_WRITE_WORDS)

    def rank(tool: McpTool | PlannedOperation) -> tuple[int, str]:
        name_words = _discovery_words(tool.name)
        description_words = _discovery_words(tool.description)
        hint_words = DISCOVERY_HINTS.get(tool.name, set())
        alias_words = DISCOVERY_ALIASES.get(tool.name, set())
        matched = words & (name_words | description_words | hint_words | alias_words)
        if not matched:
            return (0, tool.name)
        score = (
            8 * len(words & (name_words | alias_words))
            + 2 * len(words & description_words)
            + 4 * len(words & hint_words)
        )
        if matched == words:
            score += 5
        if tool.read_only != wants_write:
            score += 3
        if isinstance(tool, PlannedOperation):
            score = max(1, score - 8)
        return (score, tool.name)

    ranked = ((rank(tool), isinstance(tool, PlannedOperation), tool) for tool in candidates)
    return [
        tool for (score, _), _, tool in sorted(ranked, key=lambda item: (-item[0][0], item[1], item[0][1])) if score
    ]


def _discovery_result(query: str, limit: int, offset: int, include_schema: bool) -> dict:
    if not query.strip():
        return _discovery_index()
    matches = _discover_matches(query)
    selected = matches[offset : offset + limit]
    result = {
        "operations": [
            _discovery_entry(candidate, include_schema and position < DISCOVERY_SCHEMA_LIMIT)
            for position, candidate in enumerate(selected)
        ],
        "matched": len(matches),
        "next_offset": offset + len(selected) if offset + len(selected) < len(matches) else None,
    }
    if include_schema and len(selected) > DISCOVERY_SCHEMA_LIMIT:
        result["schema_note"] = (
            f"schemas are included for the first {DISCOVERY_SCHEMA_LIMIT} matches; search for another by its name "
            "to get its schema"
        )
    if not matches:
        result["hint"] = "Nothing matched; call discover_tools without a query to see every operation by category."
    return result


def _discovery_entry(candidate: McpTool | PlannedOperation, include_schema: bool) -> dict:
    entry = {
        "name": candidate.name,
        "description": candidate.description,
        "read_only": candidate.read_only,
        "required": candidate.input_schema["required"],
        "status": "planned" if isinstance(candidate, PlannedOperation) else "available",
    }
    if isinstance(candidate, PlannedOperation):
        entry["blocker"] = candidate.blocker
        entry["blocked_by"] = candidate.blocked_by
    if include_schema:
        entry["input_schema"] = candidate.input_schema
    return entry


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


def _record_for_selector(records: dict, selector: str) -> dict | None:
    if not isinstance(selector, str) or not selector:
        return None
    key = selector.casefold()
    matches = [
        record
        for server_name, record in records.items()
        if isinstance(record, dict)
        and any(
            isinstance(value, str) and value.casefold() == key
            for value in (server_name, record.get("server_name"), record.get("name"), record.get("inventory_id"))
        )
    ]
    if len(matches) > 1:
        raise ValueError(f"device selector {selector!r} is ambiguous")
    return matches[0] if matches else None


def _device_not_found(records: dict, selector) -> str:
    names = sorted(
        str(record.get("name")) for record in records.values() if isinstance(record, dict) and record.get("name")
    )
    suggestions = close_matches(str(selector), names)
    if suggestions:
        return f"no device named {selector!r}; did you mean {' or '.join(suggestions)}?"
    return f"no device named {selector!r}; known devices: {', '.join(names) or 'none'}"


def _require_record(records: dict, selector) -> dict:
    record = _record_for_selector(records, selector)
    if record is None:
        raise ValueError(_device_not_found(records, selector))
    return record


def _channel_names(record: dict | None, direction: str) -> dict[int, str]:
    channels = ((record or {}).get("channels") or {}).get(direction) or {}
    names = {}
    for number, value in channels.items():
        name = channel_label(value)
        if str(number).isdecimal() and isinstance(name, str):
            names[int(number)] = name
    return names


def _unknown_channel(record: dict | None, direction: str, channel) -> str:
    names = _channel_names(record, direction)
    kind = "receive" if direction == "receivers" else "transmit"
    device = (record or {}).get("name") or "the device"
    suggestions = close_matches(str(channel), names.values())
    if suggestions:
        return f"{device} has no {kind} channel {channel!r}; did you mean {' or '.join(suggestions)}?"
    listing = ", ".join(f"{number}={name}" for number, name in sorted(names.items())[:16])
    more = f" and {len(names) - 16} more" if len(names) > 16 else ""
    return f"{device} has no {kind} channel {channel!r}; its {kind} channels are {listing}{more}"


def _channel_number(record: dict | None, channel: int | str, direction: str = "receivers") -> int:
    kind = "receive" if direction == "receivers" else "transmit"
    if isinstance(channel, bool):
        raise ValueError(f"{kind} channel must be a number or unique label")
    names = _channel_names(record, direction)
    if isinstance(channel, int):
        if channel < 1:
            raise ValueError(f"{kind} channel number must be positive")
        if names and channel not in names:
            raise ValueError(_unknown_channel(record, direction, channel))
        return channel
    if not isinstance(channel, str) or not channel:
        raise ValueError(f"{kind} channel must be a number or unique label")
    for candidates in (
        [number for number, name in names.items() if name == channel],
        [number for number, name in names.items() if name.casefold() == channel.casefold()],
    ):
        if len(candidates) > 1:
            raise ValueError(f"{kind} channel label {channel!r} is ambiguous; use its number")
        if candidates:
            return candidates[0]
    if channel.isdecimal() and int(channel) > 0 and (not names or int(channel) in names):
        return int(channel)
    factory = _factory_number(record, direction, channel)
    if factory is not None:
        return factory
    raise ValueError(_unknown_channel(record, direction, channel))


def _channel_request(name: str, request: dict, record: dict) -> dict:
    if name == "rename_channel":
        direction = "transmitters" if request.get("channel_type") == "tx" else "receivers"
    else:
        direction = "transmitters" if request.get("device_type") == "input" else "receivers"
    result = {key: value for key, value in request.items() if key != "channel"}
    result["channel_number"] = _channel_number(record, request["channel"], direction)
    return result


def _factory_number(record: dict | None, direction: str, channel: str) -> int | None:
    channels = ((record or {}).get("channels") or {}).get(direction) or {}
    matches = [
        int(number)
        for number, value in channels.items()
        if str(number).isdecimal()
        and isinstance(value, dict)
        and isinstance(value.get("name"), str)
        and value["name"].casefold() == channel.casefold()
    ]
    return matches[0] if len(matches) == 1 else None


def _transmit_channel_name(record: dict | None, channel: int | str) -> str:
    if isinstance(channel, bool) or not isinstance(channel, (int, str)) or channel in ("", 0):
        raise ValueError("transmit channel must be a name or positive number")
    names = _channel_names(record, "transmitters")
    if isinstance(channel, str):
        if not names or channel in names.values():
            return channel
        folded = [name for name in names.values() if name.casefold() == channel.casefold()]
        if len(folded) == 1:
            return folded[0]
        if channel.isdecimal() and int(channel) in names:
            return names[int(channel)]
        factory = _factory_number(record, "transmitters", channel)
        if factory is not None and factory in names:
            return names[factory]
        raise ValueError(_unknown_channel(record, "transmitters", channel))
    if channel < 1:
        raise ValueError("transmit channel must be a name or positive number")
    if channel not in names:
        if record is None:
            raise ValueError(
                "name the transmit channel; its device is not on the network, so numbers cannot be resolved"
            )
        raise ValueError(_unknown_channel(record, "transmitters", channel))
    return names[channel]


def _record_ready(record: dict) -> bool:
    if not record.get("online"):
        return True
    return record.get("sample_rate_hz") is not None and record.get("latency_ms") is not None


def _found_record(records: dict, selector: str) -> dict | bool | None:
    try:
        return _record_for_selector(records, selector)
    except ValueError:
        return True


def _request_selectors(arguments: dict) -> list[str]:
    selectors = [arguments.get(key) for key in ("device", "rx_device", "tx_device")]
    for route in arguments.get("routes") or []:
        if isinstance(route, dict):
            selectors.extend([route.get("rx_device"), route.get("tx_device")])
    where = arguments.get("where") if isinstance(arguments.get("where"), dict) else {}
    selectors.extend(where.get("devices") or [])
    selectors.extend(arguments.get("devices") or [] if isinstance(arguments.get("devices"), list) else [])
    return [selector for selector in selectors if isinstance(selector, str) and selector]


def _flow_specification(specification: dict, record: dict, flows: list) -> dict:
    result = copy.deepcopy(specification)
    result.setdefault("media_mode", "native_dante")
    result.setdefault("flow_type", "multicast")
    channels = result.pop("channels", None)
    if channels is not None and "channel_slots" in result:
        raise ValueError("give either channels or channel_slots, not both")
    if channels is not None:
        if not isinstance(channels, list) or not channels:
            raise ValueError("channels must be a non-empty list of transmit channel numbers or labels")
        result["channel_slots"] = [
            {"slot": index + 1, "transmitter_channel": channel} for index, channel in enumerate(channels)
        ]
    slots = result.get("channel_slots")
    if not isinstance(slots, list) or not slots:
        raise ValueError("the specification needs channels, a list of transmit channel numbers or labels")
    result["channel_slots"] = [
        {
            **slot,
            "slot": slot.get("slot", index + 1),
            "transmitter_channel": _channel_number(record, slot.get("transmitter_channel"), "transmitters"),
        }
        if isinstance(slot, dict)
        else slot
        for index, slot in enumerate(slots)
    ]
    identity = result.setdefault("identity", {})
    free = free_flow_ids(record, flows)
    if isinstance(identity, dict) and "global_flow_id" not in identity and free:
        identity["global_flow_id"] = free[0]
    return result


def _route_request(request: dict, records: dict) -> dict:
    def route(entry: dict) -> dict:
        result = dict(entry)
        receiver = _require_record(records, result["rx_device"])
        result["rx_device"] = receiver.get("server_name") or result["rx_device"]
        result["rx_channel"] = _channel_number(receiver, result["rx_channel"])
        if result.get("tx_device") is not None:
            transmitter = _record_for_selector(records, result["tx_device"])
            if transmitter is not None:
                result["tx_device"] = transmitter.get("name") or result["tx_device"]
            result["tx_channel"] = _transmit_channel_name(transmitter, result["tx_channel"])
        return result

    if "routes" in request:
        return {**request, "routes": [route(entry) for entry in request["routes"]]}
    return route(request)


def _ddm_unknown_reason(ddm_status: dict) -> str:
    failures = [
        f"{name}: {server.get('last_error') or server.get('state')}"
        for name, server in (ddm_status.get("servers") or {}).items()
        if isinstance(server, dict) and not server.get("fresh")
    ]
    detail = "; ".join(failures) or "inventory is not current"
    return f"management state is unknown because the DDM inventory is not current ({detail})"


def _batch_skip_reason(operation: str, record: dict) -> str | None:
    if not record.get("online"):
        return "offline"
    if operation == "lock_device" and record.get("is_locked") is True:
        return "already locked"
    if operation == "unlock_device" and record.get("is_locked") is False:
        return "not locked"
    return None


def _loose(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(text).casefold())


def _select_devices(records: dict, where: dict, operation: str, ddm_status: dict) -> dict:
    ddm_known = ddm_inventory_known(ddm_status)
    candidates = [record for record in records.values() if isinstance(record, dict)]
    if "devices" in where:
        named = []
        for selector in where["devices"]:
            record = _record_for_selector(records, selector)
            if record is None:
                raise ValueError(f"device {selector!r} was not found")
            named.append(record)
        candidates = named
    selected, skipped, undetermined = [], [], []
    for record in sorted(candidates, key=lambda item: str(item.get("name") or item.get("server_name"))):
        name = record.get("name") or record.get("server_name")
        models = {
            str(value).casefold()
            for value in (
                record.get("model"),
                record.get("dante_model"),
                record.get("model_id"),
                record.get("product_name"),
            )
            if value
        }
        if "model" in where and not any(_loose(where["model"]) in _loose(model) for model in models):
            continue
        if (
            "manufacturer" in where
            and where["manufacturer"].casefold() not in str(record.get("manufacturer") or "").casefold()
        ):
            continue
        if "online" in where and bool(record.get("online")) != where["online"]:
            continue
        if "locked" in where and record.get("is_locked") is not where["locked"]:
            continue
        if "management" in where:
            management = management_state(record, ddm_known)
            if management is None:
                undetermined.append(name)
                continue
            if management != where["management"]:
                continue
        reason = _batch_skip_reason(operation, record)
        if reason:
            skipped.append({"device": name, "reason": reason})
            continue
        selected.append(record)
    return {
        "selected": selected,
        "skipped": skipped,
        "undetermined": {"devices": undetermined, "reason": _ddm_unknown_reason(ddm_status)} if undetermined else None,
    }


def _batch_token(operation: str, arguments: dict, records: list[dict]) -> str:
    identity = {
        "operation": operation,
        "arguments": arguments,
        "devices": sorted(str(record.get("server_name")) for record in records),
    }
    return hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _find_domain(domain: str, domains: list[dict], server: str | None) -> dict:
    key = domain.casefold()
    matches = [
        candidate
        for candidate in domains
        if key in {str(candidate.get("id") or "").casefold(), str(candidate.get("name") or "").casefold()}
        and server in {None, candidate.get("ddm_server_profile")}
    ]
    if not matches:
        known = ", ".join(sorted(str(candidate.get("name") or candidate.get("id")) for candidate in domains))
        raise ValueError(f"domain {domain!r} was not found; known domains: {known or 'none'}")
    if len(matches) > 1:
        raise ValueError(f"domain {domain!r} matches more than one DDM server; name the server")
    return matches[0]


def _apply_domain(request: dict, domains: list[dict]) -> None:
    domain = request.pop("domain", None)
    if domain is None and isinstance(request.get("domain_id"), str):
        domain = request.pop("domain_id")
    if domain is None:
        return
    match = _find_domain(domain, domains, request.get("server"))
    request["domain_id"] = match.get("id")
    request.setdefault("server", match.get("ddm_server_profile"))


def _enrollment_request(request: dict, records: dict, domains: list[dict], ddm_status: dict) -> dict:
    result = dict(request)
    selector = result.pop("device", None)
    if selector is not None:
        record = _record_for_selector(records, selector)
        if record is None:
            raise ValueError(f"device {selector!r} was not found")
        if not record.get("ddm_device_id"):
            if ddm_status.get("enabled") and not ddm_status.get("fresh"):
                raise ValueError(f"cannot enroll {record.get('name') or selector}: {_ddm_unknown_reason(ddm_status)}")
            raise ValueError(f"{record.get('name') or selector} is not in any DDM server's inventory")
        result.setdefault("server", record.get("ddm_server_profile"))
        result.setdefault("device_id", record["ddm_device_id"])
    _apply_domain(result, domains)
    if not result.get("server") or not result.get("device_id"):
        raise ValueError("name the device, or give its DDM server and device_id")
    if result.get("action") == "enroll" and not result.get("domain_id"):
        raise ValueError("name the domain to enroll the device in")
    return result


def _route_line(subscription: dict) -> str:
    status = subscription.get("status") if isinstance(subscription.get("status"), dict) else {}
    return (
        f"{subscription.get('rx_channel')} <- {subscription.get('tx_device')}:{subscription.get('tx_channel')}: "
        f"{status.get('label') or 'unknown'}"
    )


BRIEF_RESULT_OMITTED = frozenset(
    {
        "device",
        "lock_state",
        "lock_state_code",
        "changed",
        "observation_source",
        "operation",
        "operation_availability",
        "status",
        "status_code",
        "success",
    }
)


def _result_payload(result: dict):
    text = (result.get("content") or [{}])[0].get("text")
    try:
        payload = json.loads(text) if text else None
    except ValueError:
        return text
    if isinstance(payload, dict) and not result.get("isError"):
        return {key: value for key, value in payload.items() if key not in BRIEF_RESULT_OMITTED} or None
    return payload


class DaemonMcpHandlers(McpPresetTools, McpHostAudioTools):
    mcp_token: str | None = None

    def mcp_server_info(self) -> dict:
        return {"path": MCP_PATH, "protocol_version": MCP_PROTOCOL_VERSION, "transport": "streamable-http"}

    @property
    def mcp_subscriptions(self) -> McpSubscriptions:
        subscriptions = getattr(self, "_mcp_subscriptions", None)
        if subscriptions is None:
            subscriptions = McpSubscriptions()
            self._mcp_subscriptions = subscriptions
        return subscriptions

    def _mcp_server_identity(self) -> dict:
        return {"name": "netaudio", "version": str(self.server_info.get("version", "unknown"))}

    def mcp_grant(self, headers) -> Grant | None:
        store: OAuthStore | None = getattr(self, "oauth_store", None)
        return grant_for_authorization((headers or {}).get("authorization"), self.mcp_token, store)

    async def _handle_mcp(self, method: str, body, writer, headers, reader=None) -> None:
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
        if is_modern_request(message, headers):
            await self._handle_modern_mcp(message, writer, reader, headers)
            return
        if isinstance(message, list):
            responses = [await self._mcp_message(item, writer) for item in message]
            responses = [response for response in responses if response is not None]
            self._write_mcp_raw(writer, 200 if responses else 202, responses or None)
            return
        response = await self._mcp_message(message, writer)
        self._write_mcp_raw(writer, 200 if response is not None else 202, response)

    async def _handle_modern_mcp(self, message, writer, reader, headers) -> None:
        rejection = validate_modern_request(message, headers)
        if rejection is not None:
            self._write_mcp_raw(writer, rejection.status, rejection.error)
            return
        request_id = message.get("id")
        method = message["method"]
        params = message.get("params") or {}
        if not isinstance(params, dict):
            self._write_mcp_raw(
                writer, 400, _error_response(request_id, JSON_RPC_INVALID_PARAMS, "params must be an object")
            )
            return
        if request_id is None:
            self._write_mcp_raw(writer, 202, None)
            return
        if method == "subscriptions/listen":
            requested = params.get("notifications")
            grant = getattr(self, "_mcp_current_grant", None)
            await self.mcp_subscriptions.serve(
                writer,
                reader,
                request_id,
                requested if isinstance(requested, dict) else {},
                grant.client_name if grant else "unknown client",
            )
            return
        try:
            if method == "server/discover":
                result = discover_result(MCP_INSTRUCTIONS)
            elif method in MODERN_METHODS:
                result = await self._mcp_call(method, params, writer)
            else:
                raise McpError(JSON_RPC_METHOD_NOT_FOUND, f"unknown method {method}")
        except McpError as exception:
            status = 404 if exception.code == JSON_RPC_METHOD_NOT_FOUND else 200
            self._write_mcp_raw(writer, status, _error_response(request_id, exception.code, str(exception)))
            return
        result = complete_result(method, result, self._mcp_server_identity())
        self._write_mcp_raw(writer, 200, {"jsonrpc": "2.0", "id": request_id, "result": result})

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
                "serverInfo": self._mcp_server_identity(),
                "instructions": MCP_INSTRUCTIONS,
            }
        if method == "ping":
            return {}
        if method == "tools/list":
            return {"tools": [tool.to_dict() for tool in PUBLIC_TOOLS]}
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

    def _unknown_tool(self, name) -> str:
        names = [tool.name for tool in TOOLS if tool.name not in {"discover_tools", "invoke_tool"}]
        names += list(PLANNED_BY_NAME)
        suggestions = close_matches(str(name), names)
        if not suggestions and isinstance(name, str):
            suggestions = [candidate.name for candidate in _discover_matches(name.replace("_", " "))[:3]]
        hint = f"; did you mean {' or '.join(suggestions)}?" if suggestions else "; use discover_tools to find it"
        return f"unknown operation {name!r}{hint}"

    async def _mcp_tool_call(self, params: dict, writer) -> dict:
        scope = REQUEST_INVENTORY.set({})
        try:
            return await self._mcp_tool_call_in_scope(params, writer)
        finally:
            REQUEST_INVENTORY.reset(scope)

    async def _mcp_tool_call_in_scope(self, params: dict, writer) -> dict:
        name = params.get("name")
        tool = TOOLS_BY_NAME.get(name) if isinstance(name, str) else None
        if tool is None and name in PLANNED_BY_NAME:
            return _tool_result(PLANNED_BY_NAME[name].not_implemented(), is_error=True)
        if tool is None:
            raise McpError(JSON_RPC_INVALID_PARAMS, self._unknown_tool(name))
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise McpError(JSON_RPC_INVALID_PARAMS, "arguments must be an object")
        problems = schema_errors(tool.input_schema, arguments, ignore_required=frozenset({"confirmed"}))
        if problems:
            return _tool_result(
                {
                    "error": f"invalid arguments for {tool.name}",
                    "problems": problems,
                    "input_schema": tool.input_schema,
                },
                is_error=True,
            )
        if tool.name == "discover_tools":
            return _tool_result(
                _discovery_result(
                    arguments.get("query", ""),
                    arguments.get("limit", 20),
                    arguments.get("offset", 0),
                    arguments.get("include_schema", False),
                ),
                is_error=False,
            )
        if tool.name == "invoke_tool":
            selected_name = arguments["name"]
            selected_arguments = arguments.get("arguments", {})
            if selected_name in {"discover_tools", "invoke_tool"}:
                return _tool_result(
                    {"error": "invoke_tool runs operations; call discover_tools directly"}, is_error=True
                )
            if selected_name not in TOOLS_BY_NAME and selected_name not in PLANNED_BY_NAME:
                return _tool_result({"error": self._unknown_tool(selected_name)}, is_error=True)
            return await self._mcp_tool_call({"name": selected_name, "arguments": selected_arguments}, writer)
        grant: Grant | None = getattr(self, "_mcp_current_grant", None)
        if grant is not None and not grant.can_write and not tool.read_only:
            return _tool_result(
                {
                    "error": f"{grant.client_name} was granted read-only access; {tool.name} needs the {SCOPE_WRITE} scope."
                },
                is_error=True,
            )
        if tool.name not in WARMUP_EXEMPT_TOOLS:
            await self._await_inventory(_request_selectors(arguments), writer)
        if tool.name == "apply_to_devices":
            return await self._apply_to_devices(arguments, writer)
        if tool.name in HOST_AUDIO_TOOL_NAMES or (tool.name == "get_signal_levels" and arguments.get("points")):
            try:
                payload, is_error = await self._host_audio_tool(
                    tool.name, arguments, writer, grant.client_name if grant else "unknown client"
                )
            except ValueError as exception:
                return _tool_result({"error": str(exception)}, is_error=True)
            return _tool_result(payload, is_error=is_error)
        if tool.name in PRESET_TOOLS:
            if arguments.get("confirmed") is True:
                logger.info("MCP %s called %s", grant.client_name if grant else "unknown client", tool.name)
            try:
                status, payload = await self._preset_tool(tool.name, arguments, writer)
            except ValueError as exception:
                return _tool_result({"error": str(exception)}, is_error=True)
            return _tool_result(compact(payload), is_error=status >= 400)
        request = {**tool.defaults, **arguments}
        scope = REQUEST_INVENTORY.get()
        if scope is not None and tool.read_only:
            scope["reuse"] = True
        records = self._serialized_devices()
        try:
            if tool.name == "set_ddm_enrollment":
                registry = getattr(self, "managed_inventory", None)
                request = _enrollment_request(
                    request,
                    records,
                    registry.domains() if registry is not None else [],
                    await self._ddm_status_snapshot(writer),
                )
            elif tool.name in {"update_ddm_domain", "select_ddm_context"}:
                registry = getattr(self, "managed_inventory", None)
                _apply_domain(request, registry.domains() if registry is not None else [])
                if tool.name == "update_ddm_domain" and not request.get("domain_id"):
                    raise ValueError("name the domain to rename or remove")
                if tool.name == "update_ddm_domain" and request.get("action") == "rename" and not request.get("name"):
                    raise ValueError("give the new name to rename the domain")
            elif tool.name in {"set_subscriptions", "subscribe", "unsubscribe"}:
                request = _route_request(request, records)
            elif tool.name in {"plan_transmit_flow", "create_transmit_flow", "delete_transmit_flow"}:
                record = _require_record(records, request["device"])
                request["device"] = record.get("server_name") or request["device"]
                flows = await self._fresh_transmit_flows(request["device"], writer)
                if flows is not None:
                    records = {**records}
                    key = next(key for key, value in records.items() if value is record)
                    record = records[key] = {**record, "transmitter_flows": flows}
                if "specification" in request:
                    request["specification"] = _flow_specification(request["specification"], record, flows or [])
            elif "device" in request or "rx_device" in request:
                selector_key = "device" if "device" in request else "rx_device"
                record = _record_for_selector(records, request[selector_key])
                if record is None and tool.name not in HISTORICAL_DEVICE_TOOLS:
                    raise ValueError(await self._device_not_found(records, request[selector_key], writer))
                if record is not None:
                    request[selector_key] = (
                        record.get("name") if tool.name in NAME_SELECTED_TOOLS else record.get("server_name")
                    ) or request[selector_key]
                if record is not None and "channel" in request and tool.name in CHANNEL_SELECTED_TOOLS:
                    request = _channel_request(tool.name, request, record)
        except (KeyError, TypeError, ValueError) as exception:
            return _tool_result({"error": str(exception)}, is_error=True)
        if tool.name == "get_signal_levels":
            try:
                period = signal_period(request, time.time())
            except ValueError as exception:
                return _tool_result({"error": str(exception)}, is_error=True)
            if period is not None:
                return _tool_result(await self._signal_history(request, records, period), is_error=False)
        if tool.name == "get_device_diagnostics" and not request.get("device"):
            return await self._network_diagnostics(records, writer)
        if tool.name == "get_device_diagnostics" and request.get("detail") != "debug":
            device = self._find_device(request["device"])
            if device is not None and getattr(self, "diagnostics", None) is not None:
                snapshot = self.diagnostics.diagnostics_snapshot(device, receiver_history=False, clock_history=False)
                return _tool_result(compact(diagnostics_view(snapshot, request, records)), is_error=False)
        if tool.name in {"plan_device_control", "apply_device_control"}:
            try:
                request = await self._device_control_request(request, writer)
            except ValueError as exception:
                return _tool_result({"error": str(exception)}, is_error=True)
            if tool.name == "apply_device_control" and request.get("confirmed") is not True:
                return await self._device_control_preview(request, writer)
        if tool.name == "create_transmit_flow" and request.get("confirmed") is not True:
            return await self._flow_preview(request, records, writer)
        if tool.name == "plan_transmit_flow":
            view, error = await self._flow_plan(request, records, writer)
            if view is None:
                return _tool_result(error, is_error=True)
            if view["supported"]:
                view["next"] = "create_transmit_flow with the same specification creates it"
            return _tool_result(compact(view), is_error=False)
        if tool.requires_confirmation and request.get("confirmed") is not True:
            return _tool_result(write_preview(tool.name, request, records, arguments), is_error=False)
        if tool.name in {"start_metering", "stop_metering"}:
            request["client_id"] = f"mcp:{grant.client_id if grant else 'unknown'}"
        if tool.name in {"get_network_overview", "list_devices"}:
            return await self._inventory_view(tool.name, request, writer)
        logger.info("MCP %s called %s", grant.client_name if grant else "unknown client", tool.name)
        if tool.name == "set_ddm_enrollment":
            return await self._enroll(request, records, writer)
        if tool.name == "forget_device":
            return await self._forget_device(request, records, writer)
        read_back = tool.name in READBACK_TOOLS or tool.name in ROUTE_TOOLS
        before = copy.deepcopy(records) if read_back else records
        captured = CapturedResponse(writer.get_extra_info("peername"))
        try:
            if tool.name == "get_signal_levels":
                await self._request_detailed_metering(request.get("device"))
            if tool.method == "GET":
                path = tool.path.format(**{key: quote(str(value), safe="") for key, value in request.items()})
                if tool.name == "get_device_diagnostics":
                    path += "?receiver_history=0"
                await self._dispatch("GET", path, None, captured, {"accept": "application/json"})
            else:
                path = tool.path
                if tool.name == "configure_flow_performance":
                    path = f"/set-{request.pop('kind')}-flow-performance"
                    if path == "/set-unicast-flow-performance":
                        path = "/set-unicast-performance"
                body = (
                    {key: value for key, value in request.items() if key != "detail"}
                    if tool.name == "inspect_device_controls"
                    else request
                )
                await self.post_handlers[path](captured, body)
        except TimeoutError:
            return _tool_result({"error": "device did not respond"}, is_error=True)
        except Exception as exception:
            logger.exception(f"MCP tool {tool.name} failed")
            return _tool_result({"error": str(exception)}, is_error=True)
        status, payload = captured.result()
        if status == 404 and isinstance(payload, dict) and payload.get("error") == "device not found":
            record = _record_for_selector(records, request.get("device") or request.get("rx_device"))
            if record is not None and record.get("online"):
                payload = {
                    "error": (
                        f"{record.get('name') or request.get('device')} is on the network but the server has not "
                        "finished connecting to it; try again in a few seconds"
                    )
                }
        if read_back:
            managed = any(
                (_record_for_selector(records, selector) or {}).get("management_state") == "managed"
                for selector in [request.get("device"), request.get("rx_device")]
                + [route.get("rx_device") for route in request.get("routes") or [] if isinstance(route, dict)]
                if selector
            )
            after = (
                await self._settled_records(tool.name, request, 10.0 if managed else 3.0, before)
                if status < 400
                else self._serialized_devices()
            )
            result, is_error = write_outcome(tool.name, request, before, after, status, payload)
            return _tool_result(compact(result), is_error=is_error)
        if tool.name == "set_sample_rate" and status >= 400:
            result, is_error = write_outcome(tool.name, request, before, before, status, payload)
            return _tool_result(compact(result), is_error=is_error)
        if payload is None:
            payload = {"status": status}
        elif status < 400 and tool.name == "get_device" and isinstance(payload, dict):
            ddm_known = ddm_inventory_known(await self._ddm_status_snapshot(writer))
            manufacturer = str(payload.get("manufacturer") or "")
            payload = device_view(payload, request["sections"], ddm_known, device_addresses(records), records)
            if "summary" in request["sections"] and "shure" in manufacturer.casefold():
                shure = CapturedResponse(writer.get_extra_info("peername"))
                await self._dispatch("GET", "/shure/devices", None, shure, {"accept": "application/json"})
                shure_status, shure_payload = shure.result()
                link = wireless_link(shure_payload, records, payload.get("name")) if shure_status < 400 else None
                if link:
                    payload["wireless"] = link
        elif tool.name == "inspect_device_controls" and status < 400:
            payload = controls_view(payload, request)
        elif tool.name == "plan_device_control":
            payload = control_plan_view(payload)
        elif tool.name == "apply_device_control":
            payload = control_apply_view(payload)
        elif tool.name == "get_device_diagnostics" and status < 400:
            payload = diagnostics_view(payload, request, records)
        elif tool.name == "get_wireless_devices" and status < 400:
            payload = wireless_view(payload, records)
        elif tool.name == "get_transmit_flows" and status < 400:
            payload = transmit_flows_view(
                payload, request, _record_for_selector(records, request["device"]), device_addresses(records)
            )
        elif tool.name in {"create_transmit_flow", "delete_transmit_flow"}:
            current = self._serialized_devices()
            payload = flow_result_view(
                payload, _record_for_selector(current, request["device"]), device_addresses(current)
            )
        elif tool.name == "get_issues" and status < 400:
            payload = issue_groups_view(payload, request, records)
        elif status < 400 and tool.name in TOOL_VIEWS and isinstance(payload, (dict, list)):
            payload = TOOL_VIEWS[tool.name](payload, request)
        if status < 400 and tool.name == "find_channels":
            payload = with_channel_signal(
                payload, self.metering.get_cached_levels_by_server() if self.metering else {}, records
            )
            if request.get("channel_type", "any") == "any":
                found = await self.host_audio_find(str(request.get("query") or ""))
                if found and isinstance(payload, dict):
                    payload["jack_pulseaudio_and_wireless"] = found
        if status < 400 and tool.name == "get_signal_levels":
            payload = name_meter_channels(payload, self._serialized_devices())
            off = off_devices(records)
            selected = _record_for_selector(records, request["device"]) if request.get("device") else None
            if selected is not None and last_seen_text(selected):
                payload = {
                    selected.get("name") or request["device"]: (
                        f"no levels: it is off the network, {last_seen_text(selected)}"
                    )
                }
            elif not request.get("device") and request.get("detail") != "debug":
                payload = network_levels_view(
                    {name: entry for name, entry in payload.items() if name not in off}
                    if isinstance(payload, dict)
                    else payload
                )
            elif request.get("device") and isinstance(payload, dict):
                if request.get("detail") != "debug":
                    for name, entry in payload.items():
                        if not isinstance(entry, dict):
                            continue
                        channels = (_record_for_selector(records, name) or {}).get("channels") or {}
                        for direction, key in (("rx", "receivers"), ("tx", "transmitters")):
                            if isinstance(entry.get(direction), dict):
                                entry[direction] = compact_channel_levels(entry[direction], channels.get(key))
                record = _record_for_selector(records, request["device"]) or {}
                if "shure" in str(record.get("manufacturer") or "").casefold() and record.get("name") in payload:
                    shure = CapturedResponse(writer.get_extra_info("peername"))
                    await self._dispatch("GET", "/shure/devices", None, shure, {"accept": "application/json"})
                    shure_status, shure_payload = shure.result()
                    link = wireless_link(shure_payload, records, record.get("name")) if shure_status < 400 else None
                    if link:
                        payload[record["name"]]["wireless"] = link
        elif status < 400 and tool.name == "get_server_info" and isinstance(payload, dict):
            payload = {**payload, "now": utc_now()}
        elif status < 400 and tool.name == "get_event_journal" and isinstance(payload, dict):
            payload = {
                **payload,
                "recorded_since": self.server_info.get("started_at"),
                "retention": "kept in memory; the journal starts empty whenever the server restarts",
            }
        if request.get("detail") == "debug" or "full" in (request.get("sections") or ()):
            payload = hexadecimal_byte_fields(payload)
        payload = compact(payload)
        if payload == {} and tool.name in EMPTY_RESULTS:
            payload = {"result": EMPTY_RESULTS[tool.name]}
        if tool.name == "get_issues" and status < 400 and isinstance(payload, dict) and not request.get("device"):
            conditions = await self._host_conditions()
            if conditions:
                payload["host_and_wireless"] = conditions
        return _tool_result(payload, is_error=status >= 400)

    async def _fresh_transmit_flows(self, server_name: str, writer) -> list | None:
        captured = CapturedResponse(writer.get_extra_info("peername"))
        await self._dispatch(
            "GET", f"/transmit-flows/{quote(server_name, safe='')}", None, captured, {"accept": "application/json"}
        )
        status, payload = captured.result()
        if status >= 400 or not isinstance(payload, dict):
            logger.warning("MCP could not read transmit flows for %s: %s", server_name, payload)
            return None
        return [flow.get("raw_fields") or flow for flow in payload.get("flows") or [] if isinstance(flow, dict)]

    async def _stream_health(self, records: dict, writer) -> tuple[list, list]:
        devices, problems = [], []
        for record in sorted(
            (record for record in records.values() if isinstance(record, dict) and record.get("online")),
            key=lambda item: str(item.get("name")),
        ):
            device = self.application.devices.get(record.get("server_name")) or self._direct_device_for_record(record)
            if device is not None and getattr(self, "diagnostics", None) is not None:
                status, payload = (
                    200,
                    self.diagnostics.diagnostics_snapshot(device, receiver_history=False, clock_history=False),
                )
            else:
                status, payload = 404, {"error": "no diagnostics for this device"}
            name = record.get("name")
            if status >= 400 or not isinstance(payload, dict):
                devices.append(
                    {
                        "device": name,
                        "unavailable": (payload or {}).get("error") if isinstance(payload, dict) else status,
                    }
                )
                continue
            view = diagnostics_view(payload, {}, records)
            flows = view.get("receive_flows") if isinstance(view.get("receive_flows"), list) else []
            headroom = [
                (flow["latency_ms"]["max"], flow["latency_budget_ms"])
                for flow in flows
                if isinstance(flow.get("latency_ms"), dict)
                and isinstance(flow["latency_ms"].get("max"), (int, float))
                and flow.get("latency_budget_ms")
            ]
            worst = max(headroom, key=lambda pair: pair[0] / pair[1], default=None)
            clock = view.get("clock") or {}
            role = clock.get("role")
            if role == "Leader":
                clock_text = "Leader"
            elif role or clock.get("servo") or clock.get("state"):
                clock_text = ", ".join(str(part) for part in (role, clock.get("servo") or clock.get("state")) if part)
            else:
                clock_text = "not reported"
            measured: object = len(flows)
            if not flows and _subscription_list(record):
                measured = (
                    "not measured yet"
                    if isinstance(payload.get("receiver"), dict)
                    else "not observed: the server sees no receive telemetry from this device"
                )
            devices.append(
                compact(
                    {
                        "device": name,
                        "receive_flows": measured,
                        "worst_latency_ms": worst[0] if worst else None,
                        "latency_budget_ms": worst[1] if worst else None,
                        "late_packets_since_watching": sum(
                            flow.get("late_packets_since_watching") or 0 for flow in flows
                        ),
                        "clock": clock_text,
                    }
                )
            )
            problems.extend(
                f"{name} flow {flow.get('flow')}: {flow['problem']}" for flow in flows if flow.get("problem")
            )
        return devices, problems

    async def _network_diagnostics(self, records: dict, writer) -> dict:
        devices, problems = await self._stream_health(records, writer)
        return _tool_result(
            compact(
                {
                    "devices": devices,
                    "problems": problems
                    or ["none: no late packets since the server started and every flow is within its latency budget"],
                    "detail": "get_device_diagnostics with a device shows its flows",
                }
            ),
            is_error=False,
        )

    async def _flow_plan(self, request: dict, records: dict, writer) -> tuple[dict | None, dict]:
        record = _record_for_selector(records, request["device"]) or {}
        status, plan = await self._post_captured(
            "/transmit-flows/plan", {"device": request["device"], "specification": request["specification"]}, writer
        )
        if status >= 400 or not isinstance(plan, dict):
            return None, compact(plan) if isinstance(plan, dict) else {"error": f"plan failed ({status})"}
        details = plan.get("plan") if isinstance(plan.get("plan"), dict) else {}
        names = {int(key): value for key, value in _channel_names(record, "transmitters").items()}
        problems = []
        for reason in details.get("reasons") or []:
            problems.append(reason)
        view = {
            "device": record.get("name") or request["device"],
            "supported": details.get("supported"),
            "flow": transmit_flow_view(request["specification"], names, {}),
            "problems": problems or None,
        }
        return view, {}

    async def _device_control_request(self, request: dict, writer) -> dict:
        category = request.get("category")
        requested = request.get("requested")
        if category == "bluetooth_discovery" and not isinstance(requested, bool):
            word = str(requested).strip().casefold()
            if word in DISCOVERABLE_WORDS:
                requested = True
            elif word in NOT_DISCOVERABLE_WORDS:
                requested = False
            else:
                raise ValueError('bluetooth_discovery takes "discoverable" or "not discoverable"')
        elif category == "bluetooth_pairing" and isinstance(requested, str):
            if requested.strip().casefold() in CLEAR_PAIRING_WORDS:
                requested = "clear"
        elif category == "bluetooth_identification":
            if isinstance(requested, str):
                requested = (
                    {"name_source": 1}
                    if requested.strip().casefold() in DANTE_NAME_WORDS
                    else {"name_source": 2, "custom_name": requested}
                )
            if isinstance(requested, dict) and "name_source" not in requested and "custom_name" in requested:
                requested = {**requested, "name_source": 2}
            if isinstance(requested, dict) and requested.get("name_source") == 1 and "custom_name" not in requested:
                requested = {**requested, "custom_name": await self._current_bluetooth_name(request["device"], writer)}
        return {**request, "requested": requested}

    async def _current_bluetooth_name(self, device: str, writer) -> str:
        captured = CapturedResponse(writer.get_extra_info("peername"))
        await self.post_handlers["/device-controls"](captured, {"device": device, "action": "inspect"})
        status, payload = captured.result()
        if status >= 400 or not isinstance(payload, dict):
            raise ValueError("the device's current Bluetooth name could not be read")
        observation = (payload.get("observations") or {}).get("bluetooth_identification") or {}
        value = observation.get("value") if isinstance(observation, dict) else None
        return str((value or {}).get("custom_name") or "")

    async def _device_control_preview(self, request: dict, writer) -> dict:
        captured = CapturedResponse(writer.get_extra_info("peername"))
        await self.post_handlers["/device-controls"](
            captured,
            {key: value for key, value in request.items() if key != "confirmed"} | {"action": "plan"},
        )
        status, payload = captured.result()
        if status >= 400 or not isinstance(payload, dict):
            return _tool_result(payload if isinstance(payload, dict) else {"error": str(payload)}, is_error=True)
        plan = control_plan_view(payload)
        action = payload.get("action")
        preview = {
            "changed": False,
            "confirmation_required": action == "change",
            "operation": "apply_device_control",
            "device": request.get("device"),
            "change": {"from": plan.get("from"), "to": plan.get("to")} if action == "change" else None,
            "result": plan.get("result"),
            "reason": plan.get("reason"),
            "next": NEXT_STEP if action == "change" else None,
        }
        return _tool_result(compact(preview), is_error=action in {"unsupported", "unavailable"})

    async def _flow_preview(self, request: dict, records: dict, writer) -> dict:
        view, error = await self._flow_plan(request, records, writer)
        if view is None:
            return _tool_result(error, is_error=True)
        preview = {
            "changed": False,
            "confirmation_required": True,
            "operation": "create_transmit_flow",
            "device": view["device"],
            "change": {"new flow": view["flow"]},
            "supported": view["supported"],
            "warnings": [f"the server will refuse this: {problem}" for problem in view["problems"] or []] or None,
            "next": (
                "Nothing has changed. Tell the user what this will do; after they agree, call again with the same "
                "arguments and confirmed=true."
            ),
        }
        return _tool_result(compact(preview), is_error=False)

    def _server_age(self) -> float | None:
        started = str(self.server_info.get("started_at") or "")
        try:
            moment = datetime.fromisoformat(started.replace("Z", "+00:00"))
        except ValueError:
            return None
        return (datetime.now(timezone.utc) - moment).total_seconds()

    async def _await_inventory(self, selectors: list[str], writer) -> None:
        age = self._server_age()
        if age is None or age >= WARMUP_SECONDS:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + WARMUP_SECONDS - age
        changed = asyncio.Event()

        async def on_event(event) -> None:
            changed.set()

        dispatcher = self.application.dispatcher
        for kind in (EventType.DEVICE_DISCOVERED, EventType.DEVICE_UPDATED):
            dispatcher.on(kind, on_event)
        registry = getattr(self, "managed_inventory", None)
        services = list(registry.services.values()) if registry is not None and registry.enabled else []
        try:
            while (remaining := deadline - loop.time()) > 0:
                records = self._serialized_devices()
                if selectors:
                    found = [_found_record(records, selector) for selector in selectors]
                    if all(record is True or (record and _record_ready(record)) for record in found):
                        return
                elif all(_record_ready(record) for record in records.values() if isinstance(record, dict)):
                    if ddm_inventory_known(await self._ddm_status_snapshot(writer)):
                        return
                changed.clear()
                waits = [asyncio.ensure_future(changed.wait())]
                waits.extend(asyncio.ensure_future(service.wait_for_change(remaining)) for service in services)
                _, pending = await asyncio.wait(waits, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
        finally:
            for kind in (EventType.DEVICE_DISCOVERED, EventType.DEVICE_UPDATED):
                dispatcher.off(kind, on_event)

    async def _settled_records(
        self, name: str, request: dict, timeout: float = 3.0, before: dict | None = None
    ) -> dict:
        records = self._serialized_devices()
        if write_settled(name, request, records, before):
            return records
        changed = asyncio.Event()

        async def on_update(event) -> None:
            changed.set()

        dispatcher = self.application.dispatcher
        dispatcher.on(EventType.DEVICE_UPDATED, on_update)
        registry = getattr(self, "managed_inventory", None)
        services = list(registry.services.values()) if registry is not None and registry.enabled else []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            while (remaining := deadline - loop.time()) > 0:
                changed.clear()
                waits = [asyncio.ensure_future(changed.wait())]
                waits.extend(asyncio.ensure_future(service.wait_for_change(remaining)) for service in services)
                done, pending = await asyncio.wait(waits, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                if not done:
                    break
                records = self._serialized_devices()
                if write_settled(name, request, records, before):
                    break
        finally:
            dispatcher.off(EventType.DEVICE_UPDATED, on_update)
        return records

    async def _post_captured(self, path: str, request: dict, writer) -> tuple[int, Any]:
        captured = CapturedResponse(writer.get_extra_info("peername"))
        await self.post_handlers[path](captured, request)
        return captured.result()

    def _ddm_record(self, request: dict) -> list[dict]:
        return [
            record
            for record in self._serialized_devices().values()
            if isinstance(record, dict)
            and record.get("ddm_server_profile") == request["server"]
            and record.get("ddm_device_id") == request["device_id"]
        ]

    async def _await_enrollment(self, request: dict, domain_id: str | None, timeout: float = 30.0) -> None:
        registry = getattr(self, "managed_inventory", None)
        service = registry.services.get(request["server"]) if registry is not None else None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while service is not None:
            matches = self._ddm_record(request)
            if len(matches) == 1 and matches[0].get("online") and matches[0].get("ddm_domain_id") == domain_id:
                return
            remaining = deadline - loop.time()
            if remaining <= 0 or not await service.wait_for_change(remaining):
                return

    async def _enroll(self, request: dict, records: dict, writer) -> dict:
        record = next(
            (
                candidate
                for candidate in records.values()
                if isinstance(candidate, dict)
                and candidate.get("ddm_device_id") == request.get("device_id")
                and candidate.get("ddm_server_profile") in {None, request.get("server")}
            ),
            None,
        )
        current = (
            (record.get("ddm_domain_name") or record.get("ddm_domain_id"))
            if record is not None and record.get("management_state") == "managed"
            else None
        )
        moved_from = None
        if (
            request.get("action") == "enroll"
            and current is not None
            and record.get("ddm_domain_id") != request.get("domain_id")
        ):
            status, payload = await self._post_captured(
                "/ddm/enrollment",
                {"server": request["server"], "device_id": request["device_id"], "action": "unenroll"},
                writer,
            )
            if status >= 400:
                return _tool_result(
                    {"error": f"could not take the device out of {current}", "detail": payload}, is_error=True
                )
            moved_from = current
            await self._await_enrollment(request, None)
        status, payload = await self._post_captured("/ddm/enrollment", request, writer)
        if status >= 400:
            error = {"error": "enrollment failed", "detail": payload}
            if moved_from is not None:
                error["error"] = (
                    f"left {moved_from}, but enrolling in the new domain failed; the device is now unmanaged"
                )
            return _tool_result(error, is_error=True)
        result = payload if isinstance(payload, dict) else {"accepted": True}
        if moved_from is not None:
            result = {**result, "moved_from": moved_from}
        await self._await_enrollment(request, request.get("domain_id") if request.get("action") == "enroll" else None)
        return _tool_result(compact({**result, **self._enrollment_readback(request)}), is_error=False)

    def _enrollment_readback(self, request: dict) -> dict:
        matches = self._ddm_record(request)
        if len(matches) != 1:
            return {"readback": "the refreshed inventory does not show the device once yet; check list_devices"}
        record = matches[0]
        managed = record.get("management_state") == "managed"
        routes = [
            _route_line(subscription)
            for subscription in (record.get("subscriptions") or [])
            if isinstance(subscription, dict) and subscription.get("tx_device")
        ]
        return {
            "device": record.get("name"),
            "now_in": (record.get("ddm_domain_name") or record.get("ddm_domain_id"))
            if managed
            else "no domain (unmanaged)",
            "routes": routes or None,
            "note": "Routes and clocking can take several seconds to settle after a domain change; recheck get_routing if any show a problem.",
        }

    async def _device_not_found(self, records: dict, selector, writer) -> str:
        captured = CapturedResponse(writer.get_extra_info("peername"))
        await self._dispatch("GET", "/event-journal", None, captured, {"accept": "application/json"})
        status, journal = captured.result()
        if status < 400:
            for name, rename in renamed_devices(records, journal).items():
                if name.casefold() == str(selector).casefold():
                    return f"{name} was renamed to {rename['renamed_to']} at {rename['renamed_at']}; use {rename['renamed_to']}"
            for entry in missing_devices(records, journal):
                if str(entry["name"]).casefold() == str(selector).casefold():
                    when = (
                        f"last seen {entry['disappeared_at']}"
                        if entry.get("disappeared_at")
                        else "not seen since the server started"
                    )
                    message = (
                        f"{entry['name']} is off the network ({when}), so its own routes, settings and levels are "
                        "not known"
                    )
                    if entry.get("waiting_routes"):
                        message += f"; routes on other devices that wait for it: {entry['waiting_routes']}"
                    if entry.get("routes_still_connected"):
                        message += (
                            f". {entry['routes_still_connected']} route(s) to it are still receiving audio, so it was "
                            "most likely renamed; list_devices shows the current names"
                        )
                    return message
        return _device_not_found(records, selector)

    async def _inventory_view(self, name: str, request: dict, writer) -> dict:
        snapshots = {}
        for key, path in (
            ("devices", "/devices"),
            ("issues", "/issues"),
            ("ddm", "/ddm/status"),
            ("journal", "/event-journal"),
        ):
            if name == "list_devices" and key in {"issues", "journal"}:
                continue
            captured = CapturedResponse(writer.get_extra_info("peername"))
            try:
                await self._dispatch("GET", path, None, captured, {"accept": "application/json"})
            except Exception as exception:
                return _tool_result({"error": f"{key} read failed: {exception}"}, is_error=True)
            status, snapshot = captured.result()
            if status >= 400 or not isinstance(snapshot, dict):
                return _tool_result({"error": f"{key} read failed", "status": status}, is_error=True)
            snapshots[key] = snapshot
        if name == "list_devices":
            ddm_known = ddm_inventory_known(snapshots["ddm"])
            return _tool_result(device_list_view(snapshots["devices"], ddm_known), is_error=False)
        view = network_overview_view(**snapshots)
        view.update(await self._host_overview())
        devices, problems = await self._stream_health(snapshots["devices"], writer)
        worst = [
            (entry["worst_latency_ms"] / entry["latency_budget_ms"], entry)
            for entry in devices
            if entry.get("worst_latency_ms") and entry.get("latency_budget_ms")
        ]
        top = max(worst, key=lambda pair: pair[0], default=None)
        view["streams"] = compact(
            {
                "late_packets_since_server_start": sum(
                    entry.get("late_packets_since_watching") or 0 for entry in devices
                ),
                "worst_latency": (
                    f"{top[1]['worst_latency_ms']} of {top[1]['latency_budget_ms']} ms ({top[1]['device']})"
                    if top
                    else None
                ),
                "problems": problems or None,
                "detail": "get_device_diagnostics",
            }
        )
        return _tool_result(view, is_error=False)

    async def _forget_device(self, request: dict, records: dict, writer) -> dict:
        captured = CapturedResponse(writer.get_extra_info("peername"))
        if request.get("every_device_off"):
            path = "/devices?selection=offline"
        elif request.get("device"):
            record = _record_for_selector(records, request["device"])
            if record is None:
                message = await self._device_not_found(records, request["device"], writer)
                return _tool_result({"error": message}, is_error=True)
            if not is_off(record):
                return _tool_result(
                    {"error": f"{record.get('name')} is on the network; only a device that is off can be forgotten"},
                    is_error=True,
                )
            path = f"/devices/{quote(str(record.get('server_name') or record.get('name')), safe='')}"
        else:
            return _tool_result({"error": "give a device, or every_device_off=true"}, is_error=True)
        await self._dispatch("DELETE", path, None, captured, {"accept": "application/json"})
        status, payload = captured.result()
        if status >= 400:
            return _tool_result(payload, is_error=True)
        forgotten = [
            entry.get("name") or entry.get("server_name")
            for entry in (payload or {}).get("forgotten") or ()
            if isinstance(entry, dict)
        ]
        sources = routes_by_source(records)
        routes = {name: receive_channels_text(sources[name]) for name in forgotten if name in sources}
        return _tool_result(
            compact(
                {
                    "forgotten": forgotten or None,
                    "result": None if forgotten else "nothing was off the network",
                    "routes_still_pointing_at_them": routes or None,
                    "note": "a forgotten device comes back by itself if it returns to the network; "
                    "set_subscriptions clears routes that are no longer wanted",
                }
            ),
            is_error=False,
        )

    async def _ddm_status_snapshot(self, writer) -> dict:
        captured = CapturedResponse(writer.get_extra_info("peername"))
        await self._dispatch("GET", "/ddm/status", None, captured, {"accept": "application/json"})
        status, snapshot = captured.result()
        return snapshot if status < 400 and isinstance(snapshot, dict) else {"enabled": True, "fresh": False}

    async def _apply_to_devices(self, arguments: dict, writer) -> dict:
        name = arguments["operation"]
        if name in PLANNED_BY_NAME:
            return _tool_result(PLANNED_BY_NAME[name].not_implemented(), is_error=True)
        operation = TOOLS_BY_NAME.get(name) if isinstance(name, str) else None
        if (
            operation is None
            or operation.read_only
            or operation.name == "apply_to_devices"
            or "device" not in operation.input_schema["properties"]
        ):
            return _tool_result({"error": f"{name!r} is not a write operation that takes a device"}, is_error=True)
        shared = arguments.get("arguments") or {}
        where = arguments["where"]
        if not isinstance(shared, dict) or not isinstance(where, dict):
            return _tool_result({"error": "where and arguments must be objects"}, is_error=True)
        per_device = sorted(set(shared) & {"device", "confirmed"})
        problems = [f"{key} belongs outside arguments" for key in per_device]
        problems += schema_errors(
            operation.input_schema,
            {
                **{key: value for key, value in shared.items() if key not in per_device},
                "device": "each matching device",
            },
            "arguments",
            ignore_required=frozenset({"confirmed"}),
        )
        if problems:
            return _tool_result(
                {
                    "error": f"arguments do not match {name}",
                    "problems": problems,
                    "input_schema": operation.input_schema,
                },
                is_error=True,
            )
        conditions = TOOLS_BY_NAME["apply_to_devices"].input_schema["properties"]["where"]["properties"]
        unknown_conditions = sorted(set(where) - set(conditions))
        if unknown_conditions:
            return _tool_result({"error": f"unknown conditions: {', '.join(unknown_conditions)}"}, is_error=True)
        try:
            selection = _select_devices(
                self._serialized_devices(), where, name, await self._ddm_status_snapshot(writer)
            )
        except ValueError as exception:
            return _tool_result({"error": str(exception)}, is_error=True)
        token = _batch_token(name, shared, selection["selected"])
        preview = {
            "operation": name,
            "devices": [record.get("name") or record.get("server_name") for record in selection["selected"]],
            "skipped": selection["skipped"],
            "undetermined": selection["undetermined"],
            "token": token,
        }
        supplied = arguments.get("token")
        if supplied is None:
            records = self._serialized_devices()
            changes = []
            for record in selection["selected"]:
                single = write_preview(name, {**shared, "device": record.get("server_name")}, records)
                changes.append(
                    compact(
                        {
                            "device": record.get("name") or record.get("server_name"),
                            "change": single.get("change"),
                            "effect": single.get("effect"),
                            "warnings": single.get("warnings"),
                        }
                    )
                )
            return _tool_result(
                compact(
                    {
                        "preview": True,
                        **preview,
                        "changes": changes,
                        "next": (
                            "Nothing has changed. Tell the user what this will do; after they agree, call again with "
                            f"the same arguments plus token={token!r} and confirmed=true."
                        ),
                    }
                ),
                is_error=False,
            )
        if supplied != token:
            return _tool_result(
                {
                    "error": "the matching devices or arguments changed since the preview; review this preview",
                    **preview,
                },
                is_error=True,
            )
        if arguments.get("confirmed") is not True:
            return _tool_result(
                {"error": f"confirm {name} on these devices with the user and call again with confirmed set to true"},
                is_error=True,
            )
        results = []
        for record in selection["selected"]:
            result = await self._mcp_tool_call(
                {"name": name, "arguments": {**shared, "device": record.get("server_name"), "confirmed": True}}, writer
            )
            results.append(
                {
                    "device": record.get("name") or record.get("server_name"),
                    "ok": not result.get("isError"),
                    "result": _result_payload(result),
                }
            )
        succeeded = sum(1 for entry in results if entry["ok"])
        return _tool_result(
            {
                "operation": name,
                "succeeded": succeeded,
                "failed": len(results) - succeeded,
                "results": results,
                "skipped": selection["skipped"],
                "undetermined": selection["undetermined"],
            },
            is_error=bool(results) and succeeded == 0,
        )

    async def _signal_history(self, request: dict, records: dict, period: tuple[float, float]) -> dict:
        selected = _record_for_selector(records, request["device"]) if request.get("device") else None
        if period[1] > time.time():
            await self._request_detailed_metering(request.get("device"))
            await asyncio.sleep(period[1] - time.time() + HISTORY_FLUSH_SECONDS)
        devices = (
            [selected]
            if selected is not None
            else [record for record in records.values() if isinstance(record, dict) and record.get("online")]
        )
        return compact(signal_history_view(self.level_history, devices, period, full=selected is not None))

    async def _request_detailed_metering(self, selector: str | None) -> None:
        if not self.metering:
            return
        devices = self.application.devices
        targets = [
            server_name
            for server_name in detailed_metering_targets(devices)
            if not getattr(devices[server_name], "requires_managed_control", False)
        ]
        if selector is not None:
            record = _record_for_selector(self._serialized_devices(), selector)
            selected = record.get("server_name") if record else None
            targets = [server_name for server_name in targets if server_name == selected]
        await asyncio.gather(*(self.metering.snapshot(server_name) for server_name in targets))

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
            400: "Bad Request",
            401: "Unauthorized",
            404: "Not Found",
            405: "Method Not Allowed",
        }.get(status, "Error")
        head = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n"
        )
        if status == 401:
            challenge = 'Bearer realm="netaudio"'
            if resource_metadata:
                challenge += f', resource_metadata="{resource_metadata}"'
            head += f"WWW-Authenticate: {challenge}\r\n"
        writer.write(head.encode() + b"\r\n" + body)


def _error_response(request_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _tool_result(payload, *, is_error: bool) -> dict:
    return {
        "content": [
            {"type": "text", "text": json.dumps(payload, default=str, separators=(",", ":"), ensure_ascii=False)}
        ],
        "isError": is_error,
    }
