from __future__ import annotations

import logging
import re
import time
from typing import Any

from netaudio.daemon.http.host_audio import SUMMARY_PEER_TIMEOUT_SECONDS
from netaudio.daemon.http.host_audio_control import card_change, jack_change, pulse_change
from netaudio.daemon.http.mcp_preview import NEXT_STEP
from netaudio.daemon.http.mcp_schema import object_schema
from netaudio.daemon.http.mcp_signal_history import signal_period
from netaudio.daemon.http.mcp_views import compact
from netaudio.host_audio.alsa import is_dante_interface
from netaudio.host_audio.links import bridge_channel, hardware_channel

logger = logging.getLogger("netaudio.mcp")

LOW_BATTERY_PERCENT = 25
LOW_BATTERY_MINUTES = 60
RECENT_CHANGES = 8
SECTIONS = ["summary", "connections", "ports", "pulse", "cards", "changes"]
POINT_DESCRIPTION = (
    "Where to start: a Shure channel (AD4D-A ch1), a Dante channel (lx-dante rx 1), a JACK port or alias "
    "(system:capture_1), a PulseAudio input or output (jack_in, default input), or an application (Discord). "
    "Points on this server's computer need no prefix; put another computer's name first for its points, such as "
    "workstation default input."
)
TARGET_DESCRIPTION = (
    "A PulseAudio output or input by name (jack_out, default output, default input), or an application's "
    "playback or recording stream by application name or stream number."
)

HOST_AUDIO_TOOL_SPECS: tuple[dict, ...] = (
    {
        "name": "trace_signal",
        "path": "/host-audio/trace",
        "method": "GET",
        "read_only": True,
        "description": (
            "Follow audio through the whole chain from any point: Shure wireless channels, Dante channels and "
            "routes, this computer's sound cards, JACK ports and connections, and PulseAudio inputs, outputs and "
            "application streams, on this computer and on other computers running netaudio. Lists every step "
            "upstream and downstream with its level, all measured at the same moment, and says where signal stops. "
            "Use it to answer why something can't be heard."
        ),
        "input_schema": object_schema(
            {
                "point": {"type": "string", "description": POINT_DESCRIPTION},
                "direction": {"type": "string", "enum": ["both", "upstream", "downstream"], "default": "both"},
                "levels": {
                    "type": "boolean",
                    "default": True,
                    "description": "Measure levels at each step; about one second.",
                },
            },
            ["point"],
        ),
    },
    {
        "name": "get_host_audio",
        "path": "/host-audio",
        "method": "GET",
        "read_only": True,
        "description": (
            "This computer's audio: the JACK server (rate, buffer, xruns, clients, ports and connections), "
            "PulseAudio outputs, inputs and application streams, sound cards and which Dante device each card "
            "is, and recent changes. Pick a section; client narrows connections and ports to one JACK client."
        ),
        "input_schema": object_schema(
            {
                "section": {"type": "string", "enum": SECTIONS, "default": "summary"},
                "client": {"type": "string", "description": "A JACK client name, such as system or jack_out."},
                "host": {
                    "type": "string",
                    "description": "Another computer running netaudio, such as workstation; defaults to this server's computer.",
                },
            },
            [],
        ),
    },
    {
        "name": "connect_audio_ports",
        "path": "/host-audio/jack/connect",
        "description": (
            "Connect a JACK output port to an input port on this computer, such as a sound card capture channel "
            "to an application or a PulseAudio input. Ports accept names or aliases. The result reads the "
            "graph back and says if another JACK client undid the change."
        ),
        "input_schema": object_schema(
            {
                "source": {"type": "string", "description": "Output port, such as system:capture_1."},
                "destination": {"type": "string", "description": "Input port, such as jack_in:front-left."},
            },
            ["source", "destination"],
        ),
    },
    {
        "name": "disconnect_audio_ports",
        "path": "/host-audio/jack/disconnect",
        "description": "Disconnect a JACK output port from an input port on this computer.",
        "input_schema": object_schema(
            {
                "source": {"type": "string", "description": "Output port."},
                "destination": {"type": "string", "description": "Input port."},
            },
            ["source", "destination"],
        ),
        "destructive": True,
    },
    {
        "name": "set_audio_volume",
        "path": "/host-audio/pulse/volume",
        "description": "Set the volume of a PulseAudio output, input or application stream on this computer.",
        "input_schema": object_schema(
            {
                "target": {"type": "string", "description": TARGET_DESCRIPTION},
                "volume_percent": {"type": "number", "minimum": 0, "maximum": 150},
                "volume_db": {"type": "number", "maximum": 11},
            },
            ["target"],
        ),
    },
    {
        "name": "set_audio_mute",
        "path": "/host-audio/pulse/mute",
        "description": "Mute or unmute a PulseAudio output, input or application stream on this computer.",
        "input_schema": object_schema(
            {"target": {"type": "string", "description": TARGET_DESCRIPTION}, "mute": {"type": "boolean"}},
            ["target", "mute"],
        ),
    },
    {
        "name": "set_default_audio_device",
        "path": "/host-audio/pulse/default",
        "description": "Make a PulseAudio output or input the default for new application streams on this computer.",
        "input_schema": object_schema(
            {"target": {"type": "string", "description": "A PulseAudio output or input name."}},
            ["target"],
        ),
    },
    {
        "name": "move_audio_stream",
        "path": "/host-audio/pulse/move",
        "description": "Move an application's playback stream to another PulseAudio output, or a recording stream to another input.",
        "input_schema": object_schema(
            {
                "stream": {"type": "string", "description": "Application name or stream number."},
                "destination": {"type": "string", "description": "PulseAudio output or input name."},
            },
            ["stream", "destination"],
        ),
    },
    {
        "name": "link_audio_card",
        "path": "/host-audio/cards",
        "description": (
            "Record which Dante device a sound card on this computer is, such as a Dante PCIe card or an AVIO "
            "USB adapter, so traces continue from the card's channels into Dante. Leave device empty to remove "
            "the link. Saved in the server's configuration."
        ),
        "input_schema": object_schema(
            {
                "card": {"type": "string", "description": "ALSA card ID from get_host_audio section=cards."},
                "device": {"type": "string", "description": "Dante device name; empty removes the link."},
            },
            ["card"],
        ),
    },
    {
        "name": "set_wireless_value",
        "path": "/shure/set",
        "description": (
            "Change a Shure wireless setting and read it back: channel name, gain (AD4D 0-60, where 18 is 0 dB), "
            "mute, frequency in MHz, group and channel, identify; on a P10T also rf_mute, rf_power, input_level "
            "and transmit_mode; device_name renames the unit."
        ),
        "input_schema": object_schema(
            {
                "device": {"type": "string", "description": "Shure device name, IP or MAC, from get_wireless_devices."},
                "channel": {
                    "type": ["integer", "string"],
                    "description": "Channel number or name; omit for device settings.",
                },
                "setting": {
                    "type": "string",
                    "description": "name, gain, mute, frequency, group_channel, identify, rf_mute, rf_power, input_level, transmit_mode, device_name, or a Shure command key.",
                },
                "value": {"type": ["string", "number", "boolean"]},
            },
            ["device", "setting", "value"],
        ),
    },
)
HOST_AUDIO_TOOL_NAMES = frozenset(spec["name"] for spec in HOST_AUDIO_TOOL_SPECS)
WRITE_PATHS = {spec["name"]: spec["path"] for spec in HOST_AUDIO_TOOL_SPECS if not spec.get("read_only")}
PULSE_ACTIONS = {
    "set_audio_volume": "volume",
    "set_audio_mute": "mute",
    "set_default_audio_device": "default",
    "move_audio_stream": "move",
}


def _port_label(name: str, port: dict) -> str:
    aliases = [alias for alias in port.get("aliases") or () if not alias.startswith("alsa_pcm:")]
    return name + (f" [{', '.join(aliases)}]" if aliases else "")


def _natural(value: str) -> list:
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", value)]


def _jack_line(jack: dict) -> str:
    if not jack.get("available"):
        return f"not available: {jack.get('reason')}"
    rate, frames = jack.get("sample_rate"), jack.get("buffer_size")
    latency = f" ({frames / rate * 1000:.1f} ms)" if rate and frames else ""
    clients = len({port["client"] for port in (jack.get("ports") or {}).values()})
    xruns = jack.get("xruns") or 0
    xrun_text = f"{xruns} xrun{'' if xruns == 1 else 's'} since {jack.get('xruns_since')}" + (
        f", last at {jack.get('last_xrun')}" if jack.get("last_xrun") else ""
    )
    return f"{rate / 1000:g} kHz, {frames} frames{latency}, DSP load {jack.get('cpu_load')}%, {clients} clients, {xrun_text}"


def _stream_line(kind: str, entry: dict) -> str:
    application = entry.get("application") or entry.get("binary") or "unknown application"
    device = entry.get("sink") if kind == "playback" else entry.get("source")
    arrow = "→" if kind == "playback" else "←"
    states = []
    if entry.get("corked"):
        states.append("paused")
    if entry.get("mute"):
        states.append("muted")
    volume = entry.get("volume_percent") or []
    if volume:
        states.append(f"{'/'.join(str(value) for value in volume)}%")
    media = f" “{entry['media']}”" if entry.get("media") and entry.get("media") != application else ""
    return f"{application}{media} {kind} stream {entry['index']} {arrow} {device} ({', '.join(states)})"


def _pulse_line(pulse: dict) -> str:
    if not pulse.get("available"):
        return f"not available: {pulse.get('reason')}"
    server = pulse.get("server") or {}
    playing = len(pulse.get("sink_inputs") or [])
    recording = len(pulse.get("source_outputs") or [])
    return (
        f"{server.get('name')} {server.get('version')}; default output {server.get('default_sink')}, default input "
        f"{server.get('default_source')}; {playing} playback and {recording} recording streams"
    )


def _device_line(kind: str, entry: dict, default: str | None) -> str:
    volume = entry.get("volume_percent") or []
    parts = [entry.get("state") or "unknown", f"{'/'.join(str(value) for value in volume)}%"]
    if entry.get("mute"):
        parts.append("muted")
    if entry.get("jack_client"):
        parts.append(f"JACK client {entry['jack_client']}")
    if entry.get("monitor_of"):
        parts.append(f"monitor of {entry['monitor_of']}")
    marker = " (default)" if entry.get("name") == default else ""
    return f"{kind} {entry['name']}{marker}: {entry.get('description')}; {', '.join(part for part in parts if part)}"


def _card_lines(snapshot: dict) -> list[str]:
    links = {card: device for device, card in (snapshot.get("card_links") or {}).items()}
    lines = []
    for card in snapshot.get("cards") or []:
        text = f"{card['id']}: {card.get('long_name') or card['name']}"
        if card.get("usb_product"):
            text += f" (USB {card.get('usb_vendor')} {card['usb_product']})"
        if card["id"] in links:
            text += f" is Dante device {links[card['id']]}"
        elif is_dante_interface(card):
            text += "; a Dante interface not linked to its Dante device (link_audio_card)"
        lines.append(text)
    for device, card in sorted((snapshot.get("card_links") or {}).items()):
        if card not in {entry["id"] for entry in snapshot.get("cards") or []}:
            lines.append(f"{card}: linked to {device} but not present on this computer")
    return lines


def _client_roles(snapshot: dict) -> dict[str, str]:
    jack = snapshot["jack"]
    ports = jack.get("ports") or {}
    pulse = snapshot["pulse"]
    pulse_clients = {
        entry.get("jack_client"): f"PulseAudio {'output' if kind == 'sinks' else 'input'} {entry['name']}"
        for kind in ("sinks", "sources")
        for entry in pulse.get(kind) or []
        if entry.get("jack_client")
    }
    links = {card: device for device, card in (snapshot.get("card_links") or {}).items()}
    clients: dict[str, dict] = {}
    for name, port in ports.items():
        entry = clients.setdefault(port["client"], {"input": 0, "output": 0, "cards": set(), "feeds": set()})
        entry[port["direction"]] += 1
        hardware = hardware_channel(port, snapshot["cards"]) or bridge_channel(name, snapshot.get("bridges") or {})
        if hardware:
            entry["cards"].add(hardware["card"])
        if port["direction"] == "output":
            entry["feeds"].update(destination.split(":", 1)[0] for destination in port["connections"])
    roles = {}
    for client, entry in sorted(clients.items(), key=lambda item: _natural(item[0])):
        parts = []
        if entry["cards"]:
            cards = ", ".join(f"{card} ({links[card]})" if card in links else card for card in sorted(entry["cards"]))
            parts.append(f"sound card {cards}")
        if client in pulse_clients:
            parts.append(pulse_clients[client])
        parts.append(f"{entry['input']} in, {entry['output']} out")
        if entry["feeds"]:
            parts.append(f"feeds {', '.join(sorted(entry['feeds'] - {client}, key=_natural)) or 'itself'}")
        roles[client] = "; ".join(parts)
    return roles


def _change_line(component: str, change: dict) -> str:
    kind = change.get("kind")
    when = change.get("time")
    if kind in {"connected", "disconnected"}:
        pairs = change.get("pairs") or []
        listed = ", ".join(f"{source} → {destination}" for source, destination in pairs[:4])
        more = f" and {len(pairs) - 4} more" if len(pairs) > 4 else ""
        return f"{when} JACK {kind} {listed}{more}"
    details = ", ".join(
        f"{key} {value}" for key, value in change.items() if key not in {"time", "kind"} and value not in (None, "")
    )
    label = "JACK" if component == "jack" else "PulseAudio"
    return f"{when} {label} {str(kind).replace('_', ' ')}" + (f": {details}" if details else "")


def _changes(snapshot: dict, limit: int | None) -> list[str]:
    entries = [("jack", change) for change in snapshot["jack"].get("changes") or []]
    entries += [("pulse", change) for change in snapshot["pulse"].get("changes") or []]
    entries.sort(key=lambda entry: str(entry[1].get("time")), reverse=True)
    if limit is not None:
        entries = entries[:limit]
    return [_change_line(component, change) for component, change in entries]


def host_audio_view(snapshot: dict, arguments: dict) -> dict:
    section = arguments.get("section", "summary")
    client = arguments.get("client")
    jack = snapshot["jack"]
    pulse = snapshot["pulse"]
    ports = jack.get("ports") or {}
    if client and client not in {port["client"] for port in ports.values()}:
        clients = sorted({port["client"] for port in ports.values()}, key=_natural)
        return {"error": f"no JACK client {client!r}; clients: {', '.join(clients)}"}
    selected = {name: port for name, port in ports.items() if not client or port["client"] == client}
    view: dict[str, Any] = {"host": snapshot["host"]}
    if section == "summary":
        roles = _client_roles(snapshot)
        view.update(
            {
                "jack": _jack_line(jack),
                "pulseaudio": _pulse_line(pulse),
                "cards": _card_lines(snapshot),
                "jack_clients": {name: role for name, role in roles.items() if not client or name == client},
                "streams": [_stream_line("playback", entry) for entry in pulse.get("sink_inputs") or []]
                + [_stream_line("recording", entry) for entry in pulse.get("source_outputs") or []],
                "recent_changes": _changes(snapshot, RECENT_CHANGES),
                "more": "section connections, ports, pulse, cards or changes; trace_signal follows audio through",
            }
        )
    elif section == "connections":
        view["connections"] = [
            f"{_port_label(name, port)} → "
            + ", ".join(_port_label(destination, ports.get(destination, {})) for destination in port["connections"])
            for name, port in sorted(selected.items(), key=lambda item: _natural(item[0]))
            if port["direction"] == "output" and port["connections"]
        ]
        if client:
            view["heard_by_client"] = [
                f"{_port_label(name, port)} ← " + ", ".join(port["connections"])
                for name, port in sorted(selected.items(), key=lambda item: _natural(item[0]))
                if port["direction"] == "input" and port["connections"]
            ]
        view["unconnected_ports"] = len([port for port in selected.values() if not port["connections"]])
    elif section == "ports":
        lines = []
        for name, port in sorted(selected.items(), key=lambda item: _natural(item[0])):
            hardware = hardware_channel(port, snapshot["cards"]) or bridge_channel(name, snapshot.get("bridges") or {})
            text = f"{_port_label(name, port)}: {port['type']} {port['direction']}"
            if hardware:
                text += f", {hardware['card']} {hardware['direction']} {hardware['channel']}"
            if port["connections"]:
                arrow = "→" if port["direction"] == "output" else "←"
                text += f" {arrow} {', '.join(port['connections'])}"
            lines.append(text)
        view["ports"] = lines
    elif section == "pulse":
        server = pulse.get("server") or {}
        view.update(
            {
                "pulseaudio": _pulse_line(pulse),
                "outputs": [
                    _device_line("output", entry, server.get("default_sink")) for entry in pulse.get("sinks") or []
                ],
                "inputs": [
                    _device_line("input", entry, server.get("default_source")) for entry in pulse.get("sources") or []
                ],
                "streams": [_stream_line("playback", entry) for entry in pulse.get("sink_inputs") or []]
                + [_stream_line("recording", entry) for entry in pulse.get("source_outputs") or []],
            }
        )
    elif section == "cards":
        view["cards"] = _card_lines(snapshot)
        view["links"] = snapshot.get("card_links") or "none"
        if snapshot.get("bridges"):
            view["bridged_by"] = {
                client: f"{bridge['program']} {bridge['direction']} on {bridge['card']}"
                for client, bridge in snapshot["bridges"].items()
            }
    elif section == "changes":
        view["changes"] = _changes(snapshot, None) or "no changes since the server connected"
    return compact(view)


def _shure_battery(transmitter: dict) -> str | None:
    percent = transmitter.get("battery_charge_percent")
    minutes = transmitter.get("battery_minutes")
    if isinstance(percent, int) and 0 <= percent <= 100:
        return f"{percent}%"
    if isinstance(minutes, int) and minutes < 65530:
        return f"{minutes // 60}h{minutes % 60:02d}m"
    return None


def wireless_lines(devices: dict) -> list[str]:
    lines = []
    for device in sorted(devices.values(), key=lambda item: str(item.get("name"))):
        if not device.get("online"):
            continue
        channels = []
        for number, channel in sorted((device.get("channels") or {}).items(), key=lambda item: int(item[0])):
            text = f"ch{number} {channel.get('name') or ''}".rstrip()
            if "active" in channel:
                if not channel["active"]:
                    text += " no transmitter"
                else:
                    transmitter = channel.get("transmitter") or {}
                    battery = _shure_battery(transmitter)
                    text += f" {transmitter.get('model') or 'transmitter'} on" + (
                        f", battery {battery}" if battery else ""
                    )
            elif channel.get("rf_mute"):
                text += " RF muted"
            elif isinstance(channel.get("frequency"), int):
                text += f" on {channel['frequency'] / 1000:.3f} MHz"
            channels.append(text)
        lines.append(
            f"{device.get('name')} ({device.get('model') or device.get('device_type')}): {'; '.join(channels)}"
        )
    return lines


def wireless_conditions(devices: dict) -> list[str]:
    conditions = []
    for device in devices.values():
        name = device.get("name")
        if not device.get("online"):
            continue
        for number, channel in (device.get("channels") or {}).items():
            label = f"{name} ch{number} {channel.get('name') or ''}".rstrip()
            if channel.get("active"):
                transmitter = channel.get("transmitter") or {}
                percent = transmitter.get("battery_charge_percent")
                minutes = transmitter.get("battery_minutes")
                if (isinstance(percent, int) and percent <= LOW_BATTERY_PERCENT) or (
                    isinstance(minutes, int) and minutes <= LOW_BATTERY_MINUTES
                ):
                    conditions.append(f"{label}: transmitter battery low ({_shure_battery(transmitter)})")
                if str(transmitter.get("mute_status") or "").upper() in {"ON", "MUTE", "MUTED"}:
                    conditions.append(f"{label}: transmitter muted")
            interference = channel.get("interference_status")
            if interference not in (None, "", "NONE"):
                conditions.append(f"{label}: interference {interference}")
            encryption = channel.get("encryption_status")
            if device.get("encryption_mode") not in (None, "OFF") and encryption not in (None, "", "OK"):
                conditions.append(f"{label}: encryption {encryption}")
            if channel.get("audio_mute"):
                conditions.append(f"{label}: audio muted on the receiver")
    return conditions


def write_result_view(name: str, payload: Any) -> Any:
    if not isinstance(payload, dict) or (payload.get("error") and "source" not in payload and "target" not in payload):
        return payload
    if name in {"connect_audio_ports", "disconnect_audio_ports"}:
        return {
            "connection": f"{payload.get('source')} \u2192 {payload.get('destination')}",
            "was": "connected" if payload.get("connected_now") else "disconnected",
            "now": payload.get("now"),
            "result": payload.get("result") or payload.get("error"),
            "source_feeds": payload.get("source_feeds") or "nothing",
            "destination_hears": payload.get("destination_hears") or "nothing",
        }
    if name in PULSE_ACTIONS:
        return {
            "target": payload.get("target"),
            "was": payload.get("current"),
            "now": payload.get("now"),
            "result": payload.get("result") or payload.get("error"),
        }
    if name == "set_wireless_value":
        channel = payload.get("channel")
        return {
            "target": f"{payload.get('device')}" + (f" ch{channel}" if channel is not None else ""),
            "setting": payload.get("key"),
            "was": payload.get("current_described"),
            "now": payload.get("reported_described") or "no reply from the device",
            "verified": payload.get("verified"),
        }
    return payload


class McpHostAudioTools:
    host_audio: Any
    shure: Any

    def _host_audio_preview(self, name: str, arguments: dict) -> dict:
        preview: dict[str, Any] = {"changed": False, "confirmation_required": True, "operation": name}
        if name in {"connect_audio_ports", "disconnect_audio_ports"}:
            change = jack_change(self.host_audio.jack, arguments, name == "connect_audio_ports")
            preview["change"] = {
                "connection": f"{change['source']} → {change['destination']}",
                "from": "connected" if change["connected_now"] else "disconnected",
                "to": change["requested"],
            }
            preview["source_feeds_now"] = change["source_feeds"] or "nothing"
            preview["destination_hears_now"] = change["destination_hears"] or "nothing"
            if change["connected_now"] == (name == "connect_audio_ports"):
                preview["note"] = "already so; nothing would change"
            preview["warning"] = "a JACK patchbay or session manager may undo this; the result reports it if so"
        elif name in PULSE_ACTIONS:
            change = pulse_change(self.host_audio.pulse, PULSE_ACTIONS[name], arguments)
            preview["change"] = {"target": change["target"], "from": change["current"], "to": change["requested"]}
            if change["current"] == change["requested"]:
                preview["note"] = "already so; nothing would change"
        elif name == "link_audio_card":
            change = card_change(self.host_audio.snapshot(), self._serialized_devices(), arguments)
            preview["change"] = {
                "card": f"{change['card']} ({change['card_name']})",
                "from": change["current"] or "not linked",
                "to": change["device"] or "not linked",
            }
            preview["jack_ports_on_card"] = change["jack_ports"]
        elif name == "set_wireless_value":
            change = self.wireless_change(arguments)
            channel = change["channel"]
            target = change["device"] + (
                f" ch{channel} {change['channel_name'] or ''}".rstrip() if channel is not None else ""
            )
            channel_part = f"{channel} " if channel is not None else ""
            preview["change"] = {
                "target": target,
                "setting": change["key"],
                "from": change["current_described"],
                "to": change["requested_described"],
                "command": f"SET {channel_part}{change['key']} {change['value']}",
            }
            if change["current_described"] == change["requested_described"]:
                preview["note"] = "already so; nothing would change"
        preview["next"] = NEXT_STEP
        return compact(preview)

    async def _host_audio_tool(self, name: str, arguments: dict, writer, client_name: str) -> tuple[Any, bool]:
        if name == "set_wireless_value":
            if not self.shure:
                return {"error": "Shure support is not running"}, True
        elif self.host_audio is None:
            return {"error": "host audio support is turned off in this server's configuration"}, True
        if name == "trace_signal":
            status, payload = await self.host_audio_trace(
                arguments["point"], arguments.get("direction", "both"), arguments.get("levels", True)
            )
            return compact(payload), status >= 400
        if name == "get_signal_levels":
            points = arguments["points"]
            status, payload = await self.host_audio_point_levels(
                [points] if isinstance(points, str) else points, period=signal_period(arguments, time.time())
            )
            return compact(payload), status >= 400
        if name == "get_host_audio":
            status, snapshot = await self.host_audio_snapshot(arguments.get("host"))
            if status >= 400:
                return snapshot, True
            payload = host_audio_view(snapshot, arguments)
            return payload, "error" in payload
        try:
            if arguments.get("confirmed") is not True:
                if name != "set_wireless_value":
                    await self.host_audio.ensure()
                return self._host_audio_preview(name, arguments), False
        except ValueError as exception:
            return {"error": str(exception)}, True
        logger.info("MCP %s called %s", client_name, name)
        body = {key: value for key, value in arguments.items() if key != "confirmed"}
        status, payload = await self._post_captured(WRITE_PATHS[name], body, writer)
        return compact(write_result_view(name, payload)), status >= 400

    async def _all_host_snapshots(self) -> dict[str, dict]:
        if self.host_audio is None:
            return {}
        local = self.host_audio.snapshot()
        snapshots, _ = await self._peer_snapshots(SUMMARY_PEER_TIMEOUT_SECONDS)
        return {local["host"]: local, **snapshots}

    async def _host_overview(self) -> dict:
        view: dict[str, Any] = {}
        snapshots = await self._all_host_snapshots()
        for host, snapshot in snapshots.items():
            summary = compact(
                {
                    "jack": _jack_line(snapshot["jack"]),
                    "pulseaudio": _pulse_line(snapshot["pulse"]),
                    "dante_cards": [line for line in _card_lines(snapshot) if "Dante" in line] or None,
                }
            )
            if host == self.host_audio.host:
                view["host_audio"] = {
                    "host": host,
                    **summary,
                    "detail": "trace_signal follows audio through; get_host_audio lists clients and connections",
                }
            else:
                view.setdefault("other_computers_audio", {})[host] = summary
        if self.shure and self.shure.devices:
            view["wireless"] = wireless_lines({mac: device.to_json() for mac, device in self.shure.devices.items()})
        return view

    def _snapshot_conditions(self, snapshot: dict, records: dict) -> list[str]:
        conditions: list[str] = []
        host = snapshot["host"]
        jack = snapshot["jack"]
        for key, label in (("jack", "JACK"), ("pulse", "PulseAudio")):
            component = snapshot[key]
            if component.get("seen_running") and not component.get("available"):
                conditions.append(f"{label} on {host} stopped: {component.get('reason')}")
        if jack.get("xruns"):
            conditions.append(
                f"JACK on {host} had {jack['xruns']} xrun{'' if jack['xruns'] == 1 else 's'} since "
                f"{jack.get('xruns_since')}, last at {jack.get('last_xrun')}"
            )
        present = {card["id"] for card in snapshot["cards"]}
        for device, card in sorted((snapshot.get("card_links") or {}).items()):
            record = records.get(device)
            if card not in present:
                continue
            if record is None or not record.get("online"):
                conditions.append(
                    f"sound card {card} on {host} is linked to Dante device {device}, which is not online"
                )
                continue
            rate = record.get("sample_rate_hz")
            if jack.get("available") and rate and jack.get("sample_rate") and rate != jack["sample_rate"]:
                uses_card = any(
                    (hardware_channel(port, snapshot["cards"]) or {}).get("card") == card
                    for port in (jack.get("ports") or {}).values()
                )
                if uses_card:
                    conditions.append(
                        f"JACK on {host} runs at {jack['sample_rate']} Hz but {device} (card {card}) is at {rate} Hz"
                    )
        for card in snapshot["cards"]:
            if is_dante_interface(card) and card["id"] not in (snapshot.get("card_links") or {}).values():
                conditions.append(
                    f"sound card {card['id']} on {host} is a Dante interface not linked to its Dante device; "
                    "link_audio_card on that computer lets traces cross it"
                )
        return conditions

    async def _host_conditions(self) -> list[str]:
        conditions: list[str] = []
        snapshots = await self._all_host_snapshots()
        if snapshots:
            records = {
                record.get("name"): record for record in self._serialized_devices().values() if isinstance(record, dict)
            }
            for snapshot in snapshots.values():
                conditions.extend(self._snapshot_conditions(snapshot, records))
        if self.shure:
            conditions.extend(
                wireless_conditions({mac: device.to_json() for mac, device in self.shure.devices.items()})
            )
        return conditions
