from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field
from typing import Any

from netaudio.host_audio.links import bridge_channel, hardware_channel

MAXIMUM_STARTS = 12
REMOTE_PRIORITY = 3
MAXIMUM_LINES = 80
SIGNAL_FLOOR_DBFS = -80.0
SHURE_LEVEL_OFFSET = 120
ARROWS = {"downstream": "→", "upstream": "←"}
LABEL_KIND = re.compile(r"\s*\([^()]*\)$")


def _natural(value: str) -> list:
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", value)]


def _fold(value: Any) -> str:
    return " ".join(str(value).split()).casefold()


def _channel_label(channel: Any) -> str | None:
    if isinstance(channel, dict):
        label = channel.get("friendly_name") or channel.get("name")
        return str(label) if label not in (None, "") else None
    return None


@dataclass
class Node:
    identity: str
    kind: str
    label: str
    details: dict = field(default_factory=dict)


@dataclass
class Step:
    depth: int
    identity: str
    note: str | None
    repeated: bool = False
    parent: str | None = None


class SignalGraph:
    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}
        self.downstream: dict[str, list[tuple[str, str | None]]] = {}
        self.upstream: dict[str, list[tuple[str, str | None]]] = {}
        self.names: dict[str, list[tuple[int, str]]] = {}

    def add(self, node: Node, names: dict[str, int] | None = None) -> Node:
        if node.identity not in self.nodes:
            names = {node.label: 0, LABEL_KIND.sub("", node.label): 0, **(names or {})}
        self.nodes.setdefault(node.identity, node)
        for name, priority in (names or {}).items():
            if name:
                self.names.setdefault(_fold(name), []).append((priority, node.identity))
        return self.nodes[node.identity]

    def link(self, source: str, destination: str, note: str | None = None) -> None:
        if source not in self.nodes or destination not in self.nodes:
            return
        if any(existing == destination for existing, _ in self.downstream.get(source, ())):
            return
        self.downstream.setdefault(source, []).append((destination, note))
        self.upstream.setdefault(destination, []).append((source, note))

    def resolve(self, point: str) -> tuple[list[str], list[str]]:
        matches = self.names.get(_fold(point))
        if matches:
            best = min(priority for priority, _ in matches)
            identities = sorted({identity for priority, identity in matches if priority == best}, key=_natural)
            return identities[:MAXIMUM_STARTS], []
        suggestions = difflib.get_close_matches(_fold(point), list(self.names), n=5, cutoff=0.6)
        return [], suggestions

    def walk(self, starts: list[str], direction: str) -> list[Step]:
        edges = self.downstream if direction == "downstream" else self.upstream
        steps: list[Step] = []
        seen: set[str] = set(starts)

        def visit(identity: str, depth: int) -> None:
            for following, note in sorted(edges.get(identity, ()), key=lambda edge: _natural(edge[0])):
                if len(steps) >= MAXIMUM_LINES:
                    return
                repeated = following in seen
                steps.append(Step(depth, following, note, repeated, identity))
                if not repeated:
                    seen.add(following)
                    visit(following, depth + 1)

        for start in starts:
            visit(start, 1)
        return steps

    def remaining(self, starts: list[str], direction: str, steps: list[Step]) -> int:
        edges = self.downstream if direction == "downstream" else self.upstream
        reachable: set[str] = set()
        pending = list(starts)
        while pending:
            identity = pending.pop()
            for following, _ in edges.get(identity, ()):
                if following not in reachable:
                    reachable.add(following)
                    pending.append(following)
        shown = {step.identity for step in steps}
        return len(reachable - shown - set(starts))


def _shure_state(channel: dict) -> str:
    if "active" in channel and not channel["active"]:
        return "no transmitter"
    if channel.get("rf_mute"):
        return "RF muted"
    if channel.get("audio_mute"):
        return "muted"
    return "on"


def _add_wireless(graph: SignalGraph, wireless: dict, wireless_links: dict[str, str]) -> None:
    for mac, device in wireless.items():
        if not isinstance(device, dict):
            continue
        device_name = device.get("name") or mac
        for number, channel in sorted((device.get("channels") or {}).items(), key=lambda item: int(item[0])):
            if not isinstance(channel, dict):
                continue
            channel_name = channel.get("name") or ""
            identity = f"wireless:{mac}:{number}"
            label = f"{device_name} ch{number}" + (f" {channel_name}" if channel_name else "")
            graph.add(
                Node(
                    identity,
                    "wireless",
                    f"{label} (Shure {device.get('model') or device.get('device_type')})",
                    {"device": device, "channel": channel, "state": _shure_state(channel)},
                ),
                {
                    f"{device_name} ch{number}": 0,
                    f"{device_name} ch {number}": 0,
                    f"{device_name} {number}": 0,
                    f"{device_name}:{number}": 0,
                    f"{device_name} {channel_name}": 0,
                    channel_name: 1,
                    device_name: 2,
                    mac: 2,
                },
            )
            dante_device = wireless_links.get(mac)
            if dante_device:
                graph.link(identity, f"dante:{dante_device}:tx:{number}", "Dante output")


def _add_dante(graph: SignalGraph, records: dict) -> None:
    by_name = {}
    for record in records.values():
        if not isinstance(record, dict) or not record.get("name"):
            continue
        name = record["name"]
        by_name[name] = record
        channels = record.get("channels") or {}
        for direction, key in (("rx", "receivers"), ("tx", "transmitters")):
            for number, channel in (channels.get(key) or {}).items():
                label = _channel_label(channel) or str(number)
                identity = f"dante:{name}:{direction}:{number}"
                graph.add(
                    Node(
                        identity,
                        f"dante_{direction}",
                        f"{name} {direction} {number}" + (f" {label}" if label != str(number) else "") + " (Dante)",
                        {
                            "device": name,
                            "direction": direction,
                            "channel": int(number),
                            "online": record.get("online"),
                        },
                    ),
                    {
                        f"{name} {direction} {number}": 0,
                        f"{name} {direction} {label}": 0,
                        f"{name}:{label}": 1,
                        f"{name} {label}": 1,
                        label: 1,
                    },
                )
    for name, record in by_name.items():
        for subscription in record.get("subscriptions") or []:
            if not isinstance(subscription, dict) or not subscription.get("tx_device"):
                continue
            raw_status = subscription.get("status")
            status: dict = raw_status if isinstance(raw_status, dict) else {}
            if status.get("state") == "none":
                continue
            rx_number = subscription.get("rx_channel_number")
            transmitter = by_name.get(subscription["tx_device"])
            tx_label = subscription.get("tx_channel")
            tx_number = None
            if transmitter is not None:
                for number, channel in ((transmitter.get("channels") or {}).get("transmitters") or {}).items():
                    if tx_label in {_channel_label(channel), (channel or {}).get("name"), str(number)}:
                        tx_number = number
                        break
            if tx_number is None:
                identity = f"dante:{subscription['tx_device']}:tx:{tx_label}"
                graph.add(
                    Node(
                        identity,
                        "dante_tx",
                        f"{subscription['tx_device']} tx {tx_label} (Dante, not on the network)",
                        {"device": subscription["tx_device"], "direction": "tx", "online": False},
                    )
                )
                tx_identity = identity
            else:
                tx_identity = f"dante:{subscription['tx_device']}:tx:{tx_number}"
            note = status.get("label")
            if status.get("severity") not in (None, "ok", "none") and status.get("detail"):
                note = f"{note}: {status['detail']}"
            graph.link(tx_identity, f"dante:{name}:rx:{rx_number}", note)


def _ports_by_client(ports: dict) -> dict[str, dict[str, list[str]]]:
    clients: dict[str, dict[str, list[str]]] = {}
    for name, port in ports.items():
        clients.setdefault(port["client"], {"input": [], "output": []})[port["direction"]].append(name)
    for directions in clients.values():
        for names in directions.values():
            names.sort(key=_natural)
    return clients


@dataclass
class HostScope:
    host: str
    remote: bool = False

    def jack(self, port: str) -> str:
        return f"jack@{self.host}:{port}"

    def pulse(self, kind: str, key: Any) -> str:
        return f"pulse@{self.host}:{kind}:{key}"

    def label(self, text: str) -> str:
        return f"{self.host} {text}" if self.remote else text

    def names(self, names: dict[str, int]) -> dict[str, int]:
        if not self.remote:
            return names
        scoped: dict[str, int] = {}
        for name, priority in names.items():
            if not name:
                continue
            scoped[f"{self.host} {name}"] = priority
            scoped[f"{name}@{self.host}"] = priority
            scoped.setdefault(name, priority + REMOTE_PRIORITY)
        return scoped


def _add_jack(graph: SignalGraph, scope: HostScope, snapshot: dict, pulse_clients: set[str]) -> None:
    ports = snapshot["jack"].get("ports") or {}
    cards = snapshot.get("cards") or []
    bridges = snapshot.get("bridges") or {}
    devices_by_card = {card: device for device, card in (snapshot.get("card_links") or {}).items()}
    clients = _ports_by_client(ports)
    for name, port in ports.items():
        hardware = hardware_channel(port, cards) or bridge_channel(name, bridges)
        aliases = [alias for alias in port.get("aliases") or () if not alias.startswith("alsa_pcm:")]
        label = name + (f" [{', '.join(aliases)}]" if aliases else "")
        where = (
            f"JACK, sound card {hardware['card']} {hardware['direction']} {hardware['channel']}" if hardware else "JACK"
        )
        details = {"host": scope.host, "port": name, "direction": port["direction"], "hardware": hardware}
        if port["direction"] == "input" and len(port["connections"]) > 1:
            details["mixes"] = list(port["connections"])
        names = {name: 0, f"jack {name}": 0, port["client"]: 2}
        for alias in port.get("aliases") or ():
            names[alias] = 1
        if port.get("pretty_name"):
            names[port["pretty_name"]] = 1
        graph.add(Node(scope.jack(name), "jack", scope.label(f"{label} ({where})"), details), scope.names(names))
    for name, port in ports.items():
        if port["direction"] == "output":
            for destination in port["connections"]:
                graph.link(scope.jack(name), scope.jack(destination), None)
        hardware = graph.nodes[scope.jack(name)].details.get("hardware")
        if hardware is None:
            continue
        device = devices_by_card.get(hardware["card"])
        if device is None:
            continue
        note = f"card {hardware['card']}" + (f" on {scope.host}" if scope.remote else "")
        if hardware["direction"] == "capture":
            graph.link(f"dante:{device}:rx:{hardware['channel']}", scope.jack(name), note)
        else:
            graph.link(scope.jack(name), f"dante:{device}:tx:{hardware['channel']}", note)
    for client, directions in clients.items():
        inputs, outputs = directions["input"], directions["output"]
        if not inputs or not outputs or client in pulse_clients or client in bridges:
            continue
        if any(graph.nodes[scope.jack(name)].details.get("hardware") for name in inputs + outputs):
            continue
        if len(inputs) == len(outputs):
            for source, destination in zip(inputs, outputs):
                graph.link(scope.jack(source), scope.jack(destination), f"through {client}, ports paired in order")
        else:
            for source in inputs:
                for destination in outputs:
                    graph.link(scope.jack(source), scope.jack(destination), f"through {client}")


def _add_pulse(graph: SignalGraph, scope: HostScope, snapshot: dict) -> set[str]:
    pulse = snapshot["pulse"]
    server = pulse.get("server") or {}
    jack_clients: set[str] = set()
    clients = _ports_by_client(snapshot["jack"].get("ports") or {})
    for kind, entries, default_key, default_name in (
        ("sink", pulse.get("sinks") or [], "default_sink", "default output"),
        ("source", pulse.get("sources") or [], "default_source", "default input"),
    ):
        for entry in entries:
            word = "output" if kind == "sink" else "input"
            default = entry["name"] == server.get(default_key)
            label = f"PulseAudio {word} {entry['name']}" + (" (default)" if default else "")
            names = {entry["name"]: 0, f"pulse {entry['name']}": 0, f"{word} {entry['name']}": 0}
            if entry.get("description"):
                names[entry["description"]] = 1
            if default:
                names[default_name] = 0
            graph.add(
                Node(scope.pulse(kind, entry["name"]), f"pulse_{kind}", scope.label(label), {"entry": entry}),
                scope.names(names),
            )
            if entry.get("jack_client") in clients:
                jack_clients.add(entry["jack_client"])
    for entry in pulse.get("sources") or []:
        if entry.get("monitor_of"):
            graph.link(scope.pulse("sink", entry["monitor_of"]), scope.pulse("source", entry["name"]), "monitor")
    for kind, entries, device_key in (
        ("playback", pulse.get("sink_inputs") or [], "sink"),
        ("recording", pulse.get("source_outputs") or [], "source"),
    ):
        for entry in entries:
            identity = scope.pulse(kind, entry["index"])
            application = entry.get("application") or entry.get("binary") or "unknown application"
            label = f"{application} {kind} stream {entry['index']}"
            if entry.get("media") and entry.get("media") != application:
                label += f" \u201c{entry['media']}\u201d"
            names = {f"stream {entry['index']}": 0, f"{kind} {entry['index']}": 0}
            for value in (entry.get("application"), entry.get("binary")):
                if value:
                    names[value] = 1
                    names[f"{value} {kind}"] = 0
            if entry.get("media"):
                names[entry["media"]] = 2
            graph.add(
                Node(identity, f"pulse_{kind}", scope.label(f"{label} (PulseAudio)"), {"entry": entry}),
                scope.names(names),
            )
            device = entry.get(device_key)
            if device:
                if kind == "playback":
                    graph.link(identity, scope.pulse("sink", device), None)
                else:
                    graph.link(scope.pulse("source", device), identity, None)
    return jack_clients


def _add_pulse_links(graph: SignalGraph, scope: HostScope, snapshot: dict) -> None:
    pulse = snapshot["pulse"]
    clients = _ports_by_client(snapshot["jack"].get("ports") or {})
    for kind, entries in (("sink", pulse.get("sinks") or []), ("source", pulse.get("sources") or [])):
        for entry in entries:
            jack_client = entry.get("jack_client")
            if not jack_client or jack_client not in clients:
                continue
            identity = scope.pulse(kind, entry["name"])
            if kind == "sink":
                for port in clients[jack_client]["output"]:
                    graph.link(identity, scope.jack(port), f"JACK client {jack_client}")
            else:
                for port in clients[jack_client]["input"]:
                    graph.link(scope.jack(port), identity, f"JACK client {jack_client}")


def build_graph(
    *,
    records: dict,
    wireless: dict,
    wireless_links: dict[str, str],
    hosts: list[tuple[HostScope, dict]],
) -> SignalGraph:
    graph = SignalGraph()
    _add_dante(graph, records)
    _add_wireless(graph, wireless, wireless_links)
    for scope, snapshot in hosts:
        pulse_clients = _add_pulse(graph, scope, snapshot) if snapshot["pulse"].get("available") else set()
        if snapshot["jack"].get("available"):
            _add_jack(graph, scope, snapshot, pulse_clients)
            if snapshot["pulse"].get("available"):
                _add_pulse_links(graph, scope, snapshot)
    return graph


def _meter_target(node: Node, snapshots: dict[str, dict]) -> tuple[str, str, list[str]]:
    host, name = node.details["host"], node.details["port"]
    port = ((snapshots.get(host) or {}).get("jack", {}).get("ports") or {}).get(name) or {}
    sources = [name] if port.get("direction") == "output" else list(port.get("connections") or ())
    return host, name, sources


def jack_meter_targets(
    graph: SignalGraph, identities: list[str], snapshots: dict[str, dict]
) -> dict[str, dict[str, list[str]]]:
    targets: dict[str, dict[str, list[str]]] = {}
    for identity in identities:
        node = graph.nodes.get(identity)
        if node is None:
            continue
        followers = [node]
        if node.kind in {"pulse_sink", "pulse_source"}:
            edges = (
                graph.downstream.get(identity, ()) if node.kind == "pulse_sink" else graph.upstream.get(identity, ())
            )
            followers = [graph.nodes[following] for following, _ in edges if following in graph.nodes]
        for follower in followers:
            if follower.kind != "jack":
                continue
            host, name, sources = _meter_target(follower, snapshots)
            targets.setdefault(host, {})[name] = sources
    return targets


def _jack_level(result: dict | None) -> tuple[str, str]:
    if not result:
        return "unknown", "not measured"
    if result.get("error"):
        return "unknown", result["error"]
    if result.get("connected") is False:
        return "silent", "nothing connected"
    peak, rms = result.get("peak_dbfs"), result.get("rms_dbfs")
    if peak is None:
        return "silent", "digital silence"
    text = f"peak {peak:g} dBFS, RMS {rms:g} dBFS" if rms is not None else f"peak {peak:g} dBFS"
    return ("signal" if peak >= SIGNAL_FLOOR_DBFS else "silent"), text


def _dante_level(reading: dict | None) -> tuple[str, str]:
    if not isinstance(reading, dict):
        return "unknown", "not metered"
    state = reading.get("state")
    dbfs = reading.get("dbfs")
    if state in {"signal_present", "clipping"}:
        text = f"{dbfs:g} dBFS" if isinstance(dbfs, (int, float)) else "signal"
        return "signal", text + (" (clipping)" if state == "clipping" else "")
    if state == "below_threshold":
        return "quiet", f"very quiet ({dbfs:g} dBFS)" if isinstance(dbfs, (int, float)) else "very quiet"
    if state in {"mute_or_floor", "muted"}:
        return "silent", "silent"
    return "unknown", str(state or "unknown")


def shure_level_text(channel: dict) -> str | None:
    rms = channel.get("audio_level_rms")
    peak = channel.get("audio_level_peak")
    if isinstance(rms, int):
        peak_text = f", peak {peak - SHURE_LEVEL_OFFSET} dBFS" if isinstance(peak, int) else ""
        return f"RMS {rms - SHURE_LEVEL_OFFSET} dBFS{peak_text}"
    left, right = channel.get("audio_in_level_l"), channel.get("audio_in_level_r")
    if isinstance(left, int) or isinstance(right, int):
        return f"input meter left {left}, right {right} in the P10T's own units, not dBFS; larger is louder"
    return None


def _wireless_level(details: dict) -> tuple[str, str]:
    channel = details.get("channel") or {}
    state = details.get("state")
    if state == "no transmitter":
        return "silent", "no transmitter"
    if state in {"muted", "RF muted"}:
        return "silent", state
    rms = channel.get("audio_level_rms")
    text = shure_level_text(channel)
    if isinstance(rms, int):
        return ("signal" if rms - SHURE_LEVEL_OFFSET >= SIGNAL_FLOOR_DBFS else "silent"), text
    return "unknown", text or state or "on"


def node_level(
    graph: SignalGraph,
    identity: str,
    jack_levels: dict[str, dict],
    dante_levels: dict[str, dict],
) -> tuple[str, str] | None:
    node = graph.nodes.get(identity)
    if node is None:
        return None
    if node.kind == "jack":
        return _jack_level((jack_levels.get(node.details["host"]) or {}).get(node.details["port"]))
    if node.kind in {"dante_rx", "dante_tx"}:
        if node.details.get("online") is False:
            return "silent", "device offline"
        readings = (dante_levels.get(node.details["device"]) or {}).get(node.details["direction"]) or {}
        reading = readings.get(node.details.get("channel"))
        if reading is None:
            reading = readings.get(str(node.details.get("channel")))
        return _dante_level(reading)
    if node.kind == "wireless":
        return _wireless_level(node.details)
    if node.kind in {"pulse_sink", "pulse_source"}:
        entry = node.details["entry"]
        followers = (
            graph.downstream.get(identity, ()) if node.kind == "pulse_sink" else graph.upstream.get(identity, ())
        )
        results = [
            (jack_levels.get(graph.nodes[following].details["host"]) or {}).get(graph.nodes[following].details["port"])
            for following, _ in followers
            if graph.nodes.get(following) is not None and graph.nodes[following].kind == "jack"
        ]
        volume = entry.get("volume_percent") or []
        suffix = f", volume {'/'.join(str(value) for value in volume)}%" if volume else ""
        if entry.get("mute"):
            suffix += ", muted"
        if results:
            loudest = max(results, key=lambda result: (result or {}).get("peak_dbfs") or -1000.0)
            state, text = _jack_level(loudest)
            return state, (f"loudest channel {text}" if len(results) > 1 else text) + suffix
        return "unknown", (entry.get("state") or "unknown") + suffix
    if node.kind in {"pulse_playback", "pulse_recording"}:
        entry = node.details["entry"]
        parts = []
        if entry.get("corked"):
            parts.append("paused")
        if entry.get("mute"):
            parts.append("muted")
        volume = entry.get("volume_percent") or []
        if volume:
            parts.append(f"volume {'/'.join(str(value) for value in volume)}%")
        state = "silent" if entry.get("mute") or entry.get("corked") else "unknown"
        return state, ", ".join(parts) or "running"
    return None


def render(
    graph: SignalGraph,
    starts: list[str],
    steps: list[Step],
    direction: str,
    levels: dict[str, tuple[str, str]],
) -> tuple[list[str], list[str]]:
    arrow = ARROWS[direction]
    lines = []
    for step in steps:
        node = graph.nodes[step.identity]
        text = f"{'  ' * (step.depth - 1)}{arrow} {node.label}"
        if step.note:
            text += f" via {step.note}"
        if step.repeated:
            text += " (shown above)"
        elif step.identity in levels:
            text += f": {levels[step.identity][1]}"
        if direction == "downstream" and not step.repeated and node.details.get("mixes"):
            text += f" (a mix of {', '.join(node.details['mixes'])})"
        lines.append(text)
    stops = []
    for step in steps:
        if step.parent is None or step.repeated:
            continue
        upstream, downstream = (
            (step.parent, step.identity) if direction == "downstream" else (step.identity, step.parent)
        )
        upstream_state = levels.get(upstream, ("unknown", ""))[0]
        downstream_state = levels.get(downstream, ("unknown", ""))[0]
        if upstream_state in {"signal", "quiet"} and downstream_state == "silent":
            stops.append(f"{graph.nodes[upstream].label} has signal but {graph.nodes[downstream].label} is silent")
    return lines, stops
