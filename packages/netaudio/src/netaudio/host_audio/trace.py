from __future__ import annotations

import difflib
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from netaudio.host_audio.links import bridge_channel, hardware_channel
from netaudio.shure.memory import type_words

MAXIMUM_STARTS = 12
REMOTE_PRIORITY = 3
MAXIMUM_LINES = 80
SOURCE_LIMIT = 8
ANSWER_CAUSE_LIMIT = 2
RELATED_ANSWER_LIMIT = 4
RELATED_FOLLOW_LIMIT = 3
LOOSE_LIMIT = 4
ALSO_NAMED_LIMIT = 6
STEM_MINIMUM = 4
SPOKEN_FILLER_WORDS = frozenset({"my", "our", "your", "the", "a", "an", "this", "that"})
GENERIC_NAME_WORDS = frozenset(
    {"avio", "usb", "usbc", "aes", "bt", "rx", "tx", "lx", "dante", "ch", "in", "out", "left", "right", "local"}
)
LEVEL_CHANGE_DB = 3.0
PEAK_TEXT = re.compile(r"peak (-?\d+(?:\.\d+)?) dBFS")
LOOSE_RANK = {"pulse_sink": 0, "pulse_source": 0, "pulse_playback": 0, "pulse_recording": 0, "wireless": 1}
SIGNAL_FLOOR_DBFS = -80.0
SHURE_LEVEL_OFFSET = 120
ARROWS = {"downstream": "→", "upstream": "←"}
TX_NUMBER = re.compile(r"^tx \d+ (?=\S)")
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
        self.local_host: str | None = None

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

    def resolve_device(self, point: str) -> list[str]:
        folded = _fold(point)
        routed = [
            identity
            for identity, node in self.nodes.items()
            if node.kind in {"dante_rx", "dante_tx"}
            and _fold(node.details.get("device")) == folded
            and (self.downstream.get(identity) if node.kind == "dante_tx" else self.upstream.get(identity))
        ]
        return sorted(routed, key=_natural)[:MAXIMUM_STARTS]

    def resolve_loosely(self, point: str, prefix: bool = False) -> list[str]:
        words = re.findall(r"[a-z0-9]+", _fold(point))
        if not words:
            return []
        found: set[str] = set()
        for name, entries in self.names.items():
            tokens = re.findall(r"[a-z0-9]+", name)
            if all(word in tokens or (prefix and any(token.startswith(word) for token in tokens)) for word in words):
                found.update(identity for _, identity in entries)
        if "monitor" not in words:
            found = {identity for identity in found if not self.nodes[identity].label.endswith(".monitor")}
        spoken = [word for word in words if word not in SPOKEN_FILLER_WORDS]
        if not found and spoken and spoken != words:
            return self.resolve_loosely(" ".join(spoken))
        if not found and not prefix:
            stems = [word[:-1] if len(word) >= STEM_MINIMUM else word for word in words]
            if stems != words:
                return self.resolve_loosely(" ".join(stems), prefix=True)
        if not found:
            return []

        def rank(identity: str) -> tuple[int, bool]:
            kind = self.nodes[identity].kind
            host = identity.split("@", 1)[1].split(":", 1)[0] if "@" in identity else self.local_host
            return LOOSE_RANK.get(kind, 2 if kind.startswith("dante") else 3), host != self.local_host

        best = min(rank(identity) for identity in found)
        chosen = sorted((identity for identity in found if rank(identity) == best), key=_natural)
        if len(chosen) > LOOSE_LIMIT:
            transmitters = [identity for identity in chosen if self.nodes[identity].kind == "dante_tx"]
            chosen = transmitters if transmitters else chosen
        return chosen if len(chosen) <= LOOSE_LIMIT else []

    def also_named(self, point: str, exclude: set[str]) -> list[str]:
        return [
            LABEL_KIND.sub("", self.nodes[identity].label) for identity in self.also_named_identities(point, exclude)
        ][:ALSO_NAMED_LIMIT]

    def also_named_identities(self, point: str, exclude: set[str]) -> list[str]:
        words = [
            word
            for word in re.findall(r"[a-z]+", _fold(point))
            if len(word) > 1 and word not in GENERIC_NAME_WORDS and word not in SPOKEN_FILLER_WORDS
        ]
        found = []
        for name, entries in self.names.items():
            tokens = set(re.findall(r"[a-z]+", name))
            if words and all(word in tokens for word in words):
                for _, identity in entries:
                    node = self.nodes[identity]
                    if identity not in exclude and node.kind not in {"dante_rx", "dante_tx"} and identity not in found:
                        found.append(identity)
        return sorted(found, key=_natural)

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
                    **{word: 3 for word in type_words(device.get("device_type"))},
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
                        f"{name} {direction} {number}"
                        + (f" {label}" if label != str(number) else "")
                        + (" (Dante, off the network)" if record.get("online") is False else " (Dante)"),
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
            if tx_number is None or (transmitter or {}).get("online") is False:
                note = None
            if record.get("remembered_last_seen"):
                note = f"its last known route, from {record['remembered_last_seen']}"
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
            return {**names, **{f"{self.host} {name}": priority for name, priority in names.items() if name}}
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
    graph.local_host = next((scope.host for scope, _ in hosts if not scope.remote), None)
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
        state, text = _jack_level((jack_levels.get(node.details["host"]) or {}).get(node.details["port"]))
        stage = node.details.get("stage")
        if stage and node.details.get("direction") == "output":
            if stage["muted"] and state != "signal":
                return "silent", "muted at this volume stage"
            if stage.get("level") is not None and stage["level"] != 100:
                text += f", volume stage at {stage['level']:g}%"
        return state, text
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
            text += f" {step.note}" if step.note.startswith("through ") else f" via {step.note}"
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
            stage = graph.nodes[downstream].details.get("stage") or {}
            if stage.get("muted"):
                stops.append(f"muted at {_stage_name(graph.nodes[downstream])}")
            else:
                stops.append(
                    f"{LABEL_KIND.sub('', graph.nodes[downstream].label)} is silent although "
                    f"{LABEL_KIND.sub('', graph.nodes[upstream].label)} has signal"
                )
    return lines, stops


def _joined(names: list[str]) -> str:
    shown = names[:SOURCE_LIMIT]
    text = ", ".join(shown[:-1]) + (f" and {shown[-1]}" if len(shown) > 1 else shown[0])
    return text + (f" and {len(names) - SOURCE_LIMIT} more" if len(names) > SOURCE_LIMIT else "")


def _host_of(node: Node, identity: str) -> str | None:
    host = node.details.get("host") or (identity.split("@", 1)[1].split(":", 1)[0] if "@" in identity else None)
    return host if host and node.label.startswith(f"{host} ") else None


def _grouped(entries: list[tuple[str | None, str]], local_host: str | None) -> str:
    names: dict[str, list[str]] = {}
    for host, name in entries:
        where = host or local_host
        if where not in names.setdefault(name, []):
            names[name].append(where)
    if all(hosts == [local_host] for hosts in names.values()):
        return _joined(list(names))
    by_hosts: dict[tuple, list[str]] = {}
    for name, hosts in names.items():
        by_hosts.setdefault(tuple(hosts), []).append(name)
    return ", ".join(
        f"{_joined(group)} on {_joined([str(host) for host in hosts])}" for hosts, group in by_hosts.items()
    )


def _stage_name(node: Node) -> str:
    client = node.details["port"].split(":")[0]
    name = (node.details.get("stage") or {}).get("name")
    host = node.details.get("host")
    where = f" on {host}" if node.label.startswith(f"{host} ") else ""
    return f"{client} ({name}){where}" if name else f"{client}{where}"


def _is_muted_stage(node: Node) -> bool:
    return (
        node.kind == "jack"
        and node.details.get("direction") == "output"
        and bool((node.details.get("stage") or {}).get("muted"))
    )


def mute_wording(graph: SignalGraph, starts: list[str]) -> str:
    if any(_is_muted_stage(graph.nodes[identity]) for identity in starts):
        return "it is muted at"
    seen = set(starts)
    pending = list(starts)
    muted_upstream = bypassed = False
    while pending:
        identity = pending.pop()
        node = graph.nodes.get(identity)
        if node is None:
            continue
        if _is_muted_stage(node):
            muted_upstream = True
            continue
        sources = [source for source, _ in graph.upstream.get(identity, ())]
        if not sources:
            bypassed = True
        for source in sources:
            if source not in seen:
                seen.add(source)
                pending.append(source)
    if not muted_upstream:
        return "it is muted further on, at"
    return "some of its inputs are muted at" if bypassed else "it is muted at"


def muted_stages(graph: SignalGraph, identities: list[str]) -> list[str]:
    return list(
        dict.fromkeys(
            _stage_name(graph.nodes[identity])
            for identity in identities
            if graph.nodes[identity].kind == "jack"
            and graph.nodes[identity].details.get("direction") == "output"
            and (graph.nodes[identity].details.get("stage") or {}).get("muted")
        )
    )


def silence_causes(
    graph: SignalGraph,
    starts: list[str],
    steps: list[Step],
    levels: dict[str, tuple[str, str]],
    muted: list[str],
) -> list[str] | None:
    identities = list(dict.fromkeys(starts + [step.identity for step in steps if not step.repeated]))
    if any(levels.get(identity, ("unknown", ""))[0] in {"signal", "quiet"} for identity in identities):
        return None
    sources = [identity for identity in identities if not graph.upstream.get(identity)]
    if not sources:
        return None
    first = graph.nodes[starts[0]]
    consumer = first.kind in {"pulse_sink", "dante_rx"} or (
        first.kind == "jack" and first.details.get("direction") == "input"
    )
    if sources == starts and not steps and consumer:
        return [f"nothing feeds {LABEL_KIND.sub('', first.label)}"]
    offline: list[str] = []
    carried: dict[str, list[str]] = {}
    quiet_channels: dict[str, list[str]] = {}
    silent_transmitters: dict[str, list[str]] = {}
    groups: dict[str, list[tuple[str | None, str]]] = {
        "paused": [],
        "muted": [],
        "playing but silent": [],
        "nothing playing to": [],
        "sending silence": [],
        "not measured": [],
    }
    others: list[str] = []
    for identity in sources:
        node = graph.nodes[identity]
        text = levels.get(identity, ("unknown", "not measured"))[1]
        host = _host_of(node, identity)
        if node.kind in {"dante_rx", "dante_tx"} and node.details.get("online") is False:
            offline.append(node.details["device"])
            for following, _ in graph.downstream.get(identity, ()):
                receiver = graph.nodes.get(following)
                if receiver is not None and receiver.kind == "dante_rx":
                    label = LABEL_KIND.sub("", receiver.label).split(" ", 3)
                    carried.setdefault(node.details["device"], []).append(label[3] if len(label) > 3 else label[-1])
        elif node.kind in {"pulse_playback", "pulse_recording"}:
            entry = node.details["entry"]
            application = entry.get("application") or entry.get("binary") or "an application"
            condition = "paused" if entry.get("corked") else "muted" if entry.get("mute") else "playing but silent"
            groups[condition].append((host, application))
        elif node.kind == "pulse_sink":
            groups["nothing playing to"].append((host, node.details["entry"]["name"]))
        elif node.kind == "jack":
            measured_state = levels.get(identity, ("unknown", ""))[0]
            condition = "not measured" if measured_state == "unknown" else "sending silence"
            groups[condition].append((host, node.details["port"].split(":")[0]))
        elif node.kind == "dante_tx":
            device = node.details["device"]
            channel = TX_NUMBER.sub("", LABEL_KIND.sub("", node.label).removeprefix(f"{device} "))
            silent_transmitters.setdefault(device, []).append(channel)
        elif node.kind == "dante_rx":
            device = node.details["device"]
            channel = LABEL_KIND.sub("", node.label).removeprefix(f"{device} ")
            quiet_channels.setdefault(device, []).append(channel)
        else:
            others.append(f"{LABEL_KIND.sub('', node.label)} is {text}")
    parts = []
    offline = list(dict.fromkeys(offline))
    if offline:
        named = [
            f"{device} ({_joined(list(dict.fromkeys(carried[device])))})" if carried.get(device) else device
            for device in offline
        ]
        parts.append(f"{_joined(named)} {'is' if len(offline) == 1 else 'are'} off the network")
        unpowered = list(
            dict.fromkeys(
                str(node.details["outlet"].get("name"))
                for node in (graph.nodes[identity] for identity in sources)
                if node.details.get("device") in offline
                and isinstance(node.details.get("outlet"), dict)
                and node.details["outlet"].get("state") == "off"
            )
        )
        if unpowered:
            parts[-1] += f", switched off at {'outlet' if len(unpowered) == 1 else 'outlets'} {_joined(unpowered)}"
    for condition, entries in groups.items():
        if not entries:
            continue
        if condition == "nothing playing to":
            parts.append(f"nothing is playing to {_grouped(entries, graph.local_host)}")
        elif condition == "sending silence":
            parts.append(f"{_grouped(entries, graph.local_host)} {'sends' if len(entries) == 1 else 'send'} silence")
        elif condition == "not measured":
            parts.append(f"{_grouped(entries, graph.local_host)} {'was' if len(entries) == 1 else 'were'} not measured")
        else:
            parts.append(f"{_grouped(entries, graph.local_host)} {'is' if len(entries) == 1 else 'are'} {condition}")
    for device, channels in silent_transmitters.items():
        parts.append(f"{device} sends silence on {_joined(channels)}")
    for device, channels in quiet_channels.items():
        parts.append(f"{device} {_joined(channels)} {'is' if len(channels) == 1 else 'are'} silent")
    parts.extend(others[:SOURCE_LIMIT])
    if muted:
        parts.append(f"it is muted at {_joined(muted)}")
    return parts


def silence_summary(causes: list[str], wording: str = "it is muted at") -> str:
    causes = [
        cause.replace("it is muted at", wording, 1) if cause.startswith("it is muted at ") else cause
        for cause in causes
    ]
    text = f"No signal: {causes[0]}."
    if len(causes) > 1:
        text += " Also, " + "; ".join(causes[1:]) + "."
    return text if causes[0].startswith("nothing feeds ") else text + " Every step of this chain is silent."


def _subject(point: str) -> str:
    plain = point.replace(" ", "").isalpha() and point[:1].islower()
    return point[:1].upper() + point[1:] if plain else point


def _has_signal(levels: dict[str, tuple[str, str]], identity: str) -> bool:
    return levels.get(identity, ("unknown", ""))[0] in {"signal", "quiet"}


def trace_answer(
    graph: SignalGraph,
    point: str,
    starts: list[str],
    walks: dict[str, list[Step]],
    levels: dict[str, tuple[str, str]] | None,
    stops: list[str],
    causes: list[str] | None,
    related: list[str] | None = None,
) -> str:
    subject = _subject(point)
    if not any(walks.values()):
        names = [LABEL_KIND.sub("", graph.nodes[identity].label) for identity in starts]
        shown = f" ({_joined(names)})" if names != [point] else ""
        paths = list(dict.fromkeys(name if "[" in name else name.split(":")[0] for name in related or []))
        others = f"; other points with that name: {_joined(paths[:RELATED_ANSWER_LIMIT])}" if paths else ""
        return f"Nothing is connected to {point}{shown}{others}."
    downstream = walks.get("downstream") or []
    upstream = walks.get("upstream") or []
    roots = [step.identity for step in upstream if not step.repeated and not graph.upstream.get(step.identity)]
    if levels is None:
        reached = destination_parts(graph, downstream)
        if reached:
            return f"{subject} reaches {'; '.join(reached)}."
        if roots:
            return f"{subject} is fed by {_joined([LABEL_KIND.sub('', graph.nodes[root].label) for root in roots])}."
        return f"Nothing is connected to {point}."
    if causes:
        muted = next((cause for cause in causes if cause.startswith("it is muted at ")), None)
        sources = [cause for cause in causes if cause != muted]
        if all(graph.nodes[identity].details.get("online") is False for identity in starts):
            return f"{subject} is off the network" + (f", and {muted}" if muted else "") + "."
        if not sources:
            return f"{subject} has no signal because {muted}."
        unmeasured = any(cause.endswith(" not measured") for cause in sources)
        everything = "no measured source feeding it has signal" if unmeasured else "nothing feeding it has signal"
        joiner = ", and " if any("," in source for source in sources) else " and "
        reason = joiner.join(sources) if len(sources) <= ANSWER_CAUSE_LIMIT else everything
        wording = mute_wording(graph, starts) if muted else None
        if wording is None or (reason == everything and wording.startswith("some of its inputs")):
            also = None
        else:
            also = muted.replace("it is muted at", wording.replace(" muted", " also muted", 1), 1)
        return f"{subject} has no signal because {reason}" + (f", and {also}" if also else "") + "."
    signal = any(_has_signal(levels, identity) for identity in starts)
    if stops:
        muted = list(dict.fromkeys(stop.removeprefix("muted at ") for stop in stops if stop.startswith("muted at ")))
        if muted:
            return f"{subject} {'has signal, but it' if signal else 'has no signal because it'} is muted at {_joined(muted)}."
        return f"{subject} {'has signal, but' if signal else 'has no signal:'} {stops[0]}."
    if not signal:
        measured = any(levels.get(identity, ("unknown", ""))[0] != "unknown" for identity in starts)
        return f"{subject} has no signal." if measured else f"{subject}'s level could not be measured."
    walked = [step.identity for steps in walks.values() for step in steps if not step.repeated]
    clear = not any(levels.get(identity, ("unknown", ""))[0] == "silent" for identity in walked)
    ending = "; nothing on the way is muted or silent." if clear and walked else "."
    reached = destination_parts(graph, downstream, lambda identity: _has_signal(levels, identity))
    if reached:
        return f"{subject} has signal and reaches {'; '.join(reached)}{ending}"
    sources = [LABEL_KIND.sub("", graph.nodes[root].label) for root in roots if _has_signal(levels, root)]
    if sources:
        return f"{subject} has signal, from {_joined(sources)}{ending}"
    return f"{subject} has signal{ending}"


@dataclass
class PathResult:
    name: str
    starts: list[str]
    walks: dict[str, list[Step]]
    stops: list[str]
    causes: list[str] | None
    signal: bool
    output: bool
    answer: str = ""


def _is_output(node: Node) -> bool:
    hardware = node.details.get("hardware") or {}
    return node.kind == "pulse_sink" or (node.kind == "jack" and hardware.get("direction") == "playback")


def _path_name(graph: SignalGraph, node: Node) -> str:
    base = LABEL_KIND.sub("", node.label)
    if node.kind != "jack" or " [" in base:
        return base
    name = base.rsplit(":", 1)[0]
    host = node.details.get("host")
    return name if not host or name.startswith(f"{host} ") else f"{host} {name}"


def related_paths(graph: SignalGraph, identities: list[str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for identity in identities:
        if graph.upstream.get(identity) or graph.downstream.get(identity):
            groups.setdefault(_path_name(graph, graph.nodes[identity]), []).append(identity)
    outputs = {name: group for name, group in groups.items() if all(_is_output(graph.nodes[i]) for i in group)}
    return dict(list((outputs or groups).items())[:RELATED_FOLLOW_LIMIT])


def path_result(
    graph: SignalGraph, name: str, starts: list[str], directions: tuple[str, ...], levels: dict[str, tuple[str, str]]
) -> PathResult:
    walks = {direction: graph.walk(starts, direction) for direction in directions}
    stops = [stop for direction in directions for stop in render(graph, starts, walks[direction], direction, levels)[1]]
    identities = list(dict.fromkeys(starts + [step.identity for steps in walks.values() for step in steps]))
    walked = walks.get("upstream", walks.get("downstream", []))
    causes = None if stops else silence_causes(graph, starts, walked, levels, muted_stages(graph, identities))
    result = PathResult(
        name,
        starts,
        walks,
        list(dict.fromkeys(stops)),
        causes,
        any(_has_signal(levels, identity) for identity in starts),
        all(_is_output(graph.nodes[identity]) for identity in starts),
    )
    result.answer = trace_answer(graph, name, starts, walks, levels, result.stops, causes)
    return result


def related_answer(point: str, results: list[PathResult], unconnected: list[str]) -> str:
    shown = [result for result in results if result.output] or results
    loud = [result.name for result in shown if result.signal]
    quiet = [result.name for result in shown if not result.signal]
    if loud:
        lead = f"{_joined(loud)} {'has' if len(loud) == 1 else 'have'} signal now" + (
            f"; {_joined(quiet)} {'is' if len(quiet) == 1 else 'are'} silent" if quiet else ""
        )
    else:
        kind = "output" if shown[0].output else "path"
        lead = (
            f"No {kind} named after {point} has signal right now: "
            f"{_joined(quiet)} {'is' if len(quiet) == 1 else 'are'} silent"
        )
    details = [result.answer for result in sorted(shown, key=lambda result: not result.signal)]
    return " ".join([lead + ".", *details, f"Nothing is connected to {_joined(unconnected)}."])


def device_answer(
    graph: SignalGraph, point: str, starts: list[str], directions: tuple[str, ...], levels: dict[str, tuple[str, str]]
) -> str | None:
    transmitters = [identity for identity in starts if graph.nodes[identity].kind == "dante_tx"]
    receivers = [identity for identity in starts if graph.nodes[identity].kind == "dante_rx"]
    if not transmitters or not receivers:
        return None
    device = graph.nodes[starts[0]].details["device"]
    sends = path_result(graph, "what it sends", transmitters, directions, levels)
    receives = path_result(graph, "what it receives", receivers, directions, levels)
    lead = (
        f"{_subject(point)} sends {'signal' if sends.signal else 'silence'} and "
        f"receives {'signal' if receives.signal else 'nothing'}."
    )
    sentences = [lead]
    for result, unrouted in ((sends, "Nothing receives what it sends."), (receives, "Nothing is routed to it.")):
        if not any(result.walks.values()):
            sentences.append(unrouted)
            continue
        causes = [cause for cause in result.causes or [] if not cause.startswith(f"{device} sends silence")]
        if result.causes is not None and not causes:
            continue
        if result.causes is not None:
            result.causes = causes
            muted = [cause for cause in causes if cause.startswith("it is muted at ")]
            if muted and len(muted) == len(causes):
                stage = muted[0].removeprefix("it is muted at ")
                sentences.append(f"{_subject(result.name)} is also muted further on, at {stage}.")
                continue
            result.answer = trace_answer(graph, result.name, result.starts, result.walks, levels, result.stops, causes)
        sentences.append(result.answer)
    return " ".join(sentences)


def destinations_summary(graph: SignalGraph, steps: list[Step]) -> str | None:
    parts = destination_parts(graph, steps)
    return "It ends at: " + "; ".join(parts) + "." if parts else None


def destination_parts(graph: SignalGraph, steps: list[Step], keep: Callable[[str], bool] | None = None) -> list[str]:
    groups: dict[str, list[tuple[str | None, str]]] = {}
    dante: dict[str, list[str]] = {}
    for step in steps:
        if step.repeated or graph.downstream.get(step.identity):
            continue
        if keep is not None and not keep(step.identity):
            continue
        node = graph.nodes[step.identity]
        host = _host_of(node, step.identity)
        if node.kind == "pulse_recording":
            entry = node.details["entry"]
            parent = graph.nodes.get(step.parent or "")
            if (
                entry.get("media") == "Peak detect"
                and parent is not None
                and parent.kind == "pulse_source"
                and not parent.details["entry"].get("monitor_of")
            ):
                name = parent.details["entry"]["name"]
                groups.setdefault("PulseAudio inputs", []).append((_host_of(parent, step.parent), name))
                continue
            if entry.get("media") == "Peak detect":
                continue
            groups.setdefault("recorded by", []).append((host, entry.get("application") or "an application"))
        elif node.kind == "pulse_source":
            groups.setdefault("PulseAudio inputs", []).append((host, node.details["entry"]["name"]))
        elif node.kind == "pulse_sink":
            groups.setdefault("PulseAudio outputs", []).append((host, node.details["entry"]["name"]))
        elif node.kind in {"dante_rx", "dante_tx"}:
            device = node.details["device"]
            dante.setdefault(device, []).append(LABEL_KIND.sub("", node.label).removeprefix(f"{device} "))
        elif node.kind == "jack":
            hardware = node.details.get("hardware")
            name = node.label.split(" [", 1)[1].split("]", 1)[0] if " [" in node.label else node.details["port"]
            if node.details.get("direction") == "output":
                key = "JACK ports with nothing connected:"
            else:
                key = f"sound card {hardware['card']}" if hardware else "JACK ports"
            groups.setdefault(key, []).append((host, name))
    parts = [f"{kind} {_grouped(entries, graph.local_host)}" for kind, entries in groups.items()]
    parts.extend(f"{device} {_joined(channels)}" for device, channels in dante.items())
    return parts


def _peak(text: str) -> float | None:
    match = PEAK_TEXT.search(text)
    return float(match.group(1)) if match else None


def level_changes(
    graph: SignalGraph, steps: list[Step], direction: str, levels: dict[str, tuple[str, str]]
) -> list[str]:
    changes = []
    for step in steps:
        if step.repeated or step.parent is None or not (step.note or "").startswith("through "):
            continue
        before, after = (step.parent, step.identity) if direction == "downstream" else (step.identity, step.parent)
        before_state, before_text = levels.get(before, ("unknown", ""))
        after_state, after_text = levels.get(after, ("unknown", ""))
        if before_state != "signal" or after_state != "signal":
            continue
        first, second = _peak(before_text), _peak(after_text)
        if first is None or second is None or abs(second - first) < LEVEL_CHANGE_DB:
            continue
        client = step.note.removeprefix("through ").split(",", 1)[0]
        verb = "lowers" if second < first else "raises"
        changes.append(f"{client} {verb} it by {abs(second - first):.0f} dB ({first:g} to {second:g} dBFS peak)")
    return list(dict.fromkeys(changes))
