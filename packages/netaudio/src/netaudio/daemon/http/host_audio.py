from __future__ import annotations

import asyncio
import logging
from typing import Any

from netaudio.common.config_loader import load_config_document
from netaudio.daemon.http.mcp_views import _dante_match, utc_now
from netaudio.host_audio.alsa import is_dante_interface
from netaudio.host_audio.peers import PeerError
from netaudio.host_audio.trace import (
    HostScope,
    SignalGraph,
    build_graph,
    jack_meter_targets,
    node_level,
    render,
)

logger = logging.getLogger("netaudio")

DIRECTIONS = ("upstream", "downstream")
MAXIMUM_METERED_PORTS = 32
DEFAULT_METER_SECONDS = 0.5
METERING_TIMEOUT_SECONDS = 4.0
PEER_TIMEOUT_SECONDS = 3.0
SUMMARY_PEER_TIMEOUT_SECONDS = 1.5
POINT_FORMS = (
    "a Shure channel (AD4D-A ch1), a Dante channel (lx-dante rx 1), a JACK port or alias, "
    "a PulseAudio input or output, or an application name; add @computer for another computer"
)


def _hexadecimal(value: Any) -> str:
    return "".join(character for character in str(value or "").lower() if character in "0123456789abcdef")[:12]


def _first(query: dict, name: str, default: str | None = None) -> str | None:
    values = query.get(name)
    return values[-1] if values else default


def _applications(snapshot: dict) -> str:
    pulse = snapshot.get("pulse") or {}
    names = sorted(
        {
            entry.get("application") or entry.get("binary")
            for entry in (pulse.get("sink_inputs") or []) + (pulse.get("source_outputs") or [])
            if entry.get("application") or entry.get("binary")
        }
    )
    return ", ".join(names) or "none"


class DaemonHostAudioHandlers:
    host_audio: Any
    shure: Any
    metering: Any

    def _wireless_links(self, records: dict, wireless: dict) -> dict[str, str]:
        try:
            correlations = (load_config_document().get("shure") or {}).get("correlations") or {}
        except ValueError as exception:
            logger.warning("Unable to read Shure correlations: %s", exception)
            correlations = {}
        by_mac = {
            _hexadecimal(record.get("mac_address")): record.get("name")
            for record in records.values()
            if isinstance(record, dict) and record.get("mac_address")
        }
        links = {}
        for mac, device in wireless.items():
            linked = next(
                (
                    by_mac.get(_hexadecimal(dante_mac))
                    for shure_mac, dante_mac in correlations.items()
                    if _hexadecimal(shure_mac) == _hexadecimal(mac)
                ),
                None,
            )
            if linked is None and device.get("device_type") == "ad4d":
                match = _dante_match(device.get("device_type"), records)
                linked = match.get("name") if match else None
            if linked:
                links[mac] = linked
        return links

    def _wireless_payload(self) -> dict:
        if not self.shure:
            return {}
        return {mac: device.to_json() for mac, device in self.shure.devices.items()}

    async def _peer_snapshots(self, timeout: float = PEER_TIMEOUT_SECONDS) -> tuple[dict[str, dict], list[str]]:
        peers = self.host_audio.peers
        hosts = sorted(peers.peers)
        if not hosts:
            return {}, []
        results = await asyncio.gather(
            *(peers.request(host, "GET", "/host-audio", timeout=timeout) for host in hosts), return_exceptions=True
        )
        snapshots: dict[str, dict] = {}
        notes: list[str] = []
        for host, result in zip(hosts, results):
            if isinstance(result, PeerError):
                notes.append(str(result))
                continue
            if isinstance(result, BaseException):
                raise result
            status, payload = result
            if status == 404:
                notes.append(f"the netaudio server on {host} is too old to report its audio")
            elif status >= 400 or not isinstance(payload, dict) or "jack" not in payload:
                notes.append(f"the netaudio server on {host} did not report its audio (HTTP {status})")
            else:
                snapshots[host] = payload
        return snapshots, notes

    async def _signal_graph(self) -> tuple[SignalGraph, dict[str, dict], dict, list[str]]:
        await self.host_audio.ensure()
        local = self.host_audio.snapshot()
        peer_snapshots, notes = await self._peer_snapshots()
        records = self._serialized_devices()
        wireless = self._wireless_payload()
        snapshots = {local["host"]: local, **peer_snapshots}
        graph = build_graph(
            records=records,
            wireless=wireless,
            wireless_links=self._wireless_links(records, wireless),
            hosts=[(HostScope(host, remote=host != local["host"]), value) for host, value in snapshots.items()],
        )
        return graph, snapshots, records, notes

    async def _dante_levels(self, graph: SignalGraph, identities: list[str], records: dict) -> dict[str, dict]:
        if not self.metering:
            return {}
        devices = {
            graph.nodes[identity].details["device"]
            for identity in identities
            if graph.nodes[identity].kind in {"dante_rx", "dante_tx"} and graph.nodes[identity].details.get("online")
        }
        if devices:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*(self._request_detailed_metering(device) for device in sorted(devices))),
                    METERING_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                logger.info("Dante metering for a signal trace did not finish in time")
        names = {
            record.get("server_name"): record.get("name")
            for record in records.values()
            if isinstance(record, dict) and record.get("server_name")
        }
        levels = {}
        for server_name, cached in self.metering.get_cached_levels_by_server().items():
            name = names.get(server_name)
            if name in devices:
                levels[name] = {"rx": cached.get("rx_normalized") or {}, "tx": cached.get("tx_normalized") or {}}
        return levels

    async def _host_meter(self, host: str, targets: dict[str, list[str]], seconds: float) -> dict[str, dict]:
        if host == self.host_audio.host:
            return await self.host_audio.meter.measure(targets, seconds)
        try:
            status, payload = await self.host_audio.peers.request(
                host, "POST", "/host-audio/meter", {"targets": targets, "seconds": seconds}, timeout=seconds + 4
            )
        except PeerError as exception:
            return {name: {"error": str(exception)} for name in targets}
        if status >= 400 or not isinstance(payload, dict):
            error = payload.get("error") if isinstance(payload, dict) else None
            return {name: {"error": error or f"{host} could not meter (HTTP {status})"} for name in targets}
        return payload

    async def _levels(
        self, graph: SignalGraph, identities: list[str], snapshots: dict[str, dict], records: dict, seconds: float
    ) -> tuple[dict[str, tuple[str, str]], list[str]]:
        notes = []
        targets = {
            host: ports
            for host, ports in jack_meter_targets(graph, identities, snapshots).items()
            if ((snapshots.get(host) or {}).get("jack") or {}).get("available")
        }
        for host, ports in list(targets.items()):
            if len(ports) > MAXIMUM_METERED_PORTS:
                notes.append(f"measured the first {MAXIMUM_METERED_PORTS} of {len(ports)} JACK ports on {host}")
                targets[host] = dict(list(ports.items())[:MAXIMUM_METERED_PORTS])
        hosts = sorted(targets)
        measured = await asyncio.gather(
            *(self._host_meter(host, targets[host], seconds) for host in hosts),
            self._dante_levels(graph, identities, records),
        )
        jack_levels = dict(zip(hosts, measured[:-1]))
        dante_levels = measured[-1]
        levels = {}
        for identity in identities:
            level = node_level(graph, identity, jack_levels, dante_levels)
            if level is not None:
                levels[identity] = level
        return levels, notes

    @staticmethod
    def _unlinked_cards(graph: SignalGraph, identities: list[str], snapshots: dict[str, dict]) -> list[str]:
        found = set()
        for identity in identities:
            node = graph.nodes[identity]
            hardware = node.details.get("hardware") if node.kind == "jack" else None
            if not hardware:
                continue
            snapshot = snapshots.get(node.details["host"]) or {}
            linked = set((snapshot.get("card_links") or {}).values())
            dante_cards = {card["id"] for card in snapshot.get("cards") or [] if is_dante_interface(card)}
            if hardware["card"] not in linked and hardware["card"] in dante_cards:
                found.add((node.details["host"], hardware["card"]))
        return [
            f"JACK ports on ALSA card {card} on {host} are not linked to a Dante device, so the trace stops there; "
            "link_audio_card on that computer links them"
            for host, card in sorted(found)
        ]

    @staticmethod
    def _availability_notes(snapshots: dict[str, dict]) -> list[str]:
        return [
            f"{label} on {host} is not available: {snapshot[key].get('reason')}"
            for host, snapshot in snapshots.items()
            for key, label in (("jack", "JACK"), ("pulse", "PulseAudio"))
            if not (snapshot.get(key) or {}).get("available")
        ]

    async def host_audio_trace(
        self, point: str, direction: str = "both", levels: bool = True, seconds: float = DEFAULT_METER_SECONDS
    ) -> tuple[int, dict]:
        if self.host_audio is None:
            return 503, {"error": "host audio support is not running"}
        graph, snapshots, records, peer_notes = await self._signal_graph()
        local_host = self.host_audio.host
        starts, suggestions = graph.resolve(point)
        if not starts:
            hint = f"; did you mean {' or '.join(repr(value) for value in suggestions)}?" if suggestions else ""
            return 404, {
                "error": f"nothing named {point!r} in the signal chain{hint}",
                "applications_using_audio": {host: _applications(snapshot) for host, snapshot in snapshots.items()},
                "accepts": POINT_FORMS,
                "notes": peer_notes or None,
            }
        directions = DIRECTIONS if direction == "both" else (direction,)
        walks = {name: graph.walk(starts, name) for name in directions}
        identities = list(dict.fromkeys(starts + [step.identity for steps in walks.values() for step in steps]))
        measured, notes = await self._levels(graph, identities, snapshots, records, seconds) if levels else ({}, [])
        payload: dict[str, Any] = {
            "host": local_host,
            "other_computers": (
                f"also included: {', '.join(sorted(host for host in snapshots if host != local_host))}; name their "
                "points with the computer first, such as workstation default input"
                if len(snapshots) > 1
                else None
            ),
            "point": point,
            "start": [
                graph.nodes[identity].label + (f": {measured[identity][1]}" if identity in measured else "")
                for identity in starts
            ],
        }
        stops: list[str] = []
        for name in directions:
            lines, direction_stops = render(graph, starts, walks[name], name, measured)
            payload[name] = lines or [f"nothing {name} of this point is known"]
            remaining = graph.remaining(starts, name, walks[name])
            if remaining:
                payload[f"more_{name}"] = f"{remaining} more steps not shown; trace from a later point to see them"
            stops.extend(direction_stops)
        if levels:
            payload["signal_stops"] = list(dict.fromkeys(stops)) or "no step where signal stops"
            payload["measured_at"] = utc_now()
        notes.extend(self._unlinked_cards(graph, identities, snapshots))
        notes.extend(peer_notes)
        notes.extend(self._availability_notes(snapshots))
        if notes:
            payload["notes"] = notes
        return 200, payload

    async def host_audio_point_levels(
        self, points: list[str], seconds: float = DEFAULT_METER_SECONDS
    ) -> tuple[int, dict]:
        if self.host_audio is None:
            return 503, {"error": "host audio support is not running"}
        graph, snapshots, records, peer_notes = await self._signal_graph()
        resolved: dict[str, list[str]] = {}
        errors = {}
        for point in points:
            starts, suggestions = graph.resolve(point)
            if starts:
                resolved[point] = starts
            else:
                errors[point] = (
                    f"not found; did you mean {' or '.join(repr(value) for value in suggestions)}?"
                    if suggestions
                    else f"not found; accepts {POINT_FORMS}"
                )
        identities = list(dict.fromkeys(identity for starts in resolved.values() for identity in starts))
        measured, notes = await self._levels(graph, identities, snapshots, records, seconds)
        notes.extend(peer_notes)
        payload: dict[str, Any] = {
            point: [
                graph.nodes[identity].label + (f": {measured[identity][1]}" if identity in measured else ": no level")
                for identity in starts
            ]
            for point, starts in resolved.items()
        }
        payload.update({point: error for point, error in errors.items()})
        payload["measured_at"] = utc_now()
        if notes:
            payload["notes"] = notes
        return (200 if resolved else 404), payload

    async def host_audio_snapshot(self, host: str | None = None) -> tuple[int, dict]:
        if self.host_audio is None:
            return 503, {"error": "host audio support is not running"}
        if host and host.casefold().removesuffix(".local") != self.host_audio.host.casefold():
            try:
                status, payload = await self.host_audio.peers.request(host, "GET", "/host-audio")
            except PeerError as exception:
                return 404, {"error": str(exception)}
            if not isinstance(payload, dict):
                return 502, {"error": f"{host} returned no audio report"}
            return status, payload
        await self.host_audio.ensure()
        return 200, self.host_audio.snapshot()

    async def _handle_get_host_audio(self, writer, query: dict) -> None:
        status, payload = await self.host_audio_snapshot(_first(query, "host"))
        await self._send_json(writer, payload, status)

    async def _handle_get_host_audio_trace(self, writer, query: dict) -> None:
        point = _first(query, "point")
        if not point:
            await self._send_json(writer, {"error": "point is required"}, 400)
            return
        direction = _first(query, "direction", "both") or "both"
        if direction not in {"both", *DIRECTIONS}:
            await self._send_json(writer, {"error": "direction must be both, upstream or downstream"}, 400)
            return
        try:
            seconds = float(_first(query, "seconds", str(DEFAULT_METER_SECONDS)) or DEFAULT_METER_SECONDS)
        except ValueError:
            await self._send_json(writer, {"error": "seconds must be a number"}, 400)
            return
        levels = _first(query, "levels", "1") not in {"0", "false", "no"}
        status, payload = await self.host_audio_trace(point, direction, levels, seconds)
        await self._send_json(writer, payload, status)

    async def _handle_host_audio_levels(self, writer, params: dict) -> None:
        points = params.get("points")
        if isinstance(points, str):
            points = [points]
        if not isinstance(points, list) or not points or not all(isinstance(point, str) for point in points):
            await self._send_json(writer, {"error": "points must be a list of names"}, 400)
            return
        seconds = params.get("seconds", DEFAULT_METER_SECONDS)
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            await self._send_json(writer, {"error": "seconds must be a number"}, 400)
            return
        status, payload = await self.host_audio_point_levels(points, float(seconds))
        await self._send_json(writer, payload, status)

    async def _handle_host_audio_meter(self, writer, params: dict) -> None:
        if self.host_audio is None:
            await self._send_json(writer, {"error": "host audio support is not running"}, 503)
            return
        targets = params.get("targets")
        seconds = params.get("seconds", DEFAULT_METER_SECONDS)
        valid = isinstance(targets, dict) and all(
            isinstance(name, str) and isinstance(sources, list) and all(isinstance(source, str) for source in sources)
            for name, sources in targets.items()
        )
        if not valid or len(targets) > MAXIMUM_METERED_PORTS:
            await self._send_json(
                writer,
                {"error": f"targets must map up to {MAXIMUM_METERED_PORTS} port names to the ports that feed them"},
                400,
            )
            return
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            await self._send_json(writer, {"error": "seconds must be a number"}, 400)
            return
        if not self.host_audio.jack.available:
            await self._send_json(writer, {"error": f"JACK is not available: {self.host_audio.jack.reason}"}, 503)
            return
        await self._send_json(writer, await self.host_audio.meter.measure(targets, float(seconds)))
