from __future__ import annotations

import asyncio
import logging
from typing import Any

from netaudio.common.config_loader import load_config_document
from netaudio.daemon.http.mcp_views import _dante_match, utc_now
from netaudio.host_audio.alsa import is_dante_interface
from netaudio.host_audio.trace import (
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


def _hexadecimal(value: Any) -> str:
    return "".join(character for character in str(value or "").lower() if character in "0123456789abcdef")[:12]


def _first(query: dict, name: str, default: str | None = None) -> str | None:
    values = query.get(name)
    return values[-1] if values else default


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

    async def _signal_graph(self) -> tuple[SignalGraph, dict, dict]:
        await self.host_audio.ensure()
        snapshot = self.host_audio.snapshot()
        records = self._serialized_devices()
        wireless = self._wireless_payload()
        graph = build_graph(
            records=records,
            wireless=wireless,
            wireless_links=self._wireless_links(records, wireless),
            jack=snapshot["jack"],
            pulse=snapshot["pulse"],
            cards=snapshot["cards"],
            card_links=snapshot["card_links"],
            bridges=snapshot["bridges"],
        )
        return graph, snapshot, records

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

    async def _levels(
        self, graph: SignalGraph, identities: list[str], snapshot: dict, records: dict, seconds: float
    ) -> tuple[dict[str, tuple[str, str]], list[str]]:
        notes = []
        ports = snapshot["jack"].get("ports") or {}
        targets = jack_meter_targets(graph, identities, ports)
        if len(targets) > MAXIMUM_METERED_PORTS:
            notes.append(f"measured the first {MAXIMUM_METERED_PORTS} of {len(targets)} JACK ports")
            targets = dict(list(targets.items())[:MAXIMUM_METERED_PORTS])
        jack_task = (
            self.host_audio.meter.measure(targets, seconds) if targets and snapshot["jack"].get("available") else None
        )
        dante_task = self._dante_levels(graph, identities, records)
        if jack_task is not None:
            jack_levels, dante_levels = await asyncio.gather(jack_task, dante_task)
        else:
            jack_levels, dante_levels = {}, await dante_task
        levels = {}
        for identity in identities:
            level = node_level(graph, identity, jack_levels, dante_levels)
            if level is not None:
                levels[identity] = level
        return levels, notes

    @staticmethod
    def _unlinked_cards(graph: SignalGraph, identities: list[str], snapshot: dict) -> list[str]:
        linked = set(snapshot["card_links"].values())
        dante_cards = {card["id"] for card in snapshot["cards"] if is_dante_interface(card)}
        cards = sorted(
            {
                hardware["card"]
                for identity in identities
                if graph.nodes[identity].kind == "jack"
                and (hardware := graph.nodes[identity].details.get("hardware"))
                and hardware["card"] not in linked
                and hardware["card"] in dante_cards
            }
        )
        return [
            f"JACK ports on ALSA card {card} are not linked to a Dante device, so the trace stops there; "
            "link_audio_card links them"
            for card in cards
        ]

    async def host_audio_trace(
        self, point: str, direction: str = "both", levels: bool = True, seconds: float = DEFAULT_METER_SECONDS
    ) -> tuple[int, dict]:
        if self.host_audio is None:
            return 503, {"error": "host audio support is not running"}
        graph, snapshot, records = await self._signal_graph()
        starts, suggestions = graph.resolve(point)
        if not starts:
            hint = f"; did you mean {' or '.join(repr(value) for value in suggestions)}?" if suggestions else ""
            pulse = snapshot["pulse"]
            applications = sorted(
                {
                    entry.get("application") or entry.get("binary")
                    for entry in (pulse.get("sink_inputs") or []) + (pulse.get("source_outputs") or [])
                    if entry.get("application") or entry.get("binary")
                }
            )
            return 404, {
                "error": f"nothing named {point!r} in the signal chain{hint}",
                "applications_using_audio": (
                    f"on {snapshot['host']}: {', '.join(applications)}"
                    if applications
                    else f"none on {snapshot['host']}"
                ),
                "accepts": (
                    "a Shure channel (AD4D-A ch1), a Dante channel (lx-dante rx 1), a JACK port or alias, "
                    "a PulseAudio input or output, or an application name"
                ),
            }
        directions = DIRECTIONS if direction == "both" else (direction,)
        walks = {name: graph.walk(starts, name) for name in directions}
        identities = list(dict.fromkeys(starts + [step.identity for steps in walks.values() for step in steps]))
        measured, notes = await self._levels(graph, identities, snapshot, records, seconds) if levels else ({}, [])
        payload: dict[str, Any] = {
            "host": snapshot["host"],
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
        notes.extend(self._unlinked_cards(graph, identities, snapshot))
        unavailable = [
            f"{label} is not available: {snapshot[key].get('reason')}"
            for key, label in (("jack", "JACK"), ("pulse", "PulseAudio"))
            if not snapshot[key].get("available")
        ]
        notes.extend(unavailable)
        if notes:
            payload["notes"] = notes
        return 200, payload

    async def host_audio_point_levels(
        self, points: list[str], seconds: float = DEFAULT_METER_SECONDS
    ) -> tuple[int, dict]:
        if self.host_audio is None:
            return 503, {"error": "host audio support is not running"}
        graph, snapshot, records = await self._signal_graph()
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
                    else "not found"
                )
        identities = list(dict.fromkeys(identity for starts in resolved.values() for identity in starts))
        measured, notes = await self._levels(graph, identities, snapshot, records, seconds)
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

    async def _handle_get_host_audio(self, writer, query: dict) -> None:
        if self.host_audio is None:
            await self._send_json(writer, {"error": "host audio support is not running"}, 503)
            return
        await self.host_audio.ensure()
        await self._send_json(writer, self.host_audio.snapshot())

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
