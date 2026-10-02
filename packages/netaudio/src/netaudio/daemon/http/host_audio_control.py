from __future__ import annotations

import asyncio
import difflib
import logging
import time
from typing import Any

from netaudio.host_audio.links import save_card_link

logger = logging.getLogger("netaudio")

SETTLE_SECONDS = 1.5
READBACK_TIMEOUT_SECONDS = 2.0
MAXIMUM_VOLUME_PERCENT = 150
PULSE_KINDS = {
    "sink": "output",
    "source": "input",
    "sink_input": "playback stream",
    "source_output": "recording stream",
}
DEFAULT_NAMES = {"default output": ("sink", "default_sink"), "default input": ("source", "default_source")}


def _fold(value: Any) -> str:
    return " ".join(str(value).split()).casefold()


def resolve_jack_port(ports: dict, name: Any) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name a JACK port, such as system:capture_1")
    if name in ports:
        return name
    wanted = _fold(name)
    matches = sorted(
        port
        for port, entry in ports.items()
        if wanted in {_fold(port), *(_fold(alias) for alias in entry.get("aliases") or ())}
        or (entry.get("pretty_name") and _fold(entry["pretty_name"]) == wanted)
    )
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"{name!r} names several JACK ports: {', '.join(matches)}")
    candidates = list(ports) + [alias for entry in ports.values() for alias in entry.get("aliases") or ()]
    suggestions = difflib.get_close_matches(name, candidates, n=4, cutoff=0.6)
    hint = f"; did you mean {' or '.join(suggestions)}?" if suggestions else ""
    raise ValueError(f"no JACK port named {name!r}{hint}")


def jack_change(jack: Any, params: dict, connect: bool) -> dict:
    if not jack.available:
        raise ValueError(f"JACK is not available: {jack.reason}")
    ports = jack.ports
    source = resolve_jack_port(ports, params.get("source"))
    destination = resolve_jack_port(ports, params.get("destination"))
    if ports[source]["direction"] == "input" and ports[destination]["direction"] == "output":
        source, destination = destination, source
    if ports[source]["direction"] != "output" or ports[destination]["direction"] != "input":
        raise ValueError(
            f"connect an output port to an input port; {source} and {destination} are both {ports[source]['direction']}s"
        )
    if ports[source]["type"] != ports[destination]["type"]:
        raise ValueError(f"{source} is {ports[source]['type']} but {destination} is {ports[destination]['type']}")
    return {
        "source": source,
        "destination": destination,
        "connected_now": jack.connected(source, destination),
        "requested": "connected" if connect else "disconnected",
        "source_feeds": ports[source]["connections"],
        "destination_hears": ports[destination]["connections"],
    }


def _pulse_entries(pulse: Any, kinds: tuple[str, ...]) -> list[tuple[str, dict]]:
    lists = {
        "sink": pulse.sinks,
        "source": pulse.sources,
        "sink_input": pulse.sink_inputs,
        "source_output": pulse.source_outputs,
    }
    return [(kind, entry) for kind in kinds for entry in lists[kind]]


def _entry_names(kind: str, entry: dict) -> set[str]:
    names = set()
    if kind in {"sink", "source"}:
        names.update({entry.get("name"), entry.get("description")})
    else:
        names.update(
            {
                entry.get("application"),
                entry.get("binary"),
                entry.get("media"),
                f"stream {entry['index']}",
                str(entry["index"]),
            }
        )
    return {_fold(name) for name in names if name}


def describe_pulse(kind: str, entry: dict) -> str:
    if kind in {"sink", "source"}:
        return f"{PULSE_KINDS[kind]} {entry['name']}"
    application = entry.get("application") or entry.get("binary") or "unknown application"
    return f"{application} {PULSE_KINDS[kind]} {entry['index']}"


def resolve_pulse(pulse: Any, target: Any, kinds: tuple[str, ...]) -> tuple[str, dict]:
    if not pulse.available:
        raise ValueError(f"PulseAudio is not available: {pulse.reason}")
    if isinstance(target, int) and not isinstance(target, bool):
        target = str(target)
    if not isinstance(target, str) or not target.strip():
        raise ValueError("name a PulseAudio output, input or application")
    wanted = _fold(target)
    if wanted in DEFAULT_NAMES:
        kind, key = DEFAULT_NAMES[wanted]
        if kind in kinds:
            name = pulse.server.get(key)
            entry = next((entry for entry in _pulse_entries(pulse, (kind,)) if entry[1]["name"] == name), None)
            if entry is not None:
                return entry
    matches = [(kind, entry) for kind, entry in _pulse_entries(pulse, kinds) if wanted in _entry_names(kind, entry)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        listed = ", ".join(describe_pulse(kind, entry) for kind, entry in matches)
        raise ValueError(f"{target!r} matches several: {listed}; name one by its stream number")
    available = ", ".join(describe_pulse(kind, entry) for kind, entry in _pulse_entries(pulse, kinds)) or "none"
    raise ValueError(
        f"no PulseAudio {' or '.join(PULSE_KINDS[kind] for kind in kinds)} named {target!r}; available: {available}"
    )


def pulse_volume_value(params: dict) -> float:
    percent = params.get("volume_percent")
    decibels = params.get("volume_db")
    if (percent is None) == (decibels is None):
        raise ValueError("give volume_percent or volume_db")
    if percent is not None:
        if (
            isinstance(percent, bool)
            or not isinstance(percent, (int, float))
            or not 0 <= percent <= MAXIMUM_VOLUME_PERCENT
        ):
            raise ValueError(f"volume_percent must be 0 to {MAXIMUM_VOLUME_PERCENT}")
        return float(percent) / 100
    if isinstance(decibels, bool) or not isinstance(decibels, (int, float)) or decibels > 11:
        raise ValueError("volume_db must be a number no higher than 11")
    return 10 ** (float(decibels) / 60)


def pulse_change(pulse: Any, action: str, params: dict) -> dict:
    if action == "volume":
        kind, entry = resolve_pulse(pulse, params.get("target"), tuple(PULSE_KINDS))
        value = pulse_volume_value(params)
        return {
            "target": describe_pulse(kind, entry),
            "kind": kind,
            "index": entry["index"],
            "value": value,
            "current": f"{'/'.join(str(item) for item in entry.get('volume_percent') or [])}%",
            "requested": f"{'/'.join(str(round(value * 100)) for _ in entry.get('volume_percent') or [0])}%",
        }
    if action == "mute":
        kind, entry = resolve_pulse(pulse, params.get("target"), tuple(PULSE_KINDS))
        mute = params.get("mute")
        if not isinstance(mute, bool):
            raise ValueError("mute must be true or false")
        return {
            "target": describe_pulse(kind, entry),
            "kind": kind,
            "index": entry["index"],
            "value": mute,
            "current": "muted" if entry.get("mute") else "not muted",
            "requested": "muted" if mute else "not muted",
        }
    if action == "default":
        kind, entry = resolve_pulse(pulse, params.get("target"), ("sink", "source"))
        key = "default_sink" if kind == "sink" else "default_source"
        return {
            "target": describe_pulse(kind, entry),
            "kind": kind,
            "index": entry["index"],
            "value": entry["name"],
            "current": f"default {PULSE_KINDS[kind]} is {pulse.server.get(key)}",
            "requested": f"default {PULSE_KINDS[kind]} is {entry['name']}",
        }
    if action == "move":
        kind, entry = resolve_pulse(pulse, params.get("stream"), ("sink_input", "source_output"))
        destination_kind = "sink" if kind == "sink_input" else "source"
        _, destination = resolve_pulse(pulse, params.get("destination"), (destination_kind,))
        current = entry.get("sink") if kind == "sink_input" else entry.get("source")
        return {
            "target": describe_pulse(kind, entry),
            "kind": kind,
            "index": entry["index"],
            "value": destination["index"],
            "current": f"on {current}",
            "requested": f"on {destination['name']}",
            "destination": destination["name"],
        }
    raise ValueError(f"unknown PulseAudio change {action!r}")


def pulse_readback(pulse: Any, action: str, change: dict) -> str:
    if action == "default":
        key = "default_sink" if change["kind"] == "sink" else "default_source"
        return f"default {PULSE_KINDS[change['kind']]} is {pulse.server.get(key)}"
    entry = pulse.find(change["kind"], change["index"])
    if entry is None:
        return "gone"
    if action == "volume":
        return f"{'/'.join(str(item) for item in entry.get('volume_percent') or [])}%"
    if action == "mute":
        return "muted" if entry.get("mute") else "not muted"
    return f"on {entry.get('sink') if change['kind'] == 'sink_input' else entry.get('source')}"


def card_change(snapshot: dict, records: dict, params: dict) -> dict:
    cards = {card["id"]: card for card in snapshot["cards"]}
    card = params.get("card")
    if not isinstance(card, str) or card not in cards:
        available = ", ".join(f"{card_id} ({entry['name']})" for card_id, entry in cards.items()) or "none"
        raise ValueError(f"no ALSA card {card!r} on {snapshot['host']}; cards: {available}")
    device = params.get("device")
    if device in ("", None):
        device = None
    else:
        names = {record.get("name") for record in records.values() if isinstance(record, dict)}
        if device not in names:
            suggestions = difflib.get_close_matches(str(device), [name for name in names if name], n=4, cutoff=0.6)
            hint = f"; did you mean {' or '.join(suggestions)}?" if suggestions else ""
            raise ValueError(f"no Dante device named {device!r}{hint}")
    current = next((name for name, linked in snapshot["card_links"].items() if linked == card), None)
    ports = sum(
        1
        for port in (snapshot["jack"].get("ports") or {}).values()
        if any(alias.startswith(f"alsa_pcm:hw:{card},") for alias in port.get("aliases") or ())
    )
    return {
        "card": card,
        "card_name": cards[card].get("long_name") or cards[card]["name"],
        "current": current,
        "device": device,
        "jack_ports": ports,
    }


class DaemonHostAudioControlHandlers:
    host_audio: Any

    async def _jack_write(self, writer, params: dict, connect: bool) -> None:
        jack = self.host_audio.jack if self.host_audio else None
        try:
            if jack is None:
                raise ValueError("host audio support is not running")
            change = jack_change(jack, params, connect)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        source, destination = change["source"], change["destination"]
        if change["connected_now"] == connect:
            await self._send_json(
                writer, {**change, "now": change["requested"], "result": "already so; nothing changed"}
            )
            return
        import jack as jack_library

        generation = jack.generation
        try:
            if connect:
                await jack.connect_ports(source, destination)
            else:
                await jack.disconnect_ports(source, destination)
        except jack_library.JackError as exception:
            await self._send_json(writer, {**change, "error": str(exception)}, 502)
            return
        logger.info("JACK %s %s -> %s", "connected" if connect else "disconnected", source, destination)
        deadline = time.monotonic() + READBACK_TIMEOUT_SECONDS
        while jack.connected(source, destination) != connect and time.monotonic() < deadline:
            if await jack.wait_changed(generation, deadline - time.monotonic()):
                generation = jack.generation
        settle_deadline = time.monotonic() + SETTLE_SECONDS
        while time.monotonic() < settle_deadline:
            if not await jack.wait_changed(generation, settle_deadline - time.monotonic()):
                break
            generation = jack.generation
        now = jack.connected(source, destination)
        result = "applied" if now == connect else "JACK accepted the change, then another JACK client reverted it"
        await self._send_json(
            writer,
            {
                **change,
                "now": "connected" if now else "disconnected",
                "result": result,
                "source_feeds": jack.ports.get(source, {}).get("connections"),
                "destination_hears": jack.ports.get(destination, {}).get("connections"),
            },
            200 if now == connect else 409,
        )

    async def _handle_jack_connect(self, writer, params: dict) -> None:
        await self._jack_write(writer, params, True)

    async def _handle_jack_disconnect(self, writer, params: dict) -> None:
        await self._jack_write(writer, params, False)

    async def _pulse_write(self, writer, params: dict, action: str) -> None:
        pulse = self.host_audio.pulse if self.host_audio else None
        try:
            if pulse is None:
                raise ValueError("host audio support is not running")
            change = pulse_change(pulse, action, params)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        import pulsectl

        generation = pulse.generation
        try:
            if action == "volume":
                await pulse.set_volume(change["kind"], change["index"], change["value"])
            elif action == "mute":
                await pulse.set_mute(change["kind"], change["index"], change["value"])
            elif action == "default":
                await pulse.set_default(change["kind"], change["index"])
            else:
                await pulse.move(change["kind"], change["index"], change["value"])
        except (pulsectl.PulseError, ConnectionError) as exception:
            await self._send_json(writer, {**change, "error": str(exception)}, 502)
            return
        logger.info("PulseAudio %s %s: %s", action, change["target"], change["requested"])
        deadline = time.monotonic() + READBACK_TIMEOUT_SECONDS
        while pulse_readback(pulse, action, change) != change["requested"] and time.monotonic() < deadline:
            if await pulse.wait_changed(generation, deadline - time.monotonic()):
                generation = pulse.generation
        now = pulse_readback(pulse, action, change)
        change.pop("value", None)
        await self._send_json(
            writer,
            {**change, "now": now, "result": "applied" if now == change["requested"] else "not confirmed"},
            200 if now == change["requested"] else 409,
        )

    async def _handle_pulse_volume(self, writer, params: dict) -> None:
        await self._pulse_write(writer, params, "volume")

    async def _handle_pulse_mute(self, writer, params: dict) -> None:
        await self._pulse_write(writer, params, "mute")

    async def _handle_pulse_default(self, writer, params: dict) -> None:
        await self._pulse_write(writer, params, "default")

    async def _handle_pulse_move(self, writer, params: dict) -> None:
        await self._pulse_write(writer, params, "move")

    async def _handle_link_card(self, writer, params: dict) -> None:
        if self.host_audio is None:
            await self._send_json(writer, {"error": "host audio support is not running"}, 503)
            return
        try:
            change = card_change(self.host_audio.snapshot(), self._serialized_devices(), params)
        except ValueError as exception:
            await self._send_json(writer, {"error": str(exception)}, 400)
            return
        path = await asyncio.get_running_loop().run_in_executor(None, save_card_link, change["card"], change["device"])
        logger.info("Linked ALSA card %s to Dante device %s in %s", change["card"], change["device"], path)
        await self._send_json(writer, {**change, "saved_in": str(path), "result": "applied"})
