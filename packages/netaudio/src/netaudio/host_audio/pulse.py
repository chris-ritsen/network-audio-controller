from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

from netaudio.host_audio.component import AudioComponent, utc_now

logger = logging.getLogger("netaudio")

CLIENT_NAME = "netaudio"
NO_INDEX = 0xFFFFFFFF


def _state(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "_value", value))


def _percent(values: list[float]) -> list[int]:
    return [round(value * 100) for value in values]


def _decibels(values: list[float]) -> list[float | None]:
    return [round(60 * math.log10(value), 1) if value > 0 else None for value in values]


def _volume(item: Any) -> dict:
    volume = getattr(item, "volume", None)
    values = list(getattr(volume, "values", None) or [])
    return {"volume_percent": _percent(values), "volume_db": _decibels(values)}


def _sample_spec(item: Any) -> str | None:
    spec = getattr(item, "sample_spec", None)
    if spec is None:
        return None
    rate = getattr(spec, "rate", None)
    channels = getattr(spec, "channels", None)
    return f"{channels} ch {rate} Hz" if rate and channels else None


def _application(item: Any) -> dict:
    properties = getattr(item, "proplist", None) or {}
    return {
        "application": properties.get("application.name"),
        "binary": properties.get("application.process.binary"),
        "process_id": properties.get("application.process.id"),
        "media": properties.get("media.name") or getattr(item, "name", None),
        "role": properties.get("media.role"),
    }


class PulseGraph(AudioComponent):
    name = "pulse"

    def __init__(self, on_change=None):
        super().__init__(on_change)
        self._pulse: Any = None
        self._events_task: asyncio.Task | None = None
        self.server: dict = {}
        self.sinks: list[dict] = []
        self.sources: list[dict] = []
        self.sink_inputs: list[dict] = []
        self.source_outputs: list[dict] = []

    async def _open(self) -> str | None:
        try:
            import pulsectl
            import pulsectl_asyncio
        except ImportError:
            return "the pulsectl-asyncio Python package is not installed"
        except OSError:
            return "the PulseAudio client library (libpulse) is not installed"
        pulse = pulsectl_asyncio.PulseAsync(CLIENT_NAME)
        try:
            await pulse.connect()
        except pulsectl.PulseError as exception:
            pulse.close()
            return f"no PulseAudio server is running ({exception})"
        self._pulse = pulse
        self._events_task = asyncio.create_task(self._events(pulse), name="host-audio-pulse-events")
        return None

    async def _events(self, pulse: Any) -> None:
        import pulsectl

        try:
            async for _ in pulse.subscribe_events("all"):
                self.schedule_refresh()
        except pulsectl.PulseDisconnected:
            logger.info("PulseAudio server disconnected")
        if self._pulse is pulse:
            self._pulse = None
            pulse.close()
            self.lost("the PulseAudio server stopped")

    async def _close(self) -> None:
        task = self._events_task
        self._events_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        pulse = self._pulse
        self._pulse = None
        if pulse is not None:
            pulse.close()
        self._clear()

    def _clear(self) -> None:
        self.server = {}
        self.sinks = []
        self.sources = []
        self.sink_inputs = []
        self.source_outputs = []

    async def _read(self) -> list[dict]:
        import pulsectl

        pulse = self._pulse
        if pulse is None:
            return []
        try:
            server = await pulse.server_info()
            sinks = await pulse.sink_list()
            sources = await pulse.source_list()
            sink_inputs = await pulse.sink_input_list()
            source_outputs = await pulse.source_output_list()
        except (pulsectl.PulseDisconnected, pulsectl.PulseOperationFailed) as exception:
            self.lost(f"lost the PulseAudio server ({exception})")
            return []
        sink_names = {sink.index: sink.name for sink in sinks}
        source_names = {source.index: source.name for source in sources}
        snapshot_server = {
            "name": server.server_name,
            "version": server.server_version,
            "host": server.host_name,
            "default_sink": server.default_sink_name,
            "default_source": server.default_source_name,
        }
        snapshot_sinks = [
            {
                "index": sink.index,
                "name": sink.name,
                "description": sink.description,
                "driver": sink.driver,
                "state": _state(sink.state),
                "mute": bool(sink.mute),
                **_volume(sink),
                "channels": list(sink.channel_list),
                "format": _sample_spec(sink),
                "monitor_source": sink.monitor_source_name,
                "jack_client": (sink.proplist or {}).get("jack.client_name"),
                "device": (sink.proplist or {}).get("device.description")
                or (sink.proplist or {}).get("alsa.card_name"),
            }
            for sink in sinks
        ]
        snapshot_sources = [
            {
                "index": source.index,
                "name": source.name,
                "description": source.description,
                "driver": source.driver,
                "state": _state(source.state),
                "mute": bool(source.mute),
                **_volume(source),
                "channels": list(source.channel_list),
                "format": _sample_spec(source),
                "monitor_of": sink_names.get(source.monitor_of_sink)
                if source.monitor_of_sink not in (None, NO_INDEX)
                else None,
                "jack_client": (source.proplist or {}).get("jack.client_name"),
                "device": (source.proplist or {}).get("device.description")
                or (source.proplist or {}).get("alsa.card_name"),
            }
            for source in sources
        ]
        snapshot_sink_inputs = [
            {
                "index": stream.index,
                **_application(stream),
                "sink": sink_names.get(stream.sink),
                "corked": bool(stream.corked),
                "mute": bool(stream.mute),
                **_volume(stream),
            }
            for stream in sink_inputs
        ]
        snapshot_source_outputs = [
            {
                "index": stream.index,
                **_application(stream),
                "source": source_names.get(stream.source),
                "corked": bool(stream.corked),
                "mute": bool(stream.mute),
                **_volume(stream),
            }
            for stream in source_outputs
        ]
        changes = self._differences(
            snapshot_server, snapshot_sinks, snapshot_sources, snapshot_sink_inputs, snapshot_source_outputs
        )
        self.server = snapshot_server
        self.sinks = snapshot_sinks
        self.sources = snapshot_sources
        self.sink_inputs = snapshot_sink_inputs
        self.source_outputs = snapshot_source_outputs
        return changes

    def _differences(self, server, sinks, sources, sink_inputs, source_outputs) -> list[dict]:
        if not self.server:
            return []
        now = utc_now()
        changes: list[dict] = []
        for key, label in (("default_sink", "default_output"), ("default_source", "default_input")):
            if server.get(key) != self.server.get(key):
                changes.append({"time": now, "kind": label, "from": self.server.get(key), "to": server.get(key)})
        for kind, before, after in (
            ("output", self.sinks, sinks),
            ("input", self.sources, sources),
            ("playback_stream", self.sink_inputs, sink_inputs),
            ("recording_stream", self.source_outputs, source_outputs),
        ):
            previous = {entry["index"]: entry for entry in before}
            current = {entry["index"]: entry for entry in after}
            for index in sorted(current.keys() - previous.keys()):
                changes.append({"time": now, "kind": f"{kind}_added", **_identity(current[index])})
            for index in sorted(previous.keys() - current.keys()):
                changes.append({"time": now, "kind": f"{kind}_removed", **_identity(previous[index])})
            for index in sorted(current.keys() & previous.keys()):
                old, new = previous[index], current[index]
                for field in ("volume_percent", "mute", "sink", "source"):
                    if old.get(field) != new.get(field):
                        changes.append(
                            {
                                "time": now,
                                "kind": f"{kind}_{field}",
                                **_identity(new),
                                "from": old.get(field),
                                "to": new.get(field),
                            }
                        )
        return changes

    def _object_list(self, kind: str) -> list[dict]:
        return {
            "sink": self.sinks,
            "source": self.sources,
            "sink_input": self.sink_inputs,
            "source_output": self.source_outputs,
        }[kind]

    def find(self, kind: str, index: int) -> dict | None:
        return next((entry for entry in self._object_list(kind) if entry["index"] == index), None)

    async def _info(self, kind: str, index: int) -> Any:
        pulse = self._pulse
        if pulse is None:
            raise ConnectionError("not connected to PulseAudio")
        reader = {
            "sink": pulse.sink_info,
            "source": pulse.source_info,
            "sink_input": pulse.sink_input_info,
            "source_output": pulse.source_output_info,
        }[kind]
        return await reader(index)

    async def set_volume(self, kind: str, index: int, value: float) -> None:
        item = await self._info(kind, index)
        await self._pulse.volume_set_all_chans(item, value)

    async def set_mute(self, kind: str, index: int, mute: bool) -> None:
        item = await self._info(kind, index)
        await self._pulse.mute(item, mute)

    async def set_default(self, kind: str, index: int) -> None:
        item = await self._info(kind, index)
        await self._pulse.default_set(item)

    async def move(self, kind: str, index: int, destination: int) -> None:
        pulse = self._pulse
        if pulse is None:
            raise ConnectionError("not connected to PulseAudio")
        if kind == "sink_input":
            await pulse.sink_input_move(index, destination)
        else:
            await pulse.source_output_move(index, destination)

    async def peak(self, source: str, seconds: float, stream_index: int | None = None) -> float | None:
        pulse = self._pulse
        if pulse is None:
            return None
        return await pulse.get_peak_sample(source, seconds, stream_index)

    def to_dict(self) -> dict:
        return {
            "available": self.available,
            "reason": self.reason,
            "seen_running": self.seen_running,
            "connected_at": self.connected_at,
            "server": self.server,
            "sinks": self.sinks,
            "sources": self.sources,
            "sink_inputs": self.sink_inputs,
            "source_outputs": self.source_outputs,
            "changes": list(self.changes),
        }


def _identity(entry: dict) -> dict:
    return {key: entry.get(key) for key in ("index", "name", "application", "media") if entry.get(key) is not None}
