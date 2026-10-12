from __future__ import annotations

import asyncio
import logging
import math
import os
import socket
import time
from collections.abc import Callable
from pathlib import Path

from netaudio.host_audio import alsa
from netaudio.host_audio.component import AudioComponent
from netaudio.host_audio.jack_graph import JackGraph, server_name
from netaudio.host_audio.jack_meter import JackMeter
from netaudio.host_audio.jack_recorder import JackRecorder
from netaudio.host_audio.links import bridge_clients, card_links
from netaudio.host_audio.peers import HostAudioPeers
from netaudio.host_audio.pulse import PulseGraph
from netaudio.host_audio.watch import DirectoryWatch
from netaudio.monitoring.level_history import LevelHistory, jack_values

logger = logging.getLogger("netaudio")

SHARED_MEMORY = Path("/dev/shm")
CONNECT_ATTEMPTS = 5
CONNECT_RETRY_SECONDS = 0.5
CLOSE_SECONDS = 5.0
RECENT_LEVEL_SECONDS = 2.5
RECENT_LEVEL_WINDOW_SECONDS = 1.0


class HostAudioManager:
    def __init__(self, level_history: LevelHistory | None = None, record_jack_levels: bool = False) -> None:
        self.jack = JackGraph(self._changed)
        self.pulse = PulseGraph(self._changed)
        self.meter = JackMeter()
        self.level_history = level_history or LevelHistory()
        self.record_jack_levels = record_jack_levels
        self.recorder = JackRecorder(self._record_levels, lambda: self.jack.available)
        self._recording = False
        self._latest_levels: tuple[int, dict] | None = None
        self.peers = HostAudioPeers()
        self.host = socket.gethostname().removesuffix(".local")
        self._watch = DirectoryWatch(self._created)
        self._listeners: list[Callable[[str], None]] = []
        self._tasks: set[asyncio.Task] = set()
        self._runtime_directory = Path(os.environ["XDG_RUNTIME_DIR"]) if os.environ.get("XDG_RUNTIME_DIR") else None

    def add_listener(self, listener: Callable[[str], None]) -> None:
        self._listeners.append(listener)

    def _record_levels(self, second: int, levels: dict) -> None:
        self._latest_levels = (second, levels)
        self.level_history.record("jack", second, jack_values(levels), levels.keys())

    async def measure(self, targets: dict[str, list[str]], seconds: float) -> dict[str, dict]:
        found: dict[str, dict] = {target: {"connected": False} for target, sources in targets.items() if not sources}
        if seconds <= RECENT_LEVEL_WINDOW_SECONDS:
            found.update(self._recorded_levels(targets))
        missing = {target: sources for target, sources in targets.items() if target not in found}
        if missing:
            found.update(await self.meter.measure(missing, seconds))
        return found

    def _recorded_levels(self, targets: dict[str, list[str]]) -> dict[str, dict]:
        if not self.recorder.recording or self._latest_levels is None:
            return {}
        second, levels = self._latest_levels
        if time.time() - second > RECENT_LEVEL_SECONDS:
            return {}
        found = {}
        for target, sources in targets.items():
            readings = [levels.get(source) for source in sources]
            if not sources or any(reading is None for reading in readings):
                continue
            peaks = [reading[0] for reading in readings if reading[0] is not None]
            powers = [10 ** (reading[1] / 10) for reading in readings if reading[1] is not None]
            found[target] = {
                "peak_dbfs": max(peaks) if peaks else None,
                "rms_dbfs": round(10 * math.log10(sum(powers)), 1) if powers else None,
                "recorded_second": second,
            }
        return found

    def _changed(self, component: str) -> None:
        if component == "jack" and self._recording and self.jack.available:
            self.recorder.start(self.jack.connected_at)
        for listener in self._listeners:
            listener(component)

    async def start(self) -> None:
        await asyncio.gather(self.jack.start(), self.pulse.start())
        if (self.jack.installed or self.pulse.installed) and self._watch.start():
            if self.jack.installed:
                self._watch.watch(SHARED_MEMORY)
            if self._runtime_directory is not None:
                self._watch.watch(self._runtime_directory)
                if self.pulse.installed:
                    self._watch.watch(self._runtime_directory / "pulse")
        if self.record_jack_levels:
            self._recording = True
            self.recorder.start(self.jack.connected_at)
        logger.info(
            "Host audio: JACK %s, PulseAudio %s, JACK level recording %s",
            "connected" if self.jack.available else self.jack.reason,
            "connected" if self.pulse.available else self.pulse.reason,
            "on" if self.record_jack_levels else "off",
        )

    def jack_history_note(self) -> str | None:
        if self.record_jack_levels:
            return None
        return (
            f"JACK levels are not recorded on {self.host}. "
            "Set record_jack_levels = true under [daemon] in its netaudio config to keep a level history"
        )

    def start_peers(self, zeroconf) -> None:
        self.peers.start(zeroconf)

    async def stop(self) -> None:
        self._recording = False
        await self.recorder.stop()
        await self.meter.stop()
        await self.peers.stop()
        self._watch.stop()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await asyncio.wait_for(asyncio.gather(self.jack.stop(), self.pulse.stop()), CLOSE_SECONDS)
        except asyncio.TimeoutError:
            logger.warning("Host audio: JACK and PulseAudio did not close within %g s", CLOSE_SECONDS)
        await self.jack.shutdown_executor()

    def _created(self, path: Path) -> None:
        name = path.name
        jack_socket = f"jack_{server_name() or 'default'}_"
        if (path.parent == SHARED_MEMORY and name.startswith(jack_socket)) or name.startswith("pipewire-"):
            self._connect_soon(self.jack)
        if self._runtime_directory is not None and path == self._runtime_directory / "pulse":
            self._watch.watch(path)
            self._connect_soon(self.pulse)
        elif name == "native" and path.parent.name == "pulse":
            self._connect_soon(self.pulse)

    def _connect_soon(self, component: AudioComponent) -> None:
        if component.available or not component.installed:
            return
        task = asyncio.create_task(self._connect_with_retries(component), name=f"host-audio-{component.name}-connect")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @staticmethod
    async def _connect_with_retries(component: AudioComponent) -> None:
        for _ in range(CONNECT_ATTEMPTS):
            if await component.connect():
                logger.info("Host audio: connected to %s", component.name)
                return
            await asyncio.sleep(CONNECT_RETRY_SECONDS)

    async def ensure(self) -> None:
        await asyncio.gather(self.jack.ensure(), self.pulse.ensure())

    def snapshot(self) -> dict:
        cards = alsa.cards()
        return {
            "host": self.host,
            "jack": self.jack.to_dict(),
            "pulse": self.pulse.to_dict(),
            "cards": cards,
            "card_links": card_links(),
            "bridges": bridge_clients(cards),
        }
