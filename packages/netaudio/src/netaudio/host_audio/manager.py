from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Callable
from pathlib import Path

from netaudio.host_audio import alsa
from netaudio.host_audio.component import AudioComponent
from netaudio.host_audio.jack_graph import JackGraph, server_name
from netaudio.host_audio.jack_meter import JackMeter
from netaudio.host_audio.links import bridge_clients, card_links
from netaudio.host_audio.peers import HostAudioPeers
from netaudio.host_audio.pulse import PulseGraph
from netaudio.host_audio.watch import DirectoryWatch

logger = logging.getLogger("netaudio")

SHARED_MEMORY = Path("/dev/shm")
CONNECT_ATTEMPTS = 5
CONNECT_RETRY_SECONDS = 0.5


class HostAudioManager:
    def __init__(self) -> None:
        self.jack = JackGraph(self._changed)
        self.pulse = PulseGraph(self._changed)
        self.meter = JackMeter()
        self.peers = HostAudioPeers()
        self.host = socket.gethostname().removesuffix(".local")
        self._watch = DirectoryWatch(self._created)
        self._listeners: list[Callable[[str], None]] = []
        self._tasks: set[asyncio.Task] = set()
        self._runtime_directory = Path(os.environ["XDG_RUNTIME_DIR"]) if os.environ.get("XDG_RUNTIME_DIR") else None

    def add_listener(self, listener: Callable[[str], None]) -> None:
        self._listeners.append(listener)

    def _changed(self, component: str) -> None:
        for listener in self._listeners:
            listener(component)

    async def start(self) -> None:
        if self._watch.start():
            self._watch.watch(SHARED_MEMORY)
            if self._runtime_directory is not None:
                self._watch.watch(self._runtime_directory)
                self._watch.watch(self._runtime_directory / "pulse")
        await asyncio.gather(self.jack.start(), self.pulse.start())
        logger.info(
            "Host audio: JACK %s, PulseAudio %s",
            "connected" if self.jack.available else self.jack.reason,
            "connected" if self.pulse.available else self.pulse.reason,
        )

    def start_peers(self, zeroconf) -> None:
        self.peers.start(zeroconf)

    async def stop(self) -> None:
        await self.peers.stop()
        self._watch.stop()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(self.jack.stop(), self.pulse.stop())
        self.meter.stop()
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
        if component.available:
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
