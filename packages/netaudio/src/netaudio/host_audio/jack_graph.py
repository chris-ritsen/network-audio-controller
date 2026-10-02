from __future__ import annotations

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from netaudio.host_audio.component import AudioComponent, utc_now

logger = logging.getLogger("netaudio")

CLIENT_NAME = "netaudio"
METER_CLIENT_PREFIX = "netaudio-meter"
PRETTY_NAME_PROPERTY = "http://jackaudio.org/metadata/pretty-name"
XRUN_GROUP_SECONDS = 0.1


def server_name() -> str | None:
    return os.environ.get("JACK_DEFAULT_SERVER") or None


def _is_meter_port(name: str) -> bool:
    return name.startswith(METER_CLIENT_PREFIX)


def _port_type(port: Any) -> str:
    if port.is_audio:
        return "audio"
    if port.is_midi:
        return "midi"
    return "other"


class JackGraph(AudioComponent):
    name = "jack"

    def __init__(self, on_change=None):
        super().__init__(on_change)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="netaudio-jack")
        self._client: Any = None
        self.client_name: str | None = None
        self.ports: dict[str, dict] = {}
        self.sample_rate: int | None = None
        self.buffer_size: int | None = None
        self.realtime: bool | None = None
        self.cpu_load: float | None = None
        self.xruns = 0
        self.last_xrun: str | None = None
        self.xruns_since: str | None = None
        self._last_xrun_monotonic = 0.0

    async def _run(self, function, *arguments):
        return await asyncio.get_running_loop().run_in_executor(self._executor, function, *arguments)

    async def _open(self) -> str | None:
        try:
            import jack
        except ImportError:
            return "the JACK-Client Python package is not installed"
        except OSError:
            return "the JACK library (libjack) is not installed"
        try:
            self._client = await self._run(self._open_client, jack)
        except jack.JackOpenError:
            return "no JACK server is running"
        except jack.JackError as exception:
            return f"cannot connect to JACK: {exception}"
        self.xruns = 0
        self.last_xrun = None
        self.xruns_since = utc_now()
        return None

    def _open_client(self, jack: Any) -> Any:
        client = jack.Client(CLIENT_NAME, no_start_server=True, servername=server_name())
        client.set_port_registration_callback(self._on_port_registration, only_available=False)
        client.set_port_connect_callback(self._on_port_connect, only_available=False)
        client.set_port_rename_callback(self._on_port_rename, only_available=False)
        client.set_client_registration_callback(self._on_client_registration)
        client.set_xrun_callback(self._on_xrun)
        client.set_blocksize_callback(self._on_blocksize)
        client.set_samplerate_callback(self._on_samplerate)
        client.set_property_change_callback(self._on_property_change)
        client.set_shutdown_callback(self._on_shutdown)
        client.activate()
        self.client_name = client.name
        return client

    def _on_port_registration(self, port, register) -> None:
        self.schedule_refresh_threadsafe()

    def _on_port_connect(self, first, second, connect) -> None:
        self.schedule_refresh_threadsafe()

    def _on_port_rename(self, port, old, new) -> None:
        self.schedule_refresh_threadsafe()

    def _on_client_registration(self, name, register) -> None:
        self.schedule_refresh_threadsafe()

    def _on_property_change(self, subject, key, change) -> None:
        self.schedule_refresh_threadsafe()

    def _on_blocksize(self, blocksize) -> None:
        self.schedule_refresh_threadsafe()

    def _on_samplerate(self, samplerate) -> None:
        self.schedule_refresh_threadsafe()

    def _on_xrun(self, delayed_microseconds) -> None:
        loop = self._loop
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._record_xrun, delayed_microseconds)

    def _record_xrun(self, delayed_microseconds) -> None:
        now = time.monotonic()
        if now - self._last_xrun_monotonic < XRUN_GROUP_SECONDS:
            return
        self._last_xrun_monotonic = now
        self.xruns += 1
        self.last_xrun = utc_now()
        self.record("xrun", delayed_microseconds=round(float(delayed_microseconds), 1))
        if self.on_change is not None:
            self.on_change(self.name)

    def _on_shutdown(self, status, reason) -> None:
        loop = self._loop
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._server_stopped, str(reason) or "the JACK server stopped")

    def _server_stopped(self, reason: str) -> None:
        client = self._client
        self._client = None
        self.lost(f"the JACK server stopped ({reason})")
        if client is not None and self._loop is not None:
            self._loop.run_in_executor(self._executor, self._close_client, client)

    @staticmethod
    def _close_client(client: Any) -> None:
        import jack

        try:
            client.close()
        except jack.JackError as exception:
            logger.debug("Closing the JACK client after shutdown: %s", exception)

    async def _close(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await self._run(self._close_client, client)
        self._clear()

    def _clear(self) -> None:
        self.ports = {}
        self.sample_rate = None
        self.buffer_size = None
        self.cpu_load = None

    def _snapshot(self) -> dict:
        import jack

        client = self._client
        if client is None:
            raise jack.JackError("not connected")
        pretty_names = {}
        for subject, properties in jack.get_all_properties().items():
            value = properties.get(PRETTY_NAME_PROPERTY)
            if value is not None:
                pretty_names[subject] = value[0].decode("utf-8", errors="replace")
        ports: dict[str, dict] = {}
        for port in client.get_ports():
            name = port.name
            if _is_meter_port(name):
                continue
            entry: dict[str, Any] = {
                "client": name.split(":", 1)[0],
                "short_name": port.shortname,
                "direction": "output" if port.is_output else "input",
                "type": _port_type(port),
                "physical": port.is_physical,
                "terminal": port.is_terminal,
                "aliases": list(port.aliases),
                "connections": [],
            }
            pretty_name = pretty_names.get(port.uuid)
            if pretty_name:
                entry["pretty_name"] = pretty_name
            if port.is_output:
                entry["connections"] = sorted(
                    connected.name
                    for connected in client.get_all_connections(port)
                    if not _is_meter_port(connected.name)
                )
            ports[name] = entry
        for name, entry in ports.items():
            for destination in entry["connections"] if entry["direction"] == "output" else ():
                if destination in ports:
                    ports[destination]["connections"].append(name)
        for entry in ports.values():
            if entry["direction"] == "input":
                entry["connections"].sort()
        return {
            "ports": ports,
            "sample_rate": client.samplerate,
            "buffer_size": client.blocksize,
            "realtime": client.realtime,
            "cpu_load": round(client.cpu_load(), 1),
        }

    async def _read(self) -> list[dict]:
        import jack

        try:
            snapshot = await self._run(self._snapshot)
        except jack.JackError as exception:
            self.lost(f"lost the JACK server ({exception})")
            return []
        changes = self._differences(snapshot)
        self.ports = snapshot["ports"]
        self.sample_rate = snapshot["sample_rate"]
        self.buffer_size = snapshot["buffer_size"]
        self.realtime = snapshot["realtime"]
        self.cpu_load = snapshot["cpu_load"]
        return changes

    def _differences(self, snapshot: dict) -> list[dict]:
        changes: list[dict] = []
        now = utc_now()
        if self.sample_rate is not None and snapshot["sample_rate"] != self.sample_rate:
            changes.append(
                {"time": now, "kind": "sample_rate", "from": self.sample_rate, "to": snapshot["sample_rate"]}
            )
        if self.buffer_size is not None and snapshot["buffer_size"] != self.buffer_size:
            changes.append(
                {"time": now, "kind": "buffer_size", "from": self.buffer_size, "to": snapshot["buffer_size"]}
            )
        if not self.ports:
            return changes
        before_clients = {entry["client"] for entry in self.ports.values()}
        after_clients = {entry["client"] for entry in snapshot["ports"].values()}
        for client in sorted(after_clients - before_clients):
            count = sum(1 for entry in snapshot["ports"].values() if entry["client"] == client)
            changes.append({"time": now, "kind": "client_added", "client": client, "ports": count})
        for client in sorted(before_clients - after_clients):
            changes.append({"time": now, "kind": "client_removed", "client": client})
        before = _connection_pairs(self.ports)
        after = _connection_pairs(snapshot["ports"])
        if after - before:
            changes.append({"time": now, "kind": "connected", "pairs": sorted(list(pair) for pair in after - before)})
        if before - after:
            changes.append(
                {"time": now, "kind": "disconnected", "pairs": sorted(list(pair) for pair in before - after)}
            )
        return changes

    async def connect_ports(self, source: str, destination: str) -> None:
        await self._run(self._connect_ports, source, destination)

    def _connect_ports(self, source: str, destination: str) -> None:
        import jack

        client = self._client
        if client is None:
            raise jack.JackError("not connected to JACK")
        client.connect(source, destination)

    async def disconnect_ports(self, source: str, destination: str) -> None:
        await self._run(self._disconnect_ports, source, destination)

    def _disconnect_ports(self, source: str, destination: str) -> None:
        import jack

        client = self._client
        if client is None:
            raise jack.JackError("not connected to JACK")
        client.disconnect(source, destination)

    def connected(self, source: str, destination: str) -> bool:
        return destination in (self.ports.get(source) or {}).get("connections", ())

    def to_dict(self) -> dict:
        return {
            "available": self.available,
            "reason": self.reason,
            "seen_running": self.seen_running,
            "server": server_name() or "default",
            "client_name": self.client_name,
            "connected_at": self.connected_at,
            "sample_rate": self.sample_rate,
            "buffer_size": self.buffer_size,
            "realtime": self.realtime,
            "cpu_load": self.cpu_load,
            "xruns": self.xruns,
            "last_xrun": self.last_xrun,
            "xruns_since": self.xruns_since,
            "ports": self.ports,
            "changes": list(self.changes),
        }

    async def shutdown_executor(self) -> None:
        self._executor.shutdown(wait=False)


def _connection_pairs(ports: dict[str, dict]) -> set[tuple[str, str]]:
    return {
        (name, destination)
        for name, entry in ports.items()
        if entry["direction"] == "output"
        for destination in entry["connections"]
    }
