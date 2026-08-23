from __future__ import annotations

import asyncio
import collections
import ipaddress
import logging
import socket
import struct
import time

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.const import (
    MULTICAST_GROUP_CONTROL_MONITORING,
)
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.metering import classify_signal_presence, parse_metering_levels

logger = logging.getLogger("netaudio")

CACHE_MAX_AGE = 2.0
HISTORY_MAX_SAMPLES = 3600
BROADCAST_INTERVAL = 0.05


class MeteringManager:
    def __init__(self, application):
        self._application = application
        self._persistent_refs: dict[str, set[str]] = {}
        self._snapshot_count: dict[str, int] = {}
        self._detailed_levels: dict[str, dict] = {}
        self._signal_presence_levels: dict[str, dict] = {}
        self._latest_levels: dict[str, dict] = {}
        self._history: dict[str, collections.deque] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._transport = None
        self._host_ip = None
        self._host_mac = None
        self._keepalive_task = None
        self._broadcast_task = None
        self._active_port: int | None = None
        self._dirty_devices: set[str] = set()

    @staticmethod
    def _probe_port(port: int) -> bool:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            probe.bind(("", port))
            return True
        except OSError:
            return False
        finally:
            probe.close()

    def _is_active(self, server_name: str) -> bool:
        return bool(self._persistent_refs.get(server_name) or self._snapshot_count.get(server_name, 0) > 0)

    def _get_device(self, server_name: str):
        return self._application.devices.get(server_name)

    @staticmethod
    def _cached_result(cached: dict) -> dict:
        result = {
            "tx": dict(cached["tx"]),
            "rx": dict(cached["rx"]),
            "wall_time": cached.get("wall_time"),
            "source_ip": cached.get("source_ip"),
            "source_port": cached.get("source_port"),
            "metering_source": cached.get("metering_source"),
        }
        for key in (
            "sequence",
            "tx_count",
            "rx_count",
            "tx_first_channel_index",
            "rx_first_channel_index",
            "level_vector_offset",
            "padding_length",
            "tx_raw",
            "rx_raw",
            "tx_signal_presence",
            "rx_signal_presence",
        ):
            if key in cached:
                value = cached[key]
                if isinstance(value, dict):
                    result[key] = dict(value)
                elif isinstance(value, list):
                    result[key] = list(value)
                else:
                    result[key] = value
        return result

    @staticmethod
    def _is_fresh(sample: dict | None, now: float) -> bool:
        return bool(sample and now - sample.get("timestamp", 0) < CACHE_MAX_AGE)

    def _selected_sample(self, server_name: str, now: float | None = None) -> dict | None:
        now = time.monotonic() if now is None else now
        detailed = self._detailed_levels.get(server_name)
        if self._is_fresh(detailed, now):
            return detailed
        passive = self._signal_presence_levels.get(server_name)
        if self._is_fresh(passive, now):
            return passive
        return None

    def _append_history(self, server_name: str, sample: dict) -> None:
        if server_name not in self._history:
            self._history[server_name] = collections.deque(maxlen=HISTORY_MAX_SAMPLES)
        self._history[server_name].append(sample)

    def _server_name_for_ip(self, ip: str) -> str | None:
        for device in self._application.devices.values():
            if device.ipv4 and str(device.ipv4) == ip:
                return device.server_name
        return None

    def _send_start(self, server_name: str):
        device = self._get_device(server_name)
        if not device or not device.online:
            return
        device_ip = str(device.ipv4)
        device_name = device.name or device.server_name
        logger.debug(f"Sending metering start to {device_name} ({device_ip})")
        self._application.cmc.start_metering(
            device_ip,
            device_name,
            self._host_ip,
            self._host_mac,
            self._active_port,
        )

    def _send_stop(self, server_name: str):
        device = self._get_device(server_name)
        if not device or not device.online:
            return
        device_ip = str(device.ipv4)
        device_name = device.name or device.server_name
        logger.debug(f"Sending metering stop to {device_name} ({device_ip})")
        self._application.cmc.stop_metering(
            device_ip,
            device_name,
            self._host_ip,
            self._host_mac,
            self._active_port,
        )

    async def start(self):
        self._host_ip = _get_local_ip()
        self._host_mac = self._application.cmc.host_media_access_control_address

        preferred_port = app_settings.metering_port
        if self._probe_port(preferred_port):
            self._active_port = preferred_port
        else:
            fallback_port = preferred_port + 1
            logger.warning(
                f"Metering port {preferred_port} is in use (Dante Controller?), falling back to {fallback_port}"
            )
            self._active_port = fallback_port

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.bind(("", self._active_port))

        mreq = struct.pack(
            "4s4s",
            socket.inet_aton(MULTICAST_GROUP_CONTROL_MONITORING),
            socket.inet_aton("0.0.0.0"),
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _MeteringProtocol(self._on_metering_packet),
            sock=sock,
        )
        logger.info("MeteringManager: UDP listener started on port %d", self._active_port)

        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        self._broadcast_task = asyncio.create_task(self._broadcast_loop())

    async def _keepalive_loop(self):
        while True:
            await asyncio.sleep(5)
            for server_name in list(self._persistent_refs.keys()):
                if self._persistent_refs.get(server_name):
                    self._send_start(server_name)

    async def _broadcast_loop(self):
        while True:
            await asyncio.sleep(BROADCAST_INTERVAL)
            self._broadcast_pending()

    def _broadcast_pending(self) -> None:
        if not self._dirty_devices:
            return
        devices_to_broadcast = list(self._dirty_devices)
        self._dirty_devices.clear()
        for server_name in devices_to_broadcast:
            cached = self._selected_sample(server_name)
            if not cached:
                continue
            self._latest_levels[server_name] = cached
            self._application.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.METER_VALUES,
                    server_name=server_name,
                    data=self._cached_result(cached),
                )
            )

    async def stop(self):
        if self._broadcast_task:
            self._broadcast_task.cancel()
            self._broadcast_task = None

        if self._keepalive_task:
            self._keepalive_task.cancel()
            self._keepalive_task = None

        all_names = set(self._persistent_refs.keys()) | set(
            name for name, count in self._snapshot_count.items() if count > 0
        )
        for server_name in all_names:
            self._send_stop(server_name)

        self._persistent_refs.clear()
        self._snapshot_count.clear()
        self._detailed_levels.clear()
        self._signal_presence_levels.clear()
        self._latest_levels.clear()
        self._history.clear()
        self._events.clear()

        if self._transport:
            self._transport.close()
            self._transport = None

        logger.info("MeteringManager: stopped")

    def cleanup_device(self, server_name: str):
        self._snapshot_count.pop(server_name, None)
        self._detailed_levels.pop(server_name, None)
        self._signal_presence_levels.pop(server_name, None)
        self._latest_levels.pop(server_name, None)
        self._events.pop(server_name, None)

    def reactivate_device(self, server_name: str):
        if self._persistent_refs.get(server_name):
            logger.info(f"Reactivating metering for {server_name}")
            self._send_start(server_name)

    def get_status(self) -> dict:
        now = time.monotonic()
        result = {}
        for server_name, refs in self._persistent_refs.items():
            device = self._get_device(server_name)
            cached = self._selected_sample(server_name, now)
            result[server_name] = {
                "name": device.name if device else "",
                "server_name": server_name,
                "online": device.online if device else False,
                "receiving": cached is not None,
                "metering_source": cached.get("metering_source") if cached else None,
            }
        return result

    def get_cached_levels(self, server_name: str) -> dict | None:
        cached = self._selected_sample(server_name)
        if not cached:
            return None
        self._latest_levels[server_name] = cached
        return self._cached_result(cached)

    def get_cached_levels_by_server(self) -> dict[str, dict]:
        now = time.monotonic()
        server_names = self._detailed_levels.keys() | self._signal_presence_levels.keys()
        return {
            server_name: self._cached_result(cached)
            for server_name in server_names
            if (cached := self._selected_sample(server_name, now)) is not None
        }

    def get_history(self, server_name: str, max_samples: int | None = None) -> list[dict]:
        history = self._history.get(server_name)
        if not history:
            return []
        if max_samples is not None:
            return list(history)[-max_samples:]
        return list(history)

    def add_persistent(self, server_name: str, client_id: str):
        was_active = self._is_active(server_name)
        refs = self._persistent_refs.setdefault(server_name, set())
        refs.add(client_id)
        if not was_active:
            self._send_start(server_name)

    def remove_persistent(self, server_name: str, client_id: str):
        refs = self._persistent_refs.get(server_name)
        if refs:
            refs.discard(client_id)
            if not refs:
                del self._persistent_refs[server_name]
        if not self._is_active(server_name):
            self._send_stop(server_name)

    async def snapshot(self, server_name: str, timeout: float = 3.0) -> dict | None:
        device = self._get_device(server_name)
        if device and not device.online:
            return None

        cached = self._selected_sample(server_name)
        if cached:
            self._latest_levels[server_name] = cached
            return self._cached_result(cached)

        if self._persistent_refs.get(server_name):
            return None

        was_active = self._is_active(server_name)
        self._snapshot_count[server_name] = self._snapshot_count.get(server_name, 0) + 1

        if not was_active:
            self._send_start(server_name)

        event = self._events.get(server_name)
        if event is None:
            event = asyncio.Event()
            self._events[server_name] = event
        else:
            event.clear()

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            cached = self._selected_sample(server_name)
            if cached:
                self._latest_levels[server_name] = cached
                return self._cached_result(cached)
            return None
        except asyncio.TimeoutError:
            return None
        finally:
            count = self._snapshot_count.get(server_name, 1) - 1
            if count <= 0:
                self._snapshot_count.pop(server_name, None)
            else:
                self._snapshot_count[server_name] = count

            if not self._is_active(server_name):
                self._send_stop(server_name)

    def _on_metering_packet(self, data: bytes, source_address: tuple):
        source_ip = source_address[0]
        server_name = self._server_name_for_ip(source_ip)
        if not server_name:
            return

        device = self._get_device(server_name)
        if not device:
            return

        device.update_last_seen()

        from netaudio import core

        try:
            levels = parse_metering_levels(data)
        except core.NetaudioCoreError as error:
            logger.warning(f"Ignoring malformed metering packet from {source_ip}: {error}")
            return
        now = time.monotonic()
        sample = {
            "tx": levels["tx"],
            "rx": levels["rx"],
            "timestamp": now,
            "wall_time": time.time(),
            "source_ip": source_ip,
            "source_port": source_address[1],
            "metering_source": "detailed",
        }
        self._detailed_levels[server_name] = sample
        self._latest_levels[server_name] = sample
        self._append_history(server_name, sample)

        if self._persistent_refs.get(server_name):
            self._dirty_devices.add(server_name)

        event = self._events.get(server_name)
        if event:
            event.set()

    def record_signal_presence(self, record: dict, source_address: tuple) -> None:
        try:
            source_ip = source_address[0]
            source_port = source_address[1]
            server_name = self._server_name_for_ip(source_ip)
            if not server_name:
                return

            tx_levels = list(record["tx_levels"])
            rx_levels = list(record["rx_levels"])
            tx_count = int(record["tx_count"])
            rx_count = int(record["rx_count"])
            tx_first_channel_index = int(record["tx_first_channel_index"])
            rx_first_channel_index = int(record["rx_first_channel_index"])
            if len(tx_levels) != tx_count or len(rx_levels) != rx_count:
                return
            if any(not isinstance(value, int) or not 0 <= value <= 0xFF for value in tx_levels + rx_levels):
                return

            tx = {tx_first_channel_index + offset + 1: value for offset, value in enumerate(tx_levels)}
            rx = {rx_first_channel_index + offset + 1: value for offset, value in enumerate(rx_levels)}
            tx_indications = {channel: classify_signal_presence(value) for channel, value in tx.items()}
            rx_indications = {channel: classify_signal_presence(value) for channel, value in rx.items()}
            now = time.monotonic()
            sample = {
                "tx": tx,
                "rx": rx,
                "tx_raw": tx_levels,
                "rx_raw": rx_levels,
                "tx_signal_presence": tx_indications,
                "rx_signal_presence": rx_indications,
                "timestamp": now,
                "wall_time": time.time(),
                "source_ip": source_ip,
                "source_port": source_port,
                "metering_source": "signal_presence",
                "sequence": int(record["sequence"]),
                "tx_count": tx_count,
                "rx_count": rx_count,
                "tx_first_channel_index": tx_first_channel_index,
                "rx_first_channel_index": rx_first_channel_index,
                "level_vector_offset": int(record["level_vector_offset"]),
                "padding_length": int(record["padding_length"]),
            }
        except (KeyError, TypeError, ValueError, IndexError) as exception:
            logger.debug(f"Discarding malformed signal-presence record from {source_address}: {exception}")
            return

        self._signal_presence_levels[server_name] = sample
        selected = self._selected_sample(server_name, now)
        if selected is not sample:
            return

        self._latest_levels[server_name] = sample
        self._append_history(server_name, sample)

        self._dirty_devices.add(server_name)

        event = self._events.get(server_name)
        if event:
            event.set()


class _MeteringProtocol(asyncio.DatagramProtocol):
    def __init__(self, callback):
        self._callback = callback

    def datagram_received(self, data, addr):
        source_address = addr
        self._callback(data, source_address)


def _get_local_ip() -> ipaddress.IPv4Address:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("224.0.0.231", 1))
        local_ip = sock.getsockname()[0]
    finally:
        sock.close()
    return ipaddress.IPv4Address(local_ip)
