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
from netaudio.dante.metering import parse_metering_levels

logger = logging.getLogger("netaudio")

CACHE_MAX_AGE = 2.0
HISTORY_MAX_SAMPLES = 3600
BROADCAST_INTERVAL = 0.05


class MeteringManager:
    def __init__(self, application):
        self._application = application
        self._persistent_refs: dict[str, set[str]] = {}
        self._snapshot_count: dict[str, int] = {}
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
        self._last_broadcast: dict[str, float] = {}

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
        return bool(
            self._persistent_refs.get(server_name)
            or self._snapshot_count.get(server_name, 0) > 0
        )

    def _get_device(self, server_name: str):
        return self._application.devices.get(server_name)

    @staticmethod
    def _cached_result(cached: dict) -> dict:
        return {
            "tx": cached["tx"],
            "rx": cached["rx"],
            "wall_time": cached.get("wall_time"),
            "source_ip": cached.get("source_ip"),
        }

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
            device_ip, device_name, self._host_ip, self._host_mac,
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
            device_ip, device_name, self._host_ip, self._host_mac,
            self._active_port,
        )

    async def start(self):
        self._host_ip = _get_local_ip()
        self._host_mac = self._application.cmc._host_mac

        preferred_port = app_settings.metering_port
        if self._probe_port(preferred_port):
            self._active_port = preferred_port
        else:
            fallback_port = preferred_port + 1
            logger.warning(
                f"Metering port {preferred_port} is in use (Dante Controller?), "
                f"falling back to {fallback_port}"
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
            if not self._dirty_devices:
                continue
            devices_to_broadcast = list(self._dirty_devices)
            self._dirty_devices.clear()
            for server_name in devices_to_broadcast:
                cached = self._latest_levels.get(server_name)
                if not cached:
                    continue
                self._application.dispatcher.emit_nowait(DanteEvent(
                    type=EventType.METER_VALUES,
                    server_name=server_name,
                    data={
                        "tx": cached["tx"],
                        "rx": cached["rx"],
                        "wall_time": cached.get("wall_time"),
                        "source_ip": cached.get("source_ip"),
                    },
                ))

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
        self._latest_levels.clear()
        self._history.clear()
        self._events.clear()

        if self._transport:
            self._transport.close()
            self._transport = None

        logger.info("MeteringManager: stopped")

    def cleanup_device(self, server_name: str):
        self._snapshot_count.pop(server_name, None)
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
            cached = self._latest_levels.get(server_name)
            receiving = False
            if cached:
                age = now - cached.get("timestamp", 0)
                receiving = age < 10.0
            result[server_name] = {
                "name": device.name if device else "",
                "server_name": server_name,
                "online": device.online if device else False,
                "receiving": receiving,
            }
        return result

    def get_cached_levels(self, server_name: str) -> dict | None:
        cached = self._latest_levels.get(server_name)
        if not cached:
            return None
        return {
            "tx": cached["tx"],
            "rx": cached["rx"],
            "wall_time": cached.get("wall_time"),
            "source_ip": cached.get("source_ip"),
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

        cached = self._latest_levels.get(server_name)
        if cached and self._persistent_refs.get(server_name):
            return self._cached_result(cached)
        if cached and (time.monotonic() - cached.get("timestamp", 0)) < CACHE_MAX_AGE:
            return self._cached_result(cached)

        if self._persistent_refs.get(server_name) and not cached:
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
            cached = self._latest_levels.get(server_name)
            if cached:
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

    def _on_metering_packet(self, data: bytes, addr: tuple):
        src_ip = addr[0]
        server_name = self._server_name_for_ip(src_ip)
        if not server_name:
            return

        device = self._get_device(server_name)
        if not device:
            return

        device.update_last_seen()

        tx_count = device.tx_count_raw or device.tx_count or 0
        rx_count = device.rx_count_raw or device.rx_count or 0
        if not tx_count and not rx_count:
            return

        levels = parse_metering_levels(data, tx_count, rx_count)
        now = time.monotonic()
        sample = {
            "tx": levels["tx"],
            "rx": levels["rx"],
            "timestamp": now,
            "wall_time": time.time(),
            "source_ip": src_ip,
        }
        self._latest_levels[server_name] = sample

        if server_name not in self._history:
            self._history[server_name] = collections.deque(maxlen=HISTORY_MAX_SAMPLES)
        self._history[server_name].append(sample)

        if self._persistent_refs.get(server_name):
            self._dirty_devices.add(server_name)

        event = self._events.get(server_name)
        if event:
            event.set()


class _MeteringProtocol(asyncio.DatagramProtocol):
    def __init__(self, callback):
        self._callback = callback

    def datagram_received(self, data, addr):
        self._callback(data, addr)


def _get_local_ip() -> ipaddress.IPv4Address:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("224.0.0.231", 1))
        local_ip = sock.getsockname()[0]
    finally:
        sock.close()
    return ipaddress.IPv4Address(local_ip)
