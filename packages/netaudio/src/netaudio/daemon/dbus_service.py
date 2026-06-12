import logging
import re

from dbus_fast.aio import MessageBus
from dbus_fast import BusType

from netaudio.daemon.dbus_interfaces import (
    DanteChannelInterface,
    DanteDeviceInterface,
    ManagerInterface,
    ShureChannelInterface,
    ShureDeviceInterface,
)
from netaudio.dante.events import DanteEvent, EventType

logger = logging.getLogger("netaudio")

_SAFE_RE = re.compile(r"[^A-Za-z0-9_]")


def _safe_name(name: str) -> str:
    return _SAFE_RE.sub("_", name)


def _safe_mac(mac: str) -> str:
    return mac.lower().replace(":", "").replace("-", "")[:12]


class DBusService:
    def __init__(self, daemon):
        self._daemon = daemon
        self._bus: MessageBus | None = None
        self._manager: ManagerInterface | None = None
        self._dante_interfaces: dict[str, DanteDeviceInterface] = {}
        self._dante_channel_paths: dict[str, list[str]] = {}
        self._shure_interfaces: dict[str, ShureDeviceInterface] = {}
        self._shure_channel_paths: dict[str, list[str]] = {}
        self._prop_snapshots: dict[str, dict] = {}

    async def start(self):
        self._bus = await MessageBus(bus_type=BusType.SESSION).connect()
        await self._bus.request_name("com.netaudio.Daemon")

        self._manager = ManagerInterface(self._daemon)
        self._bus.export("/com/netaudio", self._manager)

        for server_name, device in self._daemon.devices.items():
            if device.online:
                self._export_dante_device(server_name, device)

        if self._daemon.shure:
            for mac, device in self._daemon.shure.devices.items():
                self._export_shure_device(mac, device)

        dispatcher = self._daemon.application.dispatcher
        dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_dante_discovered)
        dispatcher.on(EventType.DEVICE_UPDATED, self._on_dante_updated)
        dispatcher.on(EventType.DEVICE_REMOVED, self._on_dante_removed)
        dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_discovered)
        dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_updated)
        dispatcher.on(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)

        logger.info("D-Bus service started: com.netaudio.Daemon")

    async def stop(self):
        if not self._bus:
            return

        for server_name in list(self._dante_interfaces):
            self._unexport_dante_device(server_name)

        for mac in list(self._shure_interfaces):
            self._unexport_shure_device(mac)

        if self._manager:
            self._bus.unexport("/com/netaudio", self._manager)
            self._manager = None

        self._bus.disconnect()
        self._bus = None
        logger.info("D-Bus service stopped")

    def _export_dante_device(self, server_name, device):
        if server_name in self._dante_interfaces:
            return

        safe = _safe_name(server_name)
        path = f"/com/netaudio/dante/devices/{safe}"

        iface = DanteDeviceInterface(device)
        self._bus.export(path, iface)
        self._dante_interfaces[server_name] = iface
        self._prop_snapshots[server_name] = self._snapshot_dante(device)

        self._export_dante_channels(server_name, device, path)

    def _export_dante_channels(self, server_name, device, path):
        ch_paths = []

        if device.tx_channels:
            for num, ch in device.tx_channels.items():
                ch_path = f"{path}/tx/{num}"
                self._bus.export(ch_path, DanteChannelInterface(ch))
                ch_paths.append(ch_path)

        if device.rx_channels:
            for num, ch in device.rx_channels.items():
                ch_path = f"{path}/rx/{num}"
                self._bus.export(ch_path, DanteChannelInterface(ch))
                ch_paths.append(ch_path)

        self._dante_channel_paths[server_name] = ch_paths

    def _unexport_dante_channels(self, server_name):
        for ch_path in self._dante_channel_paths.pop(server_name, []):
            try:
                self._bus.unexport(ch_path)
            except Exception:
                pass

    def _unexport_dante_device(self, server_name):
        if server_name not in self._dante_interfaces:
            return

        safe = _safe_name(server_name)
        path = f"/com/netaudio/dante/devices/{safe}"

        self._unexport_dante_channels(server_name)

        iface = self._dante_interfaces.pop(server_name, None)
        if iface:
            try:
                self._bus.unexport(path, iface)
            except Exception:
                pass

        self._prop_snapshots.pop(server_name, None)

    def _export_shure_device(self, mac, device):
        if mac in self._shure_interfaces:
            return

        safe = _safe_mac(mac)
        path = f"/com/netaudio/shure/devices/{safe}"

        iface = ShureDeviceInterface(device)
        self._bus.export(path, iface)
        self._shure_interfaces[mac] = iface

        ch_paths = []
        if device.channels:
            for num, ch in device.channels.items():
                ch_path = f"{path}/channels/{num}"
                self._bus.export(ch_path, ShureChannelInterface(ch))
                ch_paths.append(ch_path)

        self._shure_channel_paths[mac] = ch_paths

    def _unexport_shure_device(self, mac):
        if mac not in self._shure_interfaces:
            return

        safe = _safe_mac(mac)
        path = f"/com/netaudio/shure/devices/{safe}"

        for ch_path in self._shure_channel_paths.pop(mac, []):
            try:
                self._bus.unexport(ch_path)
            except Exception:
                pass

        iface = self._shure_interfaces.pop(mac, None)
        if iface:
            try:
                self._bus.unexport(path, iface)
            except Exception:
                pass

    def _snapshot_dante(self, device):
        return {
            "name": device.name or "",
            "ipv4": str(device.ipv4) if device.ipv4 else "",
            "sample_rate": device.sample_rate or 0,
            "encoding": device.encoding or 0,
            "bit_depth": device.bit_depth or 0,
            "latency": int(device.latency) if device.latency else 0,
            "online": device.online,
            "is_locked": bool(device.is_locked),
            "aes67_enabled": bool(device.aes67_enabled),
            "clock_role": device.clock_role or "",
            "tx_count": device.tx_count or 0,
            "rx_count": device.rx_count or 0,
        }

    def _emit_dante_changed(self, server_name, device):
        iface = self._dante_interfaces.get(server_name)
        if not iface:
            return

        new_snap = self._snapshot_dante(device)
        old_snap = self._prop_snapshots.get(server_name, {})

        changed = {}
        prop_map = {
            "name": "Name",
            "ipv4": "Ipv4",
            "sample_rate": "SampleRate",
            "encoding": "Encoding",
            "bit_depth": "BitDepth",
            "latency": "Latency",
            "online": "Online",
            "is_locked": "IsLocked",
            "aes67_enabled": "Aes67Enabled",
            "clock_role": "ClockRole",
            "tx_count": "TxCount",
            "rx_count": "RxCount",
        }

        for key, prop_name in prop_map.items():
            if new_snap.get(key) != old_snap.get(key):
                changed[prop_name] = new_snap[key]

        if changed:
            try:
                iface.emit_properties_changed(changed)
            except Exception as e:
                logger.debug(f"D-Bus prop change emit error for {server_name}: {e}")

        self._prop_snapshots[server_name] = new_snap

    def _sync_dante_channels(self, server_name, device):
        safe = _safe_name(server_name)
        path = f"/com/netaudio/dante/devices/{safe}"

        self._unexport_dante_channels(server_name)
        self._export_dante_channels(server_name, device, path)

    def _dante_device_count(self):
        return sum(1 for d in self._daemon.devices.values() if d.online)

    def _shure_device_count(self):
        if not self._daemon.shure:
            return 0
        return len(self._daemon.shure.devices)

    async def _on_dante_discovered(self, event: DanteEvent):
        device = self._daemon.devices.get(event.server_name)
        if not device:
            return

        self._export_dante_device(event.server_name, device)

        if self._manager:
            self._manager.DanteDeviceAdded(event.server_name)
            self._manager.emit_properties_changed({"DanteDeviceCount": self._dante_device_count()})

    async def _on_dante_updated(self, event: DanteEvent):
        device = self._daemon.devices.get(event.server_name)
        if not device:
            return

        if event.server_name not in self._dante_interfaces:
            self._export_dante_device(event.server_name, device)
            return

        self._emit_dante_changed(event.server_name, device)
        self._sync_dante_channels(event.server_name, device)

    async def _on_dante_removed(self, event: DanteEvent):
        self._unexport_dante_device(event.server_name)

        if self._manager:
            self._manager.DanteDeviceRemoved(event.server_name)
            self._manager.emit_properties_changed({"DanteDeviceCount": self._dante_device_count()})

    async def _on_shure_discovered(self, event: DanteEvent):
        if not self._daemon.shure:
            return
        device = self._daemon.shure.devices.get(event.device_name)
        if not device:
            return

        self._export_shure_device(event.device_name, device)

        if self._manager:
            self._manager.ShureDeviceAdded(event.device_name)
            self._manager.emit_properties_changed({"ShureDeviceCount": self._shure_device_count()})

    async def _on_shure_updated(self, event: DanteEvent):
        iface = self._shure_interfaces.get(event.device_name)
        if not iface:
            return

        try:
            iface.emit_properties_changed({})
        except Exception:
            pass

    async def _on_shure_removed(self, event: DanteEvent):
        self._unexport_shure_device(event.device_name)

        if self._manager:
            self._manager.ShureDeviceRemoved(event.device_name)
            self._manager.emit_properties_changed({"ShureDeviceCount": self._shure_device_count()})
