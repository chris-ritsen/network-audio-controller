from __future__ import annotations

import asyncio
import hashlib
import logging
import re

from dbus_fast.aio import MessageBus
from dbus_fast import BusType

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.daemon.dbus_interfaces import (
    DanteChannelInterface,
    DanteDeviceInterface,
    ManagerInterface,
    ShureChannelInterface,
    ShureDeviceInterface,
)
from netaudio.daemon.dbus_state import (
    DANTE_PROPERTY_NAMES,
    SHURE_PROPERTY_NAMES,
    snapshot_dante_device,
    snapshot_shure_device,
)
from netaudio.dante.events import DanteEvent, EventType

logger = logging.getLogger("netaudio")

_SAFE_RE = re.compile(r"[^A-Za-z0-9_]")


def _safe_name(name: str) -> str:
    readable = _SAFE_RE.sub("_", name).strip("_") or "device"
    digest = hashlib.blake2s(name.encode("utf-8"), digest_size=6).hexdigest()
    return f"{readable}_{digest}"


def _safe_mac(mac: str) -> str:
    normalized = re.sub(r"[^0-9A-Fa-f]", "", mac).lower()
    if len(normalized) == 12:
        return normalized
    readable = _SAFE_RE.sub("_", mac.lower()).strip("_") or "device"
    digest = hashlib.blake2s(mac.encode("utf-8"), digest_size=6).hexdigest()
    return f"{readable}_{digest}"


class DBusService:
    def __init__(self, daemon):
        self._daemon = daemon
        self._bus: MessageBus | None = None
        self._manager: ManagerInterface | None = None
        self._dante_interfaces: dict[str, DanteDeviceInterface] = {}
        self._dante_paths: dict[str, str] = {}
        self._dante_channel_paths: dict[str, list[str]] = {}
        self._shure_interfaces: dict[str, ShureDeviceInterface] = {}
        self._shure_paths: dict[str, str] = {}
        self._shure_channel_paths: dict[str, list[str]] = {}
        self._prop_snapshots: dict[str, dict] = {}
        self._shure_prop_snapshots: dict[str, dict] = {}
        self._listeners_registered = False
        self._lifecycle_lock = DeferredAsyncioLock()

    async def start(self):
        async with self._lifecycle_lock:
            if self._bus is not None:
                return
            candidate_bus = MessageBus(bus_type=BusType.SESSION)
            try:
                self._bus = await candidate_bus.connect()
                reply = await self._bus.request_name("com.netaudio.Daemon")
                reply_value = getattr(reply, "value", reply)
                if reply_value is not None and reply_value not in (1, 4):
                    raise RuntimeError(f"D-Bus name com.netaudio.Daemon is unavailable (reply {reply})")

                self._manager = ManagerInterface(self._daemon)
                self._bus.export("/com/netaudio", self._manager)

                for server_name, device in self._daemon.devices.items():
                    if device.online:
                        self._export_dante_device(server_name, device)

                if self._daemon.shure:
                    for mac, device in self._daemon.shure.devices.items():
                        self._export_shure_device(mac, device)

                self._register_event_listeners()
            except BaseException:
                if self._bus is not None:
                    self._stop_locked()
                else:
                    try:
                        candidate_bus.disconnect()
                    except Exception as exception:
                        logger.debug(f"D-Bus failed-connect cleanup error: {exception}")
                raise

            logger.info("D-Bus service started: com.netaudio.Daemon")

    async def stop(self):
        async with self._lifecycle_lock:
            self._stop_locked()

    def _register_event_listeners(self):
        if self._listeners_registered:
            return
        dispatcher = self._daemon.application.dispatcher
        dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_dante_discovered)
        dispatcher.on(EventType.DEVICE_UPDATED, self._on_dante_updated)
        dispatcher.on(EventType.DEVICE_REMOVED, self._on_dante_removed)
        dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_discovered)
        dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_updated)
        dispatcher.on(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        self._listeners_registered = True

    def _unregister_event_listeners(self):
        if not self._listeners_registered:
            return
        dispatcher = self._daemon.application.dispatcher
        dispatcher.off(EventType.DEVICE_DISCOVERED, self._on_dante_discovered)
        dispatcher.off(EventType.DEVICE_UPDATED, self._on_dante_updated)
        dispatcher.off(EventType.DEVICE_REMOVED, self._on_dante_removed)
        dispatcher.off(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_discovered)
        dispatcher.off(EventType.SHURE_DEVICE_UPDATED, self._on_shure_updated)
        dispatcher.off(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        self._listeners_registered = False

    def _stop_locked(self):
        self._unregister_event_listeners()
        bus = self._bus

        for server_name in list(self._dante_interfaces):
            self._unexport_dante_device(server_name)

        for mac in list(self._shure_interfaces):
            self._unexport_shure_device(mac)

        if self._manager and bus:
            try:
                bus.unexport("/com/netaudio", self._manager)
            except Exception as exception:
                logger.debug(f"D-Bus manager unexport error: {exception}")
        self._manager = None

        if bus:
            try:
                bus.disconnect()
            except Exception as exception:
                logger.debug(f"D-Bus disconnect error: {exception}")
        self._bus = None
        self._dante_interfaces.clear()
        self._dante_paths.clear()
        self._dante_channel_paths.clear()
        self._shure_interfaces.clear()
        self._shure_paths.clear()
        self._shure_channel_paths.clear()
        self._prop_snapshots.clear()
        self._shure_prop_snapshots.clear()
        logger.info("D-Bus service stopped")

    def _export_dante_device(self, server_name, device):
        if server_name in self._dante_interfaces:
            return False
        if not self._bus:
            return False

        safe = _safe_name(server_name)
        path = f"/com/netaudio/dante/devices/{safe}"

        iface = DanteDeviceInterface(device)
        self._bus.export(path, iface)
        self._dante_interfaces[server_name] = iface
        self._dante_paths[server_name] = path
        self._prop_snapshots[server_name] = self._snapshot_dante(device)

        self._export_dante_channels(server_name, device, path)
        return True

    def _export_dante_channels(self, server_name, device, path):
        ch_paths = []
        if not self._bus:
            self._dante_channel_paths[server_name] = ch_paths
            return

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
                if self._bus:
                    self._bus.unexport(ch_path)
            except Exception:
                logger.exception(f"Failed to unexport D-Bus channel {ch_path}")

    def _unexport_dante_device(self, server_name):
        if server_name not in self._dante_interfaces:
            return False

        path = self._dante_paths.pop(server_name, None)

        self._unexport_dante_channels(server_name)

        iface = self._dante_interfaces.pop(server_name, None)
        if iface and path and self._bus:
            try:
                self._bus.unexport(path, iface)
            except Exception:
                logger.exception(f"Failed to unexport D-Bus device {path}")

        self._prop_snapshots.pop(server_name, None)
        return True

    def _export_shure_device(self, mac, device):
        if mac in self._shure_interfaces:
            return False
        if not self._bus:
            return False

        safe = _safe_mac(mac)
        path = f"/com/netaudio/shure/devices/{safe}"

        iface = ShureDeviceInterface(device)
        self._bus.export(path, iface)
        self._shure_interfaces[mac] = iface
        self._shure_paths[mac] = path
        self._shure_prop_snapshots[mac] = snapshot_shure_device(device)

        self._export_shure_channels(mac, device, path)
        return True

    def _export_shure_channels(self, mac, device, path):
        ch_paths = []
        if not self._bus:
            self._shure_channel_paths[mac] = ch_paths
            return
        if device.channels:
            for num, ch in device.channels.items():
                ch_path = f"{path}/channels/{num}"
                self._bus.export(ch_path, ShureChannelInterface(ch))
                ch_paths.append(ch_path)

        self._shure_channel_paths[mac] = ch_paths

    def _unexport_shure_device(self, mac):
        if mac not in self._shure_interfaces:
            return False

        path = self._shure_paths.pop(mac, None)

        for ch_path in self._shure_channel_paths.pop(mac, []):
            try:
                if self._bus:
                    self._bus.unexport(ch_path)
            except Exception:
                logger.exception(f"Failed to unexport Shure D-Bus channel {ch_path}")

        iface = self._shure_interfaces.pop(mac, None)
        if iface and path and self._bus:
            try:
                self._bus.unexport(path, iface)
            except Exception:
                logger.exception(f"Failed to unexport Shure D-Bus device {path}")
        self._shure_prop_snapshots.pop(mac, None)
        return True

    def _snapshot_dante(self, device):
        return snapshot_dante_device(device)

    def _emit_dante_changed(self, server_name, device):
        iface = self._dante_interfaces.get(server_name)
        if not iface:
            return

        iface._device = device
        new_snap = self._snapshot_dante(device)
        old_snap = self._prop_snapshots.get(server_name, {})

        changed = {}
        for key, prop_name in DANTE_PROPERTY_NAMES.items():
            if new_snap.get(key) != old_snap.get(key):
                changed[prop_name] = new_snap[key]

        if changed:
            try:
                iface.emit_properties_changed(changed)
            except Exception as e:
                logger.debug(f"D-Bus prop change emit error for {server_name}: {e}")
                return

        self._prop_snapshots[server_name] = new_snap

    def _emit_shure_changed(self, mac, device):
        iface = self._shure_interfaces.get(mac)
        if not iface:
            return

        iface._device = device
        new_snapshot = snapshot_shure_device(device)
        old_snapshot = self._shure_prop_snapshots.get(mac, {})
        changed = {
            property_name: new_snapshot[key]
            for key, property_name in SHURE_PROPERTY_NAMES.items()
            if new_snapshot.get(key) != old_snapshot.get(key)
        }
        if changed:
            try:
                iface.emit_properties_changed(changed)
            except Exception as exception:
                logger.debug(f"D-Bus Shure prop change emit error for {mac}: {exception}")
                return
        self._shure_prop_snapshots[mac] = new_snapshot

    def _sync_dante_channels(self, server_name, device):
        path = self._dante_paths.get(server_name)
        if not path:
            return

        self._unexport_dante_channels(server_name)
        self._export_dante_channels(server_name, device, path)

    def _sync_shure_channels(self, mac, device):
        path = self._shure_paths.get(mac)
        if not path:
            return
        for channel_path in self._shure_channel_paths.pop(mac, []):
            try:
                if self._bus:
                    self._bus.unexport(channel_path)
            except Exception:
                logger.exception(f"Failed to unexport Shure D-Bus channel {channel_path}")
        self._export_shure_channels(mac, device, path)

    def _dante_device_count(self):
        return sum(1 for d in self._daemon.devices.values() if d.online)

    def _shure_device_count(self):
        if not self._daemon.shure:
            return 0
        return len(self._daemon.shure.devices)

    async def _on_dante_discovered(self, event: DanteEvent):
        device = self._daemon.devices.get(event.server_name)
        if not device or not device.online:
            return

        added = self._export_dante_device(event.server_name, device)
        if not added:
            self._emit_dante_changed(event.server_name, device)
            self._sync_dante_channels(event.server_name, device)
            return

        if self._manager:
            self._manager.DanteDeviceAdded(event.server_name)
            self._manager.emit_properties_changed({"DanteDeviceCount": self._dante_device_count()})

    async def _on_dante_updated(self, event: DanteEvent):
        device = self._daemon.devices.get(event.server_name)
        if not device:
            return

        if not device.online:
            removed = self._unexport_dante_device(event.server_name)
            if removed and self._manager:
                self._manager.DanteDeviceRemoved(event.server_name)
                self._manager.emit_properties_changed({"DanteDeviceCount": self._dante_device_count()})
            return

        if event.server_name not in self._dante_interfaces:
            added = self._export_dante_device(event.server_name, device)
            if added and self._manager:
                self._manager.DanteDeviceAdded(event.server_name)
                self._manager.emit_properties_changed({"DanteDeviceCount": self._dante_device_count()})
            return

        self._emit_dante_changed(event.server_name, device)
        self._sync_dante_channels(event.server_name, device)

    async def _on_dante_removed(self, event: DanteEvent):
        removed = self._unexport_dante_device(event.server_name)

        if removed and self._manager:
            self._manager.DanteDeviceRemoved(event.server_name)
            self._manager.emit_properties_changed({"DanteDeviceCount": self._dante_device_count()})

    async def _on_shure_discovered(self, event: DanteEvent):
        if not self._daemon.shure:
            return
        device = self._daemon.shure.devices.get(event.device_name)
        if not device:
            return

        added = self._export_shure_device(event.device_name, device)
        if not added:
            self._emit_shure_changed(event.device_name, device)
            self._sync_shure_channels(event.device_name, device)
            return

        if self._manager:
            self._manager.ShureDeviceAdded(event.device_name)
            self._manager.emit_properties_changed({"ShureDeviceCount": self._shure_device_count()})

    async def _on_shure_updated(self, event: DanteEvent):
        if not self._daemon.shure:
            return
        device = self._daemon.shure.devices.get(event.device_name)
        if not device:
            await self._on_shure_removed(event)
            return

        if event.device_name not in self._shure_interfaces:
            added = self._export_shure_device(event.device_name, device)
            if added and self._manager:
                self._manager.ShureDeviceAdded(event.device_name)
                self._manager.emit_properties_changed({"ShureDeviceCount": self._shure_device_count()})
            return

        self._emit_shure_changed(event.device_name, device)
        self._sync_shure_channels(event.device_name, device)

    async def _on_shure_removed(self, event: DanteEvent):
        removed = self._unexport_shure_device(event.device_name)

        if removed and self._manager:
            self._manager.ShureDeviceRemoved(event.device_name)
            self._manager.emit_properties_changed({"ShureDeviceCount": self._shure_device_count()})
