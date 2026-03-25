import asyncio
import json
import logging
import os
import pickle
import struct
import sys

from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from netaudio.common.socket_path import (
    DaemonAlreadyRunningError,
    cleanup_daemon_socket,
    start_daemon_server,
)
from netaudio.daemon.enforcement import EnforcementManager
from netaudio.daemon.metering import MeteringManager
from netaudio.daemon.relay import RelayServer
from netaudio.shure.manager import ShureManager
from netaudio.dante.services.heartbeat import DanteHeartbeatService
from netaudio.daemon.protocol import (
    CMD_DEVICE_REQUEST,
    CMD_GET_DEVICES_JSON,
    CMD_METER_SNAPSHOT,
    CMD_METER_START,
    CMD_METER_STATUS,
    CMD_SHUTDOWN,
    CMD_METER_STOP,
    CMD_REPORT_UNRESPONSIVE,
)
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import (
    BLUETOOTH_MODEL_IDS,
    DEVICE_CONTROL_PORT,
    DEVICE_SETTINGS_PORT,
    SERVICE_CMC,
    SERVICES,
)
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_parser import DanteDeviceParser
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.services.notification import (
    NOTIFICATION_AES67_STATUS,
    NOTIFICATION_CLEAR_CONFIG_STATUS,
    NOTIFICATION_CLOCKING_STATUS,
    NOTIFICATION_DEVICE_REBOOT,
    NOTIFICATION_ENCODING_STATUS,
    NOTIFICATION_INTERFACE_STATUS,
    NOTIFICATION_MANF_VERSIONS_STATUS,
    NOTIFICATION_PROPERTY_CHANGE,
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_ROUTING_READY,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_SAMPLE_RATE_STATUS,
    NOTIFICATION_SETTINGS_CHANGE,
    NOTIFICATION_TX_CHANNEL_CHANGE,
    NOTIFICATION_TX_FLOW_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE,
    NOTIFICATION_VERSIONS_STATUS,
)

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

try:
    from netaudio.daemon.dbus_service import DBusService as _DBusService
except ImportError:
    _DBusService = None

logger = logging.getLogger("netaudio")


def _sd_notify(state):
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    import socket as _socket
    sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_DGRAM)
    try:
        if addr[0] == "@":
            addr = "\0" + addr[1:]
        sock.sendto(state.encode(), addr)
    finally:
        sock.close()


class NetaudioDaemon:
    def __init__(self, dissect=False, capture=False, relay_port=None):
        self._capture = capture
        self._relay_port = relay_port
        self._packet_store = None
        self._session_id = None

        from netaudio.common.config_loader import load_capture_profile, resolve_db_from_config

        profile_cfg, _ = load_capture_profile(None, None)

        from netaudio.common.app_config import settings as app_settings
        lock_key_value = profile_cfg.get("device_lock_key")
        if lock_key_value:
            app_settings.device_lock_key = lock_key_value.encode("ascii")
        else:
            from netaudio.common.key_extract import extract_lock_key
            extracted_key = extract_lock_key()
            if extracted_key:
                app_settings.device_lock_key = extracted_key
                logger.info("Extracted device lock key from Dante Controller")

        if capture:
            from netaudio.dante.packet_store import PacketStore

            db_path = resolve_db_from_config(None, profile_cfg)
            self._packet_store = PacketStore(db_path=db_path)

            active_session = self._packet_store.get_latest_session(active_only=True)
            if active_session:
                self._session_id = active_session["id"]
                logger.info(f"Capture: recording to session #{self._session_id}")
            else:
                logger.info("Capture: enabled but no active session")

        self.application = DanteApplication(packet_store=self._packet_store, dissect=dissect)

        if self._packet_store and self._session_id:
            for service in [self.application.arc, self.application.settings, self.application.cmc, self.application.notifications]:
                service.session_id = self._session_id

        self.server = None
        self.zeroconf = None
        self.browser = None
        self.running = False
        self._redis = None
        self._populating: set[str] = set()
        self.metering: MeteringManager | None = None
        self.relay: RelayServer | None = None
        self.heartbeat: DanteHeartbeatService | None = None
        self.shure: ShureManager | None = None
        self._dbus = None

    @property
    def devices(self) -> dict:
        return self.application.devices

    async def _connect_redis(self):
        if aioredis is None:
            logger.info("redis.asyncio not available, running without Redis")
            return

        try:
            redis_socket = os.environ.get("REDIS_SOCKET")
            redis_host = os.environ.get("REDIS_HOST") or "localhost"
            redis_port = int(os.environ.get("REDIS_PORT") or 6379)
            redis_db = int(os.environ.get("REDIS_DB") or 0)

            if redis_socket:
                self._redis = aioredis.Redis(unix_socket_path=redis_socket, db=redis_db)
            else:
                self._redis = aioredis.Redis(host=redis_host, port=redis_port, db=redis_db)

            await self._redis.ping()
            await self._redis.config_set("notify-keyspace-events", "Kgh$")
            logger.info("Connected to Redis")
        except Exception as exception:
            logger.info(f"Redis not available, continuing without it: {exception}")
            self._redis = None

    async def _publish_device_to_redis(self, device):
        if not self._redis:
            return

        key = f"netaudio:daemon:device:{device.server_name}"
        try:
            await self._redis.hset(
                key,
                mapping={
                    "server_name": device.server_name or "",
                    "name": device.name or "",
                    "ipv4": str(device.ipv4) if device.ipv4 else "",
                    "model_id": device.model_id or "",
                    "bluetooth_device": device.bluetooth_device or "",
                    "online": "1" if device.online else "0",
                    "last_seen": str(device.last_seen) if device.last_seen else "",
                },
            )
        except Exception as exception:
            logger.debug(f"Redis publish error for {device.server_name}: {exception}")

    async def _delete_device_from_redis(self, server_name):
        if not self._redis:
            return

        key = f"netaudio:daemon:device:{server_name}"
        try:
            await self._redis.delete(key)
        except Exception as exception:
            logger.debug(f"Redis delete error for {server_name}: {exception}")

    def _load_shure_correlations(self):
        try:
            from netaudio.common.config_loader import default_config_path
            path = default_config_path()
            if not path.exists():
                return {}
            import tomllib
            data = tomllib.loads(path.read_text())
            return data.get("shure", {}).get("correlations", {})
        except Exception:
            return {}

    @staticmethod
    def _normalize_mac(mac):
        return mac.lower().replace(":", "").replace("-", "")[:12]

    def _find_dante_for_shure(self, shure_device):
        from netaudio.shure.device import ShureDeviceType
        if shure_device.device_type != ShureDeviceType.ad4d:
            return None

        correlations = self._load_shure_correlations()
        dante_mac = correlations.get(shure_device.mac)
        if not dante_mac:
            return None

        normalized_target = self._normalize_mac(dante_mac)
        for device in self.application.devices.values():
            if device.mac_address and self._normalize_mac(device.mac_address) == normalized_target:
                return device

        return None

    def _dante_device_to_dict(self, device):
        result = {
            "server_name": device.server_name or "",
            "name": device.name or "",
            "ip": str(device.ipv4) if device.ipv4 else "",
            "mac": device.mac_address or "",
            "sample_rate": device.sample_rate,
            "encoding": device.encoding,
            "latency": device.latency,
        }

        if device.tx_channels:
            result["tx_channels"] = {
                str(num): {"name": ch.name or "", "friendly_name": ch.friendly_name or ""}
                for num, ch in sorted(device.tx_channels.items())
                if ch.name
            }

        if device.rx_channels:
            result["rx_channels"] = {
                str(num): {"name": ch.name or "", "friendly_name": ch.friendly_name or ""}
                for num, ch in sorted(device.rx_channels.items())
                if ch.name
            }

        if device.subscriptions:
            subs = []
            for sub in device.subscriptions:
                subs.append({
                    "rx_channel_name": sub.rx_channel_name or "",
                    "tx_device_name": sub.tx_device_name or "",
                    "tx_channel_name": sub.tx_channel_name or "",
                    "status": ", ".join(sub.status_text()) if sub.status_code is not None else "",
                })
            result["subscriptions"] = subs

        return result

    async def _publish_shure_to_redis(self, mac):
        if not self._redis:
            return

        device = self.shure.devices.get(mac)
        if not device:
            return

        data = device.to_json()

        dante = self._find_dante_for_shure(device)
        if dante:
            data["dante"] = self._dante_device_to_dict(dante)

        try:
            await self._redis.set(f"netaudio:shure:{mac}", json.dumps(data))
        except Exception as exception:
            logger.debug(f"Redis publish error for Shure {mac}: {exception}")

    async def _publish_shure_meters_to_redis(self, mac, data):
        if not self._redis:
            return

        try:
            await self._redis.set(f"netaudio:shure:meters:{mac}", json.dumps(data))
        except Exception as exception:
            logger.debug(f"Redis meter publish error for Shure {mac}: {exception}")

    async def _delete_shure_from_redis(self, mac):
        if not self._redis:
            return

        try:
            await self._redis.delete(f"netaudio:shure:{mac}", f"netaudio:shure:meters:{mac}")
        except Exception as exception:
            logger.debug(f"Redis delete error for Shure {mac}: {exception}")

    async def _on_shure_discovered(self, event: DanteEvent):
        logger.info(f"Shure device discovered: {event.device_name}")
        await self._publish_shure_to_redis(event.device_name)

    async def _on_shure_updated(self, event: DanteEvent):
        await self._publish_shure_to_redis(event.device_name)

    async def _on_shure_removed(self, event: DanteEvent):
        logger.info(f"Shure device removed: {event.device_name}")
        await self._delete_shure_from_redis(event.device_name)

    async def _on_shure_meters(self, event: DanteEvent):
        await self._publish_shure_meters_to_redis(event.device_name, event.data)

    def _register_event_listeners(self):
        self.application.dispatcher.on(EventType.DEVICE_DISCOVERED, self._on_device_discovered)
        self.application.dispatcher.on(EventType.DEVICE_UPDATED, self._on_device_updated)
        self.application.dispatcher.on(EventType.DEVICE_REMOVED, self._on_device_removed)
        self.application.dispatcher.on(EventType.SHURE_DEVICE_DISCOVERED, self._on_shure_discovered)
        self.application.dispatcher.on(EventType.SHURE_DEVICE_UPDATED, self._on_shure_updated)
        self.application.dispatcher.on(EventType.SHURE_DEVICE_REMOVED, self._on_shure_removed)
        self.application.dispatcher.on(EventType.SHURE_METER_VALUES, self._on_shure_meters)

        self.application.on_notification(NOTIFICATION_TX_CHANNEL_CHANGE, self._on_channel_name_changed)
        self.application.on_notification(NOTIFICATION_RX_CHANNEL_CHANGE, self._on_channel_name_changed)
        self.application.on_notification(NOTIFICATION_TX_LABEL_CHANGE, self._on_channel_name_changed)
        self.application.on_notification(NOTIFICATION_SAMPLE_RATE_STATUS, self._on_sample_rate_changed)
        self.application.on_notification(NOTIFICATION_ENCODING_STATUS, self._on_encoding_status)
        self.application.on_notification(NOTIFICATION_INTERFACE_STATUS, self._on_interface_status)
        self.application.on_notification(NOTIFICATION_DEVICE_REBOOT, self._on_device_reboot)
        self.application.on_notification(NOTIFICATION_AES67_STATUS, self._on_aes67_status)
        self.application.on_notification(NOTIFICATION_TX_FLOW_CHANGE, self._on_flow_changed)
        self.application.on_notification(NOTIFICATION_RX_FLOW_CHANGE, self._on_flow_changed)
        self.application.on_notification(NOTIFICATION_PROPERTY_CHANGE, self._on_property_changed)
        self.application.on_notification(NOTIFICATION_SETTINGS_CHANGE, self._on_settings_change)
        self.application.on_notification(NOTIFICATION_CLOCKING_STATUS, self._on_device_state_changed)
        self.application.on_notification(NOTIFICATION_VERSIONS_STATUS, self._on_device_state_changed)
        self.application.on_notification(NOTIFICATION_MANF_VERSIONS_STATUS, self._on_device_state_changed)
        self.application.on_notification(NOTIFICATION_CLEAR_CONFIG_STATUS, self._on_device_state_changed)
        self.application.on_notification(NOTIFICATION_ROUTING_READY, self._on_device_state_changed)
        self.application.on_notification(NOTIFICATION_ROUTING_DEVICE_CHANGE, self._on_routing_changed)

    async def _republish_correlated_shure(self, dante_device):
        if not self.shure:
            return
        correlations = self._load_shure_correlations()
        dante_mac = dante_device.mac_address or ""
        if not dante_mac:
            return
        normalized_dante = self._normalize_mac(dante_mac)
        for shure_mac, corr_dante_mac in correlations.items():
            if self._normalize_mac(corr_dante_mac) == normalized_dante:
                await self._publish_shure_to_redis(shure_mac)

    async def _on_device_discovered(self, event: DanteEvent):
        device = self.devices.get(event.server_name)
        if device:
            device.update_last_seen()
            logger.info(f"Device discovered (event): {event.server_name}")
            if device.ipv4:
                await self.application.cmc.register_device(str(device.ipv4))
            await self._publish_device_to_redis(device)
            await self._republish_correlated_shure(device)

    async def _on_device_updated(self, event: DanteEvent):
        device = self.devices.get(event.server_name)
        if device:
            await self._publish_device_to_redis(device)
            await self._republish_correlated_shure(device)

    async def _on_device_removed(self, event: DanteEvent):
        logger.info(f"Device removed (event): {event.server_name}")
        if self.metering:
            self.metering.cleanup_device(event.server_name)
        device = self.devices.get(event.server_name)
        if device:
            await self._publish_device_to_redis(device)
            await self._refresh_affected_subscriptions(device)
        else:
            await self._delete_device_from_redis(event.server_name)

    async def _refresh_affected_subscriptions(self, offline_device):
        offline_name = offline_device.name
        if not offline_name:
            return

        for server_name, device in self.devices.items():
            if not device.online or device is offline_device:
                continue

            has_sub = any(s.tx_device_name == offline_name for s in device.subscriptions)
            if not has_sub:
                continue

            arc_port = self.application.get_arc_port(device)
            if not arc_port:
                continue

            logger.info(f"Re-fetching subscriptions for {server_name} (TX device {offline_name} went offline)")
            try:
                rx_channels, subscriptions = await self.application.arc.get_rx_channels(device, arc_port)
                device.rx_channels = rx_channels
                device.subscriptions = subscriptions
                await self._publish_device_to_redis(device)
            except Exception as e:
                logger.debug(f"Error re-fetching subscriptions for {server_name}: {e}")

    async def _on_channel_name_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching channels for {server_name} (channel name changed)")
        try:
            device.tx_channels = await self.application.arc.get_tx_channels(device, arc_port)
            rx_channels, subscriptions = await self.application.arc.get_rx_channels(device, arc_port)
            device.rx_channels = rx_channels
            device.subscriptions = subscriptions
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching channels for {server_name}: {exception}")

    async def _on_flow_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching flow counts for {server_name} (flow changed)")
        try:
            counts = await self.application.arc.get_channel_count(str(device.ipv4), arc_port)
            if "tx_flow_count" in counts:
                device.tx_flow_count = counts["tx_flow_count"]
            if "rx_flow_count" in counts:
                device.rx_flow_count = counts["rx_flow_count"]
            rx_channels, subscriptions = await self.application.arc.get_rx_channels(device, arc_port)
            device.rx_channels = rx_channels
            device.subscriptions = subscriptions
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching flows for {server_name}: {exception}")

    async def _on_device_state_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        await self._fetch_device_controls(server_name)

    async def _on_routing_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching subscriptions for {server_name} (routing changed)")
        try:
            rx_channels, subscriptions = await self.application.arc.get_rx_channels(device, arc_port)
            device.rx_channels = rx_channels
            device.subscriptions = subscriptions
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching subscriptions for {server_name}: {exception}")

    async def _on_sample_rate_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        await self._refetch_device_controls(server_name)

    async def _on_encoding_status(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        await self._refetch_device_controls(server_name)

    async def _on_interface_status(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        await self._refetch_device_controls(server_name)

    async def _on_device_reboot(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        logger.info(f"Device rebooted: {server_name}")
        if device.ipv4:
            await self.application.cmc.register_device(str(device.ipv4))
        await self._refetch_device_controls(server_name)

    async def _on_aes67_status(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()

        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching AES67 status for {server_name}")
        try:
            aes67_status = await self.application.arc.get_aes67_config(str(device.ipv4), arc_port)
            if aes67_status is not None:
                device.aes67_enabled = aes67_status
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching AES67 for {server_name}: {exception}")

    async def _on_property_changed(self, event: DanteEvent):
        server_name = event.server_name
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        device.update_last_seen()
        await self._refetch_device_controls(server_name)

    async def _on_settings_change(self, event: DanteEvent):
        raw = event.data.get("raw")
        if not raw or len(raw) < 36:
            return

        source_ip = event.data.get("source_ip")
        device = self.devices.get(event.server_name)
        if not device:
            return

        settings_subtype = struct.unpack(">H", raw[34:36])[0]

        if settings_subtype == 0x000C:
            if not self._handle_bluetooth(raw, device):
                logger.debug(f"Unhandled bluetooth settings packet from {event.server_name}: {raw.hex()}")
        else:
            logger.debug(f"Unhandled settings subtype 0x{settings_subtype:04X} from {event.server_name}: {raw.hex()}")

    def _handle_bluetooth(self, data: bytes, device) -> bool:
        name = DanteDeviceParser.parse_bluetooth_status(data)

        if name is False:
            return False

        old_name = device.bluetooth_device

        if name != old_name:
            device.bluetooth_device = name
            logger.info(f"Bluetooth status changed for {device.server_name}: {old_name!r} -> {name!r}")

        return True

    async def _refetch_device_controls(self, server_name: str):
        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        arc_port = self.application.get_arc_port(device)
        device_ip = str(device.ipv4) if device.ipv4 else None
        if not arc_port or not device_ip:
            return

        logger.info(f"Re-fetching controls for {server_name}")
        try:
            await self.application.arc.get_controls(device, arc_port)
            await self._publish_device_to_redis(device)
        except Exception as exception:
            logger.debug(f"Error re-fetching controls for {server_name}: {exception}")

    async def start(self):
        self.running = True

        _sd_notify("STATUS=Connecting to Redis...")
        await self._connect_redis()

        self.server = await start_daemon_server(self.handle_client)

        logger.info("Daemon listening")

        _sd_notify("STATUS=Starting application...")
        await self.application.startup()

        self.metering = MeteringManager(self.application)
        await self.metering.start()

        self.shure = ShureManager(self.application.dispatcher)
        await self.shure.start()

        self.relay = RelayServer(self, port=self._relay_port)
        await self.relay.start()

        from netaudio.common.app_config import settings as app_settings
        self.heartbeat = DanteHeartbeatService(
            device_by_ip=self.application._device_by_ip,
            get_devices=lambda: self.application.devices,
            mark_offline=self.application.mark_device_offline,
            interface_ip=app_settings.interface_ip,
        )
        await self.heartbeat.start()

        self.enforcement = EnforcementManager(self)
        await self.enforcement.start()

        self._register_event_listeners()

        if _DBusService:
            try:
                self._dbus = _DBusService(self)
                await self._dbus.start()
            except Exception as e:
                logger.info(f"D-Bus not available, continuing without it: {e}")
                self._dbus = None

        if self.shure:
            for mac in self.shure.devices:
                await self._publish_shure_to_redis(mac)

        _sd_notify("STATUS=Starting mDNS browser...")
        self.zeroconf = AsyncZeroconf()
        self.browser = AsyncServiceBrowser(
            self.zeroconf.zeroconf,
            SERVICES,
            handlers=[self.on_service_state_change],
        )

        logger.info("mDNS browser started, watching for devices...")

        _sd_notify("READY=1\nSTATUS=Discovering devices...")

        if hasattr(self.server, "serve_forever"):
            async with self.server:
                await self.server.serve_forever()
        else:
            while self.running:
                await asyncio.sleep(1)

    async def stop(self):
        self.running = False

        if self._dbus:
            try:
                await self._dbus.stop()
            except Exception:
                pass
            self._dbus = None

        if self.heartbeat:
            await self.heartbeat.stop()

        if hasattr(self, "enforcement") and self.enforcement:
            await self.enforcement.stop()

        if self.shure:
            await self.shure.stop()

        if self.relay:
            await self.relay.stop()

        if self.metering:
            await self.metering.stop()

        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass

        await self.application.shutdown()

        if self._packet_store:
            try:
                self._packet_store.close()
            except Exception:
                pass

        if self.server:
            self.server.close()
            try:
                await self.server.wait_closed()
            except Exception:
                pass

        try:
            if self.browser:
                await self.browser.async_cancel()
        except Exception:
            pass

        try:
            if self.zeroconf:
                await self.zeroconf.async_close()
        except Exception:
            pass

        cleanup_daemon_socket()

    def on_service_state_change(self, zeroconf, service_type, name, state_change):
        if service_type == "_netaudio-chan._udp.local.":
            return

        logger.debug(f"mDNS event: {state_change.name} - {service_type} - {name}")

        asyncio.create_task(self.handle_service_change(zeroconf, service_type, name, state_change))

    async def handle_service_change(self, zeroconf, service_type, name, state_change):
        try:
            info = AsyncServiceInfo(service_type, name)

            if state_change == ServiceStateChange.Removed:
                for server_name in list(self.devices.keys()):
                    if name.startswith(server_name.replace(".local.", "")):
                        logger.info(f"Device offline (mDNS removed): {server_name}")
                        self.application.mark_device_offline(server_name)
                        online = sum(1 for d in self.devices.values() if d.online)
                        _sd_notify(f"STATUS={online} device(s) online")

                return

            if not await info.async_request(zeroconf, 3000):
                return

            addresses = info.parsed_addresses()

            if not addresses:
                return

            server_name = None

            for record in zeroconf.cache.entries_with_name(name):
                if hasattr(record, "server"):
                    server_name = record.server
                    break

            if not server_name:
                return

            service_properties = {}

            for key, value in info.properties.items():
                key = key.decode("utf-8") if isinstance(key, bytes) else key

                if not key:
                    continue

                if isinstance(value, bytes):
                    value = value.decode("utf-8")

                service_properties[key] = value

            service_data = {
                "ipv4": addresses[0],
                "name": name,
                "port": info.port,
                "properties": service_properties,
                "server_name": server_name,
                "type": service_type,
            }

            existing = self.devices.get(server_name)
            was_offline = existing is not None and not existing.online
            is_new = existing is None

            if is_new or was_offline:
                new_device = DanteDevice(server_name=server_name)
                new_device.ipv4 = addresses[0]
                self.application.register_device(server_name, new_device)
                if is_new:
                    logger.info(f"Device discovered: {server_name}")
                else:
                    logger.info(f"Device back online: {server_name}")
                online = sum(1 for d in self.devices.values() if d.online)
                _sd_notify(f"STATUS={online} device(s) online")
                if addresses[0]:
                    await self.application.cmc.register_device(addresses[0])
                if was_offline and self.metering:
                    self.metering.reactivate_device(server_name)

            device = self.devices[server_name]
            device.update_last_seen()

            old_ip = str(device.ipv4) if device.ipv4 else None
            new_ip = addresses[0]

            if old_ip and old_ip != new_ip:
                logger.info(f"Device {server_name} IP changed: {old_ip} -> {new_ip}")

            device.ipv4 = new_ip

            if not device.services:
                device.services = {}

            device.services[name] = service_data

            if "id" in service_properties and service_type == SERVICE_CMC:
                device.mac_address = service_properties["id"]

                if device.ipv4 and device.mac_address:
                    if not device.dante_model:
                        self.application._send_conmon_query_for_device(device, "make_model")

                    if not device.dante_model_id:
                        self.application._send_conmon_query_for_device(device, "dante_model")
                        asyncio.create_task(self._retry_conmon_query(server_name))

            if "model" in service_properties:
                device.model_id = service_properties["model"]

            if "mf" in service_properties:
                device.manufacturer_mdns = service_properties["mf"]
                if not device.manufacturer:
                    device.manufacturer = service_properties["mf"]

            if "server_vers" in service_properties and service_type == SERVICE_CMC:
                device.software_version = service_properties["server_vers"]

            if "router_vers" in service_properties:
                device.firmware_version = service_properties["router_vers"]

            if "rate" in service_properties:
                device.sample_rate = int(service_properties["rate"])

            if "latency_ns" in service_properties:
                device.latency = int(service_properties["latency_ns"])

            await self._publish_device_to_redis(device)

            arc_port = self.application.get_arc_port(device)
            if arc_port:
                if not is_new:
                    new_name = await self.application.arc.get_device_name(new_ip, arc_port)
                    if new_name and new_name != device.name:
                        logger.info(f"Device name changed for {server_name}: {device.name!r} -> {new_name!r}")
                        device.name = new_name
                        await self._publish_device_to_redis(device)

                if not device.tx_channels and not device.rx_channels:
                    asyncio.create_task(self._fetch_device_controls(server_name, delay=2))

        except Exception as exception:
            logger.debug(f"Service change error: {exception}")

    async def refresh_device(self, server_name: str) -> None:
        self._populating.discard(server_name)
        await self._fetch_device_controls(server_name)

    async def refresh_all_devices(self) -> None:
        tasks = []
        for server_name, device in self.devices.items():
            if device.online:
                self._populating.discard(server_name)
                tasks.append(self._fetch_device_controls(server_name))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_device_controls(self, server_name: str, delay: float = 0) -> None:
        if server_name in self._populating:
            return

        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        self._populating.add(server_name)

        try:
            if delay > 0:
                await asyncio.sleep(delay)

            retries = 3
            for attempt in range(retries):
                await self.application.arc.get_controls(device, arc_port)

                if device.name and device.tx_count is not None:
                    break

                if attempt < retries - 1:
                    logger.debug(f"Incomplete controls for {server_name}, retrying ({attempt + 1}/{retries})")
                    await asyncio.sleep(2)

            if device.bluetooth_device is None and device.model_id in BLUETOOTH_MODEL_IDS:
                device_ip = str(device.ipv4)
                self.application.settings.request_bluetooth_status(device_ip)

            logger.info(f"Fetched controls for {server_name}")
            await self._publish_device_to_redis(device)
            self.application.dispatcher.emit_nowait(
                DanteEvent(type=EventType.DEVICE_UPDATED, server_name=server_name)
            )
        except Exception as exception:
            logger.debug(f"Error fetching controls for {server_name}: {exception}")
        finally:
            self._populating.discard(server_name)

    @staticmethod
    def _is_identify_packet(packet: bytes) -> bool:
        if len(packet) < 6:
            return False
        protocol_id = struct.unpack(">H", packet[0:2])[0]
        command_id = struct.unpack(">H", packet[4:6])[0]
        return protocol_id == 0xFFFF and command_id == 0x0BC8

    async def _retry_conmon_query(self, server_name: str) -> None:
        delays = [3, 5, 10]

        for attempt, delay in enumerate(delays, 1):
            await asyncio.sleep(delay)

            device = self.devices.get(server_name)

            if not device or not device.online:
                return

            if device.dante_model_id:
                return

            if not device.ipv4 or not device.mac_address:
                return

            logger.debug(f"Conmon retry {attempt} for {server_name}")
            self.application._send_conmon_query_for_device(device, "dante_model")

        await asyncio.sleep(5)

        device = self.devices.get(server_name)

        if device and device.dante_model_id:
            logger.debug(f"Conmon dante_model populated for {server_name}: {device.dante_model_id}")
        elif device:
            logger.debug(f"Conmon dante_model still missing for {server_name} after retries")

    async def handle_client(self, reader, writer):
        try:
            cmd = await reader.read(1)

            if cmd == CMD_SHUTDOWN:
                logger.info("Shutdown command received")
                writer.close()
                await writer.wait_closed()
                self.running = False

                if self.server:
                    self.server.close()

                return

            if cmd == CMD_REPORT_UNRESPONSIVE:
                length_data = await reader.readexactly(4)
                length = struct.unpack(">I", length_data)[0]
                server_name = (await reader.readexactly(length)).decode("utf-8")

                device = self.devices.get(server_name)
                if device and device.online:
                    logger.info(f"Device unresponsive, marking offline: {server_name}")
                    self.application.mark_device_offline(server_name)

                writer.close()
                await writer.wait_closed()
                return

            if cmd == CMD_METER_SNAPSHOT:
                length_data = await reader.readexactly(4)
                length = struct.unpack(">I", length_data)[0]
                server_name = (await reader.readexactly(length)).decode("utf-8")

                device = self.devices.get(server_name)
                if not device or not device.ipv4:
                    result = json.dumps({"error": "device not found"})
                elif not self.metering:
                    result = json.dumps({"error": "metering not available"})
                else:
                    levels = await self.metering.snapshot(server_name, timeout=3.0)
                    if levels is None:
                        result = json.dumps({"error": "no metering data"})
                    else:
                        tx_names = {}
                        if device.tx_channels:
                            for ch in device.tx_channels.values():
                                tx_names[ch.number] = ch.friendly_name or ch.name
                        rx_names = {}
                        if device.rx_channels:
                            for ch in device.rx_channels.values():
                                rx_names[ch.number] = ch.friendly_name or ch.name

                        response = {
                            "tx": {},
                            "rx": {},
                            "wall_time": levels.get("wall_time"),
                            "source_ip": levels.get("source_ip"),
                        }
                        for ch_num, level in levels.get("tx", {}).items():
                            response["tx"][ch_num] = {
                                "name": tx_names.get(ch_num, ""),
                                "level": level,
                            }
                        for ch_num, level in levels.get("rx", {}).items():
                            response["rx"][ch_num] = {
                                "name": rx_names.get(ch_num, ""),
                                "level": level,
                            }
                        result = json.dumps(response)

                data = result.encode()
                length = struct.pack(">I", len(data))
                writer.write(length + data)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            if cmd == CMD_METER_START:
                length_data = await reader.readexactly(4)
                length = struct.unpack(">I", length_data)[0]
                server_name = (await reader.readexactly(length)).decode("utf-8")
                length_data = await reader.readexactly(4)
                length = struct.unpack(">I", length_data)[0]
                client_id = (await reader.readexactly(length)).decode("utf-8")

                device = self.devices.get(server_name)
                if device and self.metering:
                    self.metering.add_persistent(server_name, client_id)

                writer.close()
                await writer.wait_closed()
                return

            if cmd == CMD_METER_STOP:
                length_data = await reader.readexactly(4)
                length = struct.unpack(">I", length_data)[0]
                server_name = (await reader.readexactly(length)).decode("utf-8")
                length_data = await reader.readexactly(4)
                length = struct.unpack(">I", length_data)[0]
                client_id = (await reader.readexactly(length)).decode("utf-8")

                device = self.devices.get(server_name)
                if device and self.metering:
                    self.metering.remove_persistent(server_name, client_id)

                writer.close()
                await writer.wait_closed()
                return

            if cmd == CMD_METER_STATUS:
                if self.metering:
                    status = self.metering.get_status()
                else:
                    status = {}

                data = json.dumps(status).encode()
                length = struct.pack(">I", len(data))
                writer.write(length + data)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            if cmd == CMD_DEVICE_REQUEST:
                ip_len_data = await reader.readexactly(4)
                ip_len = struct.unpack(">I", ip_len_data)[0]
                device_ip = (await reader.readexactly(ip_len)).decode("utf-8")

                port_data = await reader.readexactly(2)
                port = struct.unpack(">H", port_data)[0]

                pkt_len_data = await reader.readexactly(4)
                pkt_len = struct.unpack(">I", pkt_len_data)[0]
                packet = await reader.readexactly(pkt_len)

                try:
                    if port == DEVICE_SETTINGS_PORT and self._is_identify_packet(packet):
                        self.application.settings.send(packet, device_ip, port)
                        response = None
                    elif port == DEVICE_SETTINGS_PORT:
                        response = await self.application.settings.request(
                            packet,
                            device_ip,
                            port,
                            logical_command_name="daemon_proxy",
                        )
                    elif port == DEVICE_CONTROL_PORT:
                        response = await self.application.cmc.request(
                            packet,
                            device_ip,
                            port,
                            logical_command_name="daemon_proxy",
                        )
                    else:
                        response = await self.application.arc.request(
                            packet,
                            device_ip,
                            port,
                            logical_command_name="daemon_proxy",
                        )
                except Exception as exc:
                    logger.debug(f"Device request proxy error: {exc}")
                    response = None

                if response is not None:
                    writer.write(b"\x01")
                    writer.write(struct.pack(">I", len(response)))
                    writer.write(response)
                else:
                    writer.write(b"\x00")
                    writer.write(struct.pack(">I", 0))

                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            if cmd == CMD_GET_DEVICES_JSON:
                devices_json = {}
                for server_name, device in self.devices.items():
                    devices_json[server_name] = {
                        "server_name": device.server_name,
                        "name": device.name,
                        "ipv4": str(device.ipv4) if device.ipv4 else None,
                        "model_id": device.model_id,
                        "bluetooth_device": device.bluetooth_device,
                        "online": device.online,
                        "last_seen": device.last_seen,
                    }
                data = json.dumps(devices_json).encode()
            else:
                devices_for_client = {}
                for server_name, device in self.devices.items():
                    client_device = DanteDevice(
                        server_name=device.server_name,
                        dump_payloads=False,
                        debug=False,
                    )
                    client_device._ipv4 = device._ipv4
                    client_device.name = device.name
                    client_device.mac_address = device.mac_address
                    client_device.model_id = device.model_id
                    client_device.sample_rate = device.sample_rate
                    client_device.latency = device.latency
                    client_device.services = device.services
                    client_device.manufacturer = device.manufacturer
                    client_device.software = device.software
                    client_device.bluetooth_device = device.bluetooth_device
                    client_device.tx_channels = device.tx_channels
                    client_device.rx_channels = device.rx_channels
                    client_device.subscriptions = device.subscriptions
                    client_device.tx_count = device.tx_count
                    client_device.rx_count = device.rx_count
                    client_device.tx_count_raw = device.tx_count_raw
                    client_device.rx_count_raw = device.rx_count_raw
                    client_device.aes67_enabled = device.aes67_enabled
                    client_device.error = str(device.error) if device.error else None
                    client_device.dante_model = device.dante_model
                    client_device.dante_model_id = device.dante_model_id
                    client_device.online = device.online
                    client_device.last_seen = device.last_seen
                    client_device.tx_flow_count = device.tx_flow_count
                    client_device.rx_flow_count = device.rx_flow_count
                    client_device.num_networks = device.num_networks
                    client_device.encoding = device.encoding
                    client_device.bit_depth = device.bit_depth
                    client_device.software_version = device.software_version
                    client_device.firmware_version = device.firmware_version
                    client_device.clock_role = device.clock_role
                    client_device.clock_mac = device.clock_mac
                    client_device.min_latency = device.min_latency
                    client_device.max_latency = device.max_latency
                    client_device.product_version = device.product_version
                    client_device.board_name = device.board_name
                    client_device.model = device.model
                    client_device.is_locked = device.is_locked
                    devices_for_client[server_name] = client_device
                data = pickle.dumps(devices_for_client)

            length = struct.pack(">I", len(data))
            writer.write(length + data)
            await writer.drain()
        except (BrokenPipeError, ConnectionResetError, ConnectionError):
            pass
        except Exception as exception:
            logger.error(f"Client handler error: {exception}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass


async def run_daemon(dissect=False, capture=False, relay_port=None):
    import signal

    daemon = NetaudioDaemon(dissect=dissect, capture=capture, relay_port=relay_port)
    loop = asyncio.get_running_loop()

    def handle_signal():
        daemon.running = False
        if daemon.server:
            daemon.server.close()

    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, handle_signal)

    try:
        await daemon.start()
    except DaemonAlreadyRunningError as error:
        logger.error(str(error))
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
    except asyncio.CancelledError:
        pass
    finally:
        await daemon.stop()
        logger.info("Daemon stopped")
