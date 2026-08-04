import asyncio
import logging
import struct

from netaudio.dante.const import BLUETOOTH_MODEL_IDS
from netaudio.dante.device_parser import DanteDeviceParser
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.latency import unavailable_latency_controls
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

logger = logging.getLogger("netaudio")

CONMON_RETRY_TIMEOUTS = [3, 5, 10]


class DanteStateService:
    def __init__(self, application):
        self.application = application
        self._populating: set[str] = set()
        self._device_locks: dict[str, asyncio.Lock] = {}
        self._registered = False

    @property
    def devices(self) -> dict:
        return self.application.devices

    def register(self) -> None:
        if self._registered:
            return
        app = self.application
        app.on_notification(NOTIFICATION_TX_CHANNEL_CHANGE, self._on_channel_name_changed)
        app.on_notification(NOTIFICATION_RX_CHANNEL_CHANGE, self._on_channel_name_changed)
        app.on_notification(NOTIFICATION_TX_LABEL_CHANGE, self._on_channel_name_changed)
        app.on_notification(NOTIFICATION_SAMPLE_RATE_STATUS, self._on_sample_rate_status)
        app.on_notification(NOTIFICATION_ENCODING_STATUS, self._on_encoding_status)
        app.on_notification(NOTIFICATION_INTERFACE_STATUS, self._on_controls_changed)
        app.on_notification(NOTIFICATION_PROPERTY_CHANGE, self._on_controls_changed)
        app.on_notification(NOTIFICATION_DEVICE_REBOOT, self._on_device_reboot)
        app.on_notification(NOTIFICATION_AES67_STATUS, self._on_aes67_status)
        app.on_notification(NOTIFICATION_TX_FLOW_CHANGE, self._on_flow_changed)
        app.on_notification(NOTIFICATION_RX_FLOW_CHANGE, self._on_flow_changed)
        app.on_notification(NOTIFICATION_SETTINGS_CHANGE, self._on_settings_change)
        app.on_notification(NOTIFICATION_CLOCKING_STATUS, self._on_device_state_changed)
        app.on_notification(NOTIFICATION_VERSIONS_STATUS, self._on_device_state_changed)
        app.on_notification(NOTIFICATION_MANF_VERSIONS_STATUS, self._on_device_state_changed)
        app.on_notification(NOTIFICATION_CLEAR_CONFIG_STATUS, self._on_device_state_changed)
        app.on_notification(NOTIFICATION_ROUTING_READY, self._on_device_state_changed)
        app.on_notification(NOTIFICATION_ROUTING_DEVICE_CHANGE, self._on_routing_changed)
        self._registered = True

    def _lock_for(self, server_name: str) -> asyncio.Lock:
        lock = self._device_locks.get(server_name)
        if lock is None:
            lock = asyncio.Lock()
            self._device_locks[server_name] = lock
        return lock

    def _emit_device_updated(self, device) -> None:
        self.application.dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.DEVICE_UPDATED,
                device_name=device.name,
                server_name=device.server_name,
            )
        )

    def _online_device(self, server_name: str):
        device = self.devices.get(server_name)
        if not device or not device.online:
            return None
        device.update_last_seen()
        return device

    async def _on_channel_name_changed(self, event: DanteEvent) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device:
            return

        if not self.application.get_arc_port(device):
            return

        logger.info(f"Re-fetching channels for {server_name} (channel name changed)")
        async with self._lock_for(server_name):
            try:
                await device.get_tx_channels()
                await device.get_rx_channels()
            except Exception as exception:
                logger.warning(f"Error re-fetching channels for {server_name}: {exception}")
                return
        self._emit_device_updated(device)

    async def _on_flow_changed(self, event: DanteEvent) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device:
            return

        if not self.application.get_arc_port(device):
            return

        logger.info(f"Re-fetching subscriptions for {server_name} (flow changed)")
        async with self._lock_for(server_name):
            try:
                await device.get_rx_channels()
            except Exception as exception:
                logger.warning(f"Error re-fetching flows for {server_name}: {exception}")
                return
        self._emit_device_updated(device)

    async def _on_routing_changed(self, event: DanteEvent) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device:
            return

        if not self.application.get_arc_port(device):
            return

        logger.info(f"Re-fetching subscriptions for {server_name} (routing changed)")
        async with self._lock_for(server_name):
            try:
                await device.get_rx_channels()
            except Exception as exception:
                logger.warning(f"Error re-fetching subscriptions for {server_name}: {exception}")
                return
        self._emit_device_updated(device)

    async def _on_device_state_changed(self, event: DanteEvent) -> None:
        device = self._online_device(event.server_name)
        if not device:
            return
        await self.fetch_device_controls(event.server_name)
        if event.data.get("notification_id") == NOTIFICATION_CLEAR_CONFIG_STATUS and device.ipv4:
            await asyncio.gather(
                self._refresh_sample_rate_status(device, "configuration cleared"),
                self._refresh_encoding_status(device, "configuration cleared"),
            )

    async def _on_controls_changed(self, event: DanteEvent) -> None:
        if event.data.get("state_applied"):
            return
        if not self._online_device(event.server_name):
            return
        await self.refetch_device_controls(event.server_name)

    async def _on_sample_rate_status(self, event: DanteEvent) -> None:
        if event.data.get("conmon_response"):
            if event.data.get("current_value_changed"):
                device = self._online_device(event.server_name)
                if device:
                    await self._refresh_settings_after_sample_rate_change(device)
            return
        if event.data.get("state_applied"):
            return
        device = self._online_device(event.server_name)
        if not device or not device.ipv4:
            return
        await self._refresh_sample_rate_status(device, "sample rate changed")

    async def _refresh_settings_after_sample_rate_change(self, device) -> None:
        logger.info(f"Re-fetching device settings for {device.server_name} (sample rate changed)")
        async with self._lock_for(device.server_name):
            device.apply_controls(unavailable_latency_controls())
            try:
                settings = await device.operations.get_device_settings()
                if settings is None:
                    logger.warning(f"Device settings unavailable for {device.server_name} after sample rate changed")
            except Exception as exception:
                logger.warning(f"Error re-fetching device settings for {device.server_name}: {exception}")
        self._emit_device_updated(device)

    async def _refresh_sample_rate_status(self, device, reason: str) -> None:
        await self._refresh_capability_status(
            device,
            reason,
            "sample rate",
            self.application.probe_sample_rate_status,
        )

    async def _on_encoding_status(self, event: DanteEvent) -> None:
        if event.data.get("state_applied") or event.data.get("conmon_response"):
            return
        device = self._online_device(event.server_name)
        if not device or not device.ipv4:
            return
        await self._refresh_encoding_status(device, "encoding changed")

    async def _refresh_encoding_status(self, device, reason: str) -> None:
        await self._refresh_capability_status(
            device,
            reason,
            "encoding",
            self.application.probe_encoding_status,
        )

    async def _refresh_gain_status(self, device, reason: str) -> None:
        await self._refresh_capability_status(
            device,
            reason,
            "gain",
            self.application.probe_gain_status,
        )

    async def _refresh_capability_status(self, device, reason: str, capability_name: str, probe_status) -> None:
        logger.info(f"Re-fetching {capability_name} status for {device.server_name} ({reason})")
        try:
            await probe_status(str(device.ipv4))
        except Exception as exception:
            logger.warning(f"Error re-fetching {capability_name} status for {device.server_name}: {exception}")

    async def _on_device_reboot(self, event: DanteEvent) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device:
            return

        logger.info(f"Device rebooted: {server_name}")
        if device.ipv4:
            await self.application.cmc.register_device(str(device.ipv4))
        await self.refetch_device_controls(server_name)

    async def _on_aes67_status(self, event: DanteEvent) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device:
            return

        if not self.application.get_arc_port(device):
            return

        logger.info(f"Re-fetching AES67 status for {server_name}")
        try:
            result = await self.application.probe_aes67_state(str(device.ipv4))
            if result:
                aes67_current, aes67_configured = result
                if aes67_current is not None:
                    device.aes67_current = aes67_current
                if aes67_configured is not None:
                    device.aes67_configured = aes67_configured
        except Exception as exception:
            logger.warning(f"Error re-fetching AES67 for {server_name}: {exception}")
        await asyncio.gather(
            self._refresh_sample_rate_status(device, "AES67 status changed"),
            self._refresh_encoding_status(device, "AES67 status changed"),
        )
        self._emit_device_updated(device)

    async def _on_settings_change(self, event: DanteEvent) -> None:
        raw = event.data.get("raw")
        if not raw or len(raw) < 36:
            return

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
            self._emit_device_updated(device)

        return True

    async def refetch_device_controls(self, server_name: str) -> None:
        device = self._online_device(server_name)
        if not device:
            return

        if not self.application.get_arc_port(device) or not device.ipv4:
            return

        logger.info(f"Re-fetching controls for {server_name}")
        async with self._lock_for(server_name):
            try:
                controls = await device.fetch_controls_data()
                if controls:
                    device.apply_controls(controls)
            except Exception as exception:
                logger.warning(f"Error re-fetching controls for {server_name}: {exception}")
                return
        self._emit_device_updated(device)

    async def refresh_device(self, server_name: str) -> None:
        self._populating.discard(server_name)
        await self.fetch_device_controls(server_name)

    async def refresh_all_devices(self) -> None:
        tasks = []
        for server_name, device in self.devices.items():
            if device.online:
                self._populating.discard(server_name)
                tasks.append(self.fetch_device_controls(server_name))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def refresh_affected_subscriptions(self, offline_device) -> None:
        offline_name = offline_device.name
        if not offline_name:
            return

        for server_name, device in self.devices.items():
            if not device.online or device is offline_device:
                continue

            has_sub = any(s.tx_device_name == offline_name for s in device.subscriptions)
            if not has_sub:
                continue

            if not self.application.get_arc_port(device):
                continue

            logger.info(f"Re-fetching subscriptions for {server_name} (TX device {offline_name} went offline)")
            async with self._lock_for(server_name):
                try:
                    await device.get_rx_channels()
                except Exception as exception:
                    logger.warning(f"Error re-fetching subscriptions for {server_name}: {exception}")
                    continue
            self._emit_device_updated(device)

    async def fetch_device_controls(self, server_name: str, delay: float = 0) -> None:
        if server_name in self._populating:
            return

        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        if not self.application.get_arc_port(device):
            return

        self._populating.add(server_name)

        try:
            if delay > 0:
                await asyncio.sleep(delay)

            async with self._lock_for(server_name):
                retries = 3
                for attempt in range(retries):
                    controls = await device.fetch_controls_data()
                    if controls:
                        device.apply_controls(controls)

                    if device.name and device.tx_count is not None:
                        break

                    if attempt < retries - 1:
                        logger.debug(f"Incomplete controls for {server_name}, retrying ({attempt + 1}/{retries})")
                        await asyncio.sleep(2)

                if device.bluetooth_device is None and device.model_id in BLUETOOTH_MODEL_IDS:
                    self.application.settings.request_bluetooth_status(str(device.ipv4))

                try:
                    device_ip = str(device.ipv4)
                    for _ in range(3):
                        result = await self.application.probe_aes67_state(device_ip)
                        if result:
                            aes67_current, aes67_configured = result
                            if aes67_current is not None:
                                device.aes67_current = aes67_current
                            if aes67_configured is not None:
                                device.aes67_configured = aes67_configured
                            break
                except Exception as exception:
                    logger.warning(f"Error probing AES67 for {server_name}: {exception}")

                capability_tasks = []
                if device.supported_sample_rates is None:
                    capability_tasks.append(self._refresh_sample_rate_status(device, "device discovered"))
                if device.supported_encodings is None:
                    capability_tasks.append(self._refresh_encoding_status(device, "device discovered"))
                if device.supported_gain_levels is None:
                    capability_tasks.append(self._refresh_gain_status(device, "device discovered"))
                if capability_tasks:
                    await asyncio.gather(*capability_tasks)

                try:
                    device_ip = str(device.ipv4)
                    for _ in range(3):
                        preferred_leader = await self.application.probe_preferred_leader_state(device_ip)
                        if preferred_leader is not None:
                            device.preferred_leader = preferred_leader
                            break
                except Exception as exception:
                    logger.warning(f"Error probing preferred leader for {server_name}: {exception}")

                if device.interfaces is None or device.link_speed_mbps is None:
                    try:
                        interfaces = await self.application.probe_interface_status(str(device.ipv4))
                        if interfaces is not None:
                            device.interfaces = interfaces
                    except Exception as exception:
                        logger.warning(f"Error probing interface status for {server_name}: {exception}")

            logger.info(f"Fetched controls for {server_name}")
            self._emit_device_updated(device)
        except Exception as exception:
            logger.warning(f"Error fetching controls for {server_name}: {exception}")
        finally:
            self._populating.discard(server_name)

    async def retry_conmon_query(self, server_name: str) -> None:
        for attempt, timeout in enumerate(CONMON_RETRY_TIMEOUTS, 1):
            device = self.devices.get(server_name)

            if not device or not device.online:
                return

            if device.dante_model_id:
                return

            if not device.ipv4 or not device.mac_address:
                return

            device_ip = str(device.ipv4)
            waiter = self.application.notifications.register_conmon_waiter(device_ip, expected_count=1)

            try:
                logger.debug(f"Conmon retry {attempt} for {server_name}")
                self.application._send_conmon_query_for_device(device, "dante_model")
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                continue
            finally:
                self.application.notifications.unregister_conmon_waiter(device_ip)

            if device.dante_model_id:
                logger.debug(f"Conmon dante_model populated for {server_name}: {device.dante_model_id}")
                return

        logger.debug(f"Conmon dante_model still missing for {server_name} after retries")
