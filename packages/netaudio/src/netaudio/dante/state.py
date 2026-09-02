from __future__ import annotations

import asyncio
import logging

from netaudio.common.app_config import settings as app_settings
from netaudio.dante import flows
from netaudio.dante.const import (
    BLUETOOTH_MODEL_IDS,
    NOTIFICATION_AES67_STATUS,
    NOTIFICATION_CLEAR_CONFIG_STATUS,
    NOTIFICATION_CLOCKING_STATUS,
    NOTIFICATION_DEVICE_REBOOT,
    NOTIFICATION_ENCODING_STATUS,
    NOTIFICATION_MANF_VERSIONS_STATUS,
    NOTIFICATION_PROPERTY_CHANGE,
    NOTIFICATION_ROUTING_DEVICE_CHANGE,
    NOTIFICATION_ROUTING_READY,
    NOTIFICATION_RX_CHANNEL_CHANGE,
    NOTIFICATION_RX_FLOW_CHANGE,
    NOTIFICATION_SAMPLE_RATE_STATUS,
    NOTIFICATION_TX_CHANNEL_CHANGE,
    NOTIFICATION_TX_FLOW_CHANGE,
    NOTIFICATION_TX_LABEL_CHANGE,
    NOTIFICATION_VERSIONS_STATUS,
)
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.latency import unavailable_latency_controls
from netaudio.dante.services.notification_packet_handlers import (
    STATUS_KIND_AES67,
    STATUS_KIND_BLUETOOTH,
    STATUS_KIND_CLEAR_CONFIGURATION,
    STATUS_KIND_CLOCK,
    STATUS_KIND_DANTE_MODEL,
    STATUS_KIND_ENCODING,
    STATUS_KIND_GAIN,
    STATUS_KIND_INTERFACE,
    STATUS_KIND_LOCK,
    STATUS_KIND_MAKE_MODEL,
    STATUS_KIND_ROUTING_CAPACITY,
    STATUS_KIND_SAMPLE_RATE,
    STATUS_KIND_SAMPLE_RATE_PULLUP,
)

logger = logging.getLogger("netaudio")

CONMON_RETRY_TIMEOUTS = [3, 5, 10]

ALWAYS_OVERWRITTEN_MODEL_FIELDS = frozenset({"manufacturer"})

FIELD_STATUS_KINDS = frozenset(
    {
        STATUS_KIND_AES67,
        STATUS_KIND_CLEAR_CONFIGURATION,
        STATUS_KIND_CLOCK,
        STATUS_KIND_ENCODING,
        STATUS_KIND_GAIN,
        STATUS_KIND_INTERFACE,
        STATUS_KIND_LOCK,
        STATUS_KIND_SAMPLE_RATE,
        STATUS_KIND_SAMPLE_RATE_PULLUP,
    }
)


def _assign_changed(device, fields: dict) -> bool:
    changed = False
    for field_name, value in fields.items():
        if getattr(device, field_name) != value:
            setattr(device, field_name, value)
            changed = True
    return changed


def apply_device_status(device, kind: str, status) -> bool:
    if kind == STATUS_KIND_AES67:
        return _assign_changed(device, {name: value for name, value in status.items() if value is not None})
    if kind == STATUS_KIND_BLUETOOTH:
        return _assign_changed(
            device,
            {"bluetooth_connected": status["connected"], "bluetooth_device": status["device_name"]},
        )
    if kind == STATUS_KIND_CLEAR_CONFIGURATION:
        return _assign_changed(device, {"clear_configuration_status": status})
    if kind == STATUS_KIND_LOCK:
        return _assign_changed(device, {"is_locked": status["is_locked"], "lock_reset_status": status})
    if kind in (STATUS_KIND_DANTE_MODEL, STATUS_KIND_MAKE_MODEL):
        fields = {
            name: value
            for name, value in status.items()
            if name in ALWAYS_OVERWRITTEN_MODEL_FIELDS or not getattr(device, name, None)
        }
        return _assign_changed(device, fields)
    if kind == STATUS_KIND_ROUTING_CAPACITY:
        fields = dict(status)
        if status["routing_ready"] is True and device.tx_count is None:
            fields["tx_count"] = status["routing_capacity_transmit_channel_count"]
            fields["tx_count_raw"] = status["routing_capacity_transmit_channel_count"]
        if status["routing_ready"] is True and device.rx_count is None:
            fields["rx_count"] = status["routing_capacity_receive_channel_count"]
            fields["rx_count_raw"] = status["routing_capacity_receive_channel_count"]
        return _assign_changed(device, fields)
    if kind in FIELD_STATUS_KINDS:
        return _assign_changed(device, status)
    return False


class DanteStateService:
    def __init__(self, application):
        self.application = application
        self._device_locks: dict[str, asyncio.Lock] = {}
        self._pending_status: dict[str, list[tuple[str, object]]] = {}
        self._populating: set[str] = set()
        self._refetching = False
        self._status_applied = False
        self._notification_handlers = {
            NOTIFICATION_AES67_STATUS: self._on_aes67_status,
            NOTIFICATION_CLEAR_CONFIG_STATUS: self._on_device_state_changed,
            NOTIFICATION_CLOCKING_STATUS: self._on_device_state_changed,
            NOTIFICATION_DEVICE_REBOOT: self._on_device_reboot,
            NOTIFICATION_ENCODING_STATUS: self._on_encoding_status,
            NOTIFICATION_MANF_VERSIONS_STATUS: self._on_device_state_changed,
            NOTIFICATION_PROPERTY_CHANGE: self._on_controls_changed,
            NOTIFICATION_ROUTING_DEVICE_CHANGE: self._on_routing_changed,
            NOTIFICATION_ROUTING_READY: self._on_device_state_changed,
            NOTIFICATION_RX_CHANNEL_CHANGE: self._on_receiver_channel_changed,
            NOTIFICATION_RX_FLOW_CHANGE: self._on_receiver_flow_changed,
            NOTIFICATION_SAMPLE_RATE_STATUS: self._on_sample_rate_status,
            NOTIFICATION_TX_CHANNEL_CHANGE: self._on_transmitter_channel_changed,
            NOTIFICATION_TX_FLOW_CHANGE: self._on_transmitter_flow_changed,
            NOTIFICATION_TX_LABEL_CHANGE: self._on_transmitter_channel_changed,
            NOTIFICATION_VERSIONS_STATUS: self._on_device_state_changed,
        }

    @property
    def devices(self) -> dict:
        return self.application.devices

    @property
    def refetching(self) -> bool:
        return self._refetching

    def attach(self) -> None:
        if self._status_applied:
            return
        self.application.dispatcher.on(EventType.DEVICE_STATUS_RECEIVED, self.on_device_status)
        self._status_applied = True

    def register(self) -> None:
        self.attach()
        if self._refetching:
            return
        self.application.dispatcher.on(EventType.NOTIFICATION_RECEIVED, self._on_notification)
        self._refetching = True

    async def _on_notification(self, event: DanteEvent) -> None:
        handler = self._notification_handlers.get(event.data.get("notification_id"))
        if handler is None:
            logger.debug(f"Unhandled notification: {event.data.get('notification_name')} from {event.server_name}")
            return
        await handler(event)

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

    def apply_pending_for_device(self, device) -> None:
        if not device.ipv4:
            return
        pending = self._pending_status.pop(str(device.ipv4), None)
        if not pending:
            return
        for kind, status in pending:
            apply_device_status(device, kind, status)
        logger.debug(f"Applied pending status for {device.ipv4}: {sorted({kind for kind, _ in pending})}")

    async def on_device_status(self, event: DanteEvent) -> None:
        kind = event.data["kind"]
        status = event.data["status"]
        source_ip = event.data["source_ip"]
        device = self.application._device_by_ip(source_ip)
        if device is None:
            self._pending_status.setdefault(source_ip, []).append((kind, status))
            return
        if not device.online and kind not in (STATUS_KIND_DANTE_MODEL, STATUS_KIND_MAKE_MODEL):
            return

        if kind == STATUS_KIND_SAMPLE_RATE:
            current_value_changed = (
                device.sample_rate is not None and device.sample_rate != status[STATUS_KIND_SAMPLE_RATE]
            )
            changed = apply_device_status(device, kind, status)
            if current_value_changed and self._refetching:
                await self._refresh_controls_after_sample_rate_change(device)
                return
        else:
            changed = apply_device_status(device, kind, status)

        if changed:
            self._emit_device_updated(device)

        if not self._refetching:
            return
        if kind == STATUS_KIND_ROUTING_CAPACITY and status["routing_ready"] is True:
            await self.fetch_device_controls(device.server_name)
        elif kind == STATUS_KIND_CLEAR_CONFIGURATION:
            await self.fetch_device_controls(device.server_name)
            await asyncio.gather(
                self._refresh_sample_rate_status(device, "configuration cleared"),
                self._refresh_encoding_status(device, "configuration cleared"),
            )

    async def _refetch_channels(self, event: DanteEvent, description: str, fetch) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device:
            return

        if not self.application.get_arc_port(device):
            return

        logger.info(f"Re-fetching {description} for {server_name}")
        async with self._lock_for(server_name):
            try:
                await fetch(device)
            except (RuntimeError, OSError) as exception:
                logger.warning(f"Error re-fetching {description} for {server_name}: {exception}")
                return
        self._emit_device_updated(device)

    async def _on_transmitter_channel_changed(self, event: DanteEvent) -> None:
        await self._refetch_channels(
            event,
            "transmitter channels (transmitter channel changed)",
            lambda device: device.get_tx_channels(),
        )

    async def _on_receiver_channel_changed(self, event: DanteEvent) -> None:
        await self._refetch_channels(
            event,
            "receiver channels (receiver channel changed)",
            lambda device: device.get_rx_channels(),
        )

    async def _on_routing_changed(self, event: DanteEvent) -> None:
        await self._refetch_channels(
            event,
            "subscriptions (routing changed)",
            lambda device: device.get_rx_channels(),
        )

    async def _on_transmitter_flow_changed(self, event: DanteEvent) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device or not device.ipv4:
            return

        arc_port = self.application.get_arc_port(device)
        if not arc_port:
            return

        logger.info(f"Re-fetching transmitter flows for {server_name} (transmitter flow changed)")
        async with self._lock_for(server_name):
            try:
                flow_protocol_identifier = device.flow_protocol_id
                if flow_protocol_identifier is None:
                    flow_protocol_identifier = await flows.detect_flow_protocol(str(device.ipv4), arc_port)
                    if flow_protocol_identifier is None:
                        logger.warning(f"No supported transmitter flow frontend for {server_name}")
                        return
                    device.flow_protocol_id = flow_protocol_identifier
                flow_inventory = await flows.query_preferred_tx_flow_inventory(
                    str(device.ipv4),
                    arc_port,
                    flow_protocol_identifier,
                )
                if flow_inventory is None:
                    logger.warning(f"Transmitter flow inventory unavailable for {server_name}")
                    return
                flow_records = flow_inventory.get("flows")
                if not isinstance(flow_records, list):
                    logger.warning(f"Malformed transmitter flow inventory for {server_name}")
                    return
                device.apply_transmitter_flow_status_page(
                    {
                        "reported_flow_count": len(flow_records),
                        "flows": flow_records,
                    }
                )
            except (RuntimeError, OSError) as exception:
                logger.warning(f"Error re-fetching transmitter flows for {server_name}: {exception}")
                return
        self._emit_device_updated(device)

    async def _on_receiver_flow_changed(self, event: DanteEvent) -> None:
        server_name = event.server_name
        device = self._online_device(server_name)
        if not device:
            return

        if not self.application.get_arc_port(device):
            return

        logger.info(f"Re-fetching receiver flows for {server_name} (receiver flow changed)")
        async with self._lock_for(server_name):
            try:
                await device.get_rx_channels()
            except (RuntimeError, OSError) as exception:
                logger.warning(f"Error re-fetching receiver channels for {server_name}: {exception}")
                return
            try:
                flow_inventory = await flows.query_preferred_receiver_flow_inventory(device)
                if flow_inventory is None:
                    logger.warning(f"Receiver flow inventory unavailable for {server_name}")
                else:
                    flow_records = flow_inventory.get("flows")
                    if isinstance(flow_records, list):
                        device.apply_receiver_flow_status_page(
                            {
                                "reported_flow_count": len(flow_records),
                                "flows": flow_records,
                            }
                        )
                    else:
                        logger.warning(f"Malformed receiver flow inventory for {server_name}")
            except (RuntimeError, OSError) as exception:
                logger.warning(f"Error re-fetching receiver flow inventory for {server_name}: {exception}")
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
        if not self._online_device(event.server_name):
            return
        await self.refetch_device_controls(event.server_name)

    async def _on_sample_rate_status(self, event: DanteEvent) -> None:
        device = self._online_device(event.server_name)
        if not device or not device.ipv4:
            return
        await self._refresh_sample_rate_status(device, "sample rate changed")

    async def _refresh_controls_after_sample_rate_change(self, device) -> None:
        logger.info(f"Re-fetching channels and device settings for {device.server_name} (sample rate changed)")
        async with self._lock_for(device.server_name):
            device.apply_controls(unavailable_latency_controls())
            try:
                controls = await device.fetch_controls_data(include_channels=True)
                if controls is None:
                    logger.warning(f"Device controls unavailable for {device.server_name} after sample rate changed")
                else:
                    device.apply_controls(controls)
            except (RuntimeError, OSError) as exception:
                logger.warning(f"Error re-fetching device controls for {device.server_name}: {exception}")
        self._emit_device_updated(device)

    async def _refresh_sample_rate_status(self, device, reason: str) -> None:
        await self._refresh_capability_status(
            device,
            reason,
            "sample rate",
            self.application.probe_sample_rate_status,
        )

    async def _on_encoding_status(self, event: DanteEvent) -> None:
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
        except (RuntimeError, OSError) as exception:
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
            await self.application.probe_aes67_state(str(device.ipv4))
        except (RuntimeError, OSError) as exception:
            logger.warning(f"Error re-fetching AES67 for {server_name}: {exception}")
        await asyncio.gather(
            self._refresh_sample_rate_status(device, "AES67 status changed"),
            self._refresh_encoding_status(device, "AES67 status changed"),
        )
        self._emit_device_updated(device)

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
            except (RuntimeError, OSError) as exception:
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
        await asyncio.gather(*tasks)

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
                except (RuntimeError, OSError) as exception:
                    logger.warning(f"Error re-fetching subscriptions for {server_name}: {exception}")
                    continue
            self._emit_device_updated(device)

    async def _probe_with_retries(self, device, description: str, probe, retries: int = 3) -> None:
        for _ in range(retries):
            try:
                await probe()
            except (RuntimeError, OSError) as exception:
                logger.debug(f"{description} probe attempt failed for {device.server_name}: {exception}")
                continue
            return
        logger.warning(f"Error probing {description} for {device.server_name}")

    async def fetch_device_controls(self, server_name: str) -> None:
        if server_name in self._populating:
            return

        device = self.devices.get(server_name)
        if not device or not device.online:
            return

        if not self.application.get_arc_port(device):
            return

        self._populating.add(server_name)

        try:
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

                device_ip = str(device.ipv4)
                if device.bluetooth_connected is None and device.model_id in BLUETOOTH_MODEL_IDS:
                    await self.application.settings.request_bluetooth_status(device_ip)

                await self._probe_with_retries(
                    device,
                    "AES67",
                    lambda: self.application.probe_aes67_state(device_ip),
                )

                capability_tasks = []
                if device.supported_sample_rates is None:
                    capability_tasks.append(self._refresh_sample_rate_status(device, "device discovered"))
                if device.supported_encodings is None:
                    capability_tasks.append(self._refresh_encoding_status(device, "device discovered"))
                if device.supported_gain_levels is None:
                    capability_tasks.append(self._refresh_gain_status(device, "device discovered"))
                if capability_tasks:
                    await asyncio.gather(*capability_tasks)

                await self._probe_with_retries(
                    device,
                    "preferred leader",
                    lambda: self.application.probe_preferred_leader_state(device_ip),
                )

                if device.interfaces is None or device.link_speed_mbps is None:
                    try:
                        await self.application.probe_interface_status(device_ip)
                    except (RuntimeError, OSError) as exception:
                        logger.warning(f"Error probing interface status for {server_name}: {exception}")

                await self._refresh_clock_status(device, "device discovered")
                await self._refresh_lock_status(device, "device discovered")

            logger.info(f"Fetched controls for {server_name}")
            self._emit_device_updated(device)
        except (RuntimeError, OSError) as exception:
            logger.warning(f"Error fetching controls for {server_name}: {exception}")
        finally:
            self._populating.discard(server_name)

    async def refresh_status_fields(self, server_name: str, reason: str) -> None:
        device = self.devices.get(server_name)
        if not device or not device.online or not device.ipv4:
            return
        async with self._lock_for(server_name):
            clock_changed = await self._refresh_clock_status(device, reason)
            lock_changed = await self._refresh_lock_status(device, reason)
        if clock_changed or lock_changed:
            self._emit_device_updated(device)

    async def _refresh_clock_status(self, device, reason: str) -> bool:
        before = (device.clock_role, device.clock_source_code)
        try:
            await self.application.probe_clocking_status(device)
        except (RuntimeError, OSError) as exception:
            logger.debug(f"Clock status unavailable for {device.server_name} ({reason}): {exception}")
            return False
        return (device.clock_role, device.clock_source_code) != before

    async def _refresh_lock_status(self, device, reason: str) -> bool:
        before = (device.is_locked, device.lock_reset_status)
        try:
            await self.application.probe_lock_status(
                str(device.ipv4),
                timeout=app_settings.lock_state_timeout,
            )
        except (RuntimeError, OSError) as exception:
            logger.debug(f"Lock status unavailable for {device.server_name} ({reason}): {exception}")
            return False
        return (device.is_locked, device.lock_reset_status) != before

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
                await self.application._send_conmon_query_for_device(
                    device,
                    self.application.settings.request_dante_model,
                )
                await asyncio.wait_for(waiter.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Conmon retry {attempt} timed out for {server_name}")
                continue
            finally:
                self.application.notifications.unregister_waiter(waiter)

            if device.dante_model_id:
                logger.debug(f"Conmon dante_model populated for {server_name}: {device.dante_model_id}")
                return

        logger.debug(f"Conmon dante_model still missing for {server_name} after retries")
