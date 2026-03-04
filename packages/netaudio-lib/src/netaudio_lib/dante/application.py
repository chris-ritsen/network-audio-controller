import asyncio
import logging
import time

from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

from netaudio_lib.dante.const import (
    BLUETOOTH_MODEL_IDS,
    DEVICE_SETTINGS_PORT,
    SERVICE_ARC,
    SERVICE_CMC,
    SERVICES,
)
from netaudio_lib.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio_lib.dante.services.arc import DanteARCService
from netaudio_lib.dante.services.cmc import DanteCMCService
from netaudio_lib.dante.services.notification import (
    DanteNotificationService,
    NOTIFICATION_NAMES,
)
from netaudio_lib.dante.services.settings import DanteSettingsService

logger = logging.getLogger("netaudio")


class DanteApplication:
    def __init__(self, packet_store=None):
        self.devices: dict = {}
        self.dispatcher = DanteEventDispatcher()
        self.arc = DanteARCService(packet_store=packet_store)
        self.settings = DanteSettingsService(packet_store=packet_store)
        from netaudio_lib.common.app_config import settings as app_settings

        self.cmc = DanteCMCService(packet_store=packet_store, interface_name=app_settings.interface)
        self.notifications = DanteNotificationService(
            dispatcher=self.dispatcher,
            device_lookup=self._device_by_ip,
            packet_store=packet_store,
        )
        self._browser = None
        self._started = False
        self._notification_handlers: dict[int, list] = {}

    def on_notification(self, notification_id: int, callback) -> None:
        if notification_id not in self._notification_handlers:
            self._notification_handlers[notification_id] = []
        self._notification_handlers[notification_id].append(callback)

    async def _dispatch_notification(self, event) -> None:
        notification_id = event.data.get("notification_id")
        if notification_id is None:
            return

        handlers = self._notification_handlers.get(notification_id)
        if handlers:
            for handler in handlers:
                try:
                    await handler(event)
                except Exception:
                    notification_name = NOTIFICATION_NAMES.get(notification_id, f"0x{notification_id:04X}")
                    logger.exception(f"Error in notification handler for {notification_name}")
        else:
            notification_name = event.data.get("notification_name", f"0x{notification_id:04X}")
            logger.debug(f"Unhandled notification: {notification_name} from {event.server_name}")

    async def startup(self) -> None:
        if self._started:
            return

        self.dispatcher.on(EventType.NOTIFICATION_RECEIVED, self._dispatch_notification)
        await self.dispatcher.start()
        await self.notifications.start()
        await self.arc.start()
        await self.settings.start()
        await self.cmc.start()
        self._started = True
        logger.info("DanteApplication started")

    async def shutdown(self) -> None:
        if not self._started:
            return

        await self.notifications.stop()
        await self.cmc.stop()
        await self.settings.stop()
        await self.arc.stop()
        await self.dispatcher.stop()

        if self._browser:
            try:
                await self._browser.async_close()
            except Exception:
                pass
            self._browser = None

        self._started = False
        logger.info("DanteApplication shut down")

    async def wait_for_discovery(self, timeout: float = 5.0) -> dict:
        from netaudio_lib.dante.browser import DanteBrowser

        browser = DanteBrowser(mdns_timeout=timeout, app=self)
        self._browser = browser
        devices = await browser.get_devices()

        if devices:
            self.devices.update(devices)

        return self.devices

    async def discover_and_populate(self, timeout: float = 5.0) -> dict:
        from netaudio_lib.dante.browser import DanteBrowser

        discovery_time = min(timeout * 0.4, 2.0)
        populate_time = timeout - discovery_time

        browser = DanteBrowser(mdns_timeout=0, app=self)
        self._browser = browser

        browser.aio_zc = AsyncZeroconf(**browser.get_zeroconf_kwargs())
        browser.aio_browser = AsyncServiceBrowser(
            browser.aio_zc.zeroconf,
            SERVICES,
            handlers=[browser.async_on_service_state_change],
        )

        await asyncio.sleep(discovery_time)

        if browser.services:
            await asyncio.gather(*browser.services, return_exceptions=True)

        device_hosts = {}
        for service_future in browser.services:
            service = service_future.result() if not service_future.cancelled() else None
            if not service or "server_name" not in service:
                continue
            server_name = service["server_name"]
            if server_name not in device_hosts:
                device_hosts[server_name] = {}
            device_hosts[server_name][service["name"]] = service

        for hostname, device_services in device_hosts.items():
            if hostname in self.devices:
                device = self.devices[hostname]
            else:
                from netaudio_lib.dante.device import DanteDevice

                device = DanteDevice(server_name=hostname, app=self)
                self.register_device(hostname, device)

            device.services = device_services
            for service_name, service in device_services.items():
                if not device.ipv4:
                    device.ipv4 = service["ipv4"]
                service_properties = service.get("properties", {})
                if "id" in service_properties and service["type"] == SERVICE_CMC:
                    device.mac_address = service_properties["id"]
                if "model" in service_properties:
                    device.model_id = service_properties["model"]
                if "mf" in service_properties and not device.manufacturer:
                    device.manufacturer = service_properties["mf"]
                if "server_vers" in service_properties and service["type"] == SERVICE_CMC:
                    device.software_version = service_properties["server_vers"]
                if "router_vers" in service_properties:
                    device.firmware_version = service_properties["router_vers"]
                if "rate" in service_properties:
                    device.sample_rate = int(service_properties["rate"])
                if "latency_ns" in service_properties:
                    device.latency = int(service_properties["latency_ns"])

        await browser.aio_browser.async_cancel()
        await browser.aio_zc.async_close()
        self._browser = None

        device_ips = [str(device.ipv4) for device in self.devices.values() if device.ipv4]
        if device_ips:
            await self.cmc.register_all(device_ips)

        populate_tasks = []
        for device in self.devices.values():
            arc_port = self.get_arc_port(device)
            if arc_port:
                populate_tasks.append(self._populate_device_controls(device, arc_port))

        if populate_tasks:
            done, pending = await asyncio.wait(
                [asyncio.create_task(task) for task in populate_tasks],
                timeout=populate_time,
            )
            for task in pending:
                task.cancel()

        await self._query_settings_fields()

        await self._query_conmon_all()

        return self.devices

    def register_device(self, server_name: str, device) -> None:
        existing = self.devices.get(server_name)

        if existing is not None:
            if not existing.online:
                existing.online = True
                existing.update_last_seen()
                if device.ipv4:
                    existing.ipv4 = device.ipv4
                if device.services:
                    existing.services = device.services

            self.devices[server_name] = existing
            self.notifications.apply_pending_for_device(existing)
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_UPDATED,
                    device_name=existing.name,
                    server_name=server_name,
                )
            )
        else:
            device._app = self
            device.update_last_seen()
            self.devices[server_name] = device
            self.notifications.apply_pending_for_device(device)
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_DISCOVERED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    def unregister_device(self, server_name: str) -> None:
        device = self.devices.pop(server_name, None)
        if device:
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_REMOVED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    def mark_device_offline(self, server_name: str) -> None:
        device = self.devices.get(server_name)
        if device and device.online:
            device.online = False
            self.dispatcher.emit_nowait(
                DanteEvent(
                    type=EventType.DEVICE_REMOVED,
                    device_name=device.name,
                    server_name=server_name,
                )
            )

    def get_arc_port(self, device) -> int | None:
        if not device.services:
            return None

        for service_data in device.services.values():
            if service_data.get("type") == SERVICE_ARC:
                return service_data.get("port")

        return None

    async def populate_controls(self, devices: dict | None = None) -> None:
        if devices is None:
            devices = self.devices

        tasks = []
        for device in devices.values():
            arc_port = self.get_arc_port(device)
            if arc_port:
                tasks.append(self._populate_device_controls(device, arc_port))
            else:
                logger.debug(f"No ARC port for {device.server_name}, skipping controls")

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _populate_device_controls(self, device, arc_port: int) -> None:
        try:
            await self.arc.get_controls(device, arc_port)
        except Exception as exception:
            device.error = exception
            logger.debug(f"Error populating controls for {device.server_name}: {exception}")

    async def _query_settings_fields(self) -> None:
        host_mac = self.cmc._host_mac
        tasks = []

        for device in self.devices.values():
            if not device.ipv4:
                continue

            if device.model_id in BLUETOOTH_MODEL_IDS:
                tasks.append(device.get_bluetooth_status(host_mac=host_mac))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _query_conmon_all(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout

        incomplete_devices = []

        for device in self.devices.values():
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                logger.debug("Conmon query timeout reached, skipping remaining devices")
                break

            device_ip = str(device.ipv4) if device.ipv4 else None

            if not device_ip or not device.mac_address:
                continue

            waiter = self.notifications.register_conmon_waiter(device_ip)

            try:
                self._send_conmon_query_for_device(device, "make_model")
                self._send_conmon_query_for_device(device, "dante_model")

                per_device_timeout = min(remaining, 1.0)

                try:
                    await asyncio.wait_for(waiter.wait(), timeout=per_device_timeout)
                    logger.debug(f"Conmon responses received for {device.server_name}")
                except asyncio.TimeoutError:
                    logger.debug(f"Conmon query partial/timeout for {device.server_name}")
                    received = self.notifications._conmon_received.get(device_ip, set())

                    if len(received) < 2:
                        incomplete_devices.append(device)
            finally:
                self.notifications.unregister_conmon_waiter(device_ip)

        for retry in range(2):
            if not incomplete_devices:
                break

            remaining = deadline - time.monotonic()

            if remaining <= 0:
                break

            still_incomplete = []

            for device in incomplete_devices:
                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    break

                device_ip = str(device.ipv4)
                needs_make_model = not device.dante_model
                needs_dante_model = not device.dante_model_id
                expected_count = int(needs_make_model) + int(needs_dante_model)

                if expected_count == 0:
                    continue

                waiter = self.notifications.register_conmon_waiter(device_ip, expected_count=expected_count)

                try:
                    if needs_make_model:
                        self._send_conmon_query_for_device(device, "make_model")

                    if needs_dante_model:
                        self._send_conmon_query_for_device(device, "dante_model")

                    per_device_timeout = min(remaining, 2.0)

                    try:
                        await asyncio.wait_for(waiter.wait(), timeout=per_device_timeout)
                        logger.debug(f"Conmon retry {retry + 1} succeeded for {device.server_name}")
                    except asyncio.TimeoutError:
                        logger.debug(f"Conmon retry {retry + 1} timeout for {device.server_name}")

                        if not device.dante_model_id:
                            still_incomplete.append(device)
                finally:
                    self.notifications.unregister_conmon_waiter(device_ip)

            incomplete_devices = still_incomplete

    def _send_conmon_query_for_device(self, device, opcode: str = "make_model") -> None:
        from netaudio_lib.dante.device_commands import DanteDeviceCommands

        if not device.ipv4 or not device.mac_address:
            return

        mac_hex = device.mac_address.replace(":", "").replace("-", "")

        if len(mac_hex) == 16 and mac_hex[6:10].upper() == "FFFE":
            mac_hex = mac_hex[:6] + mac_hex[10:]
        elif len(mac_hex) == 16 and mac_hex.upper().endswith("0000"):
            mac_hex = mac_hex[:12]

        try:
            commands = DanteDeviceCommands()

            if opcode == "make_model":
                packet = commands.command_make_model(mac_hex)
            elif opcode == "dante_model":
                packet = commands.command_dante_model(mac_hex)
            else:
                return

            self.settings.send(packet, str(device.ipv4), DEVICE_SETTINGS_PORT)
        except Exception:
            logger.debug(f"Failed to send conmon {opcode} to {device.server_name}")

    def _device_by_ip(self, ip_str: str):
        for device in self.devices.values():
            if device.ipv4 and str(device.ipv4) == ip_str:
                return device
        return None
