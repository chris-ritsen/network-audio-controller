import asyncio
import logging

from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

from netaudio_lib.dante.const import SERVICE_ARC, SERVICE_CMC, SERVICES
from netaudio_lib.dante.events import DanteEvent, DanteEventDispatcher, EventType
from netaudio_lib.dante.services.arc import DanteARCService
from netaudio_lib.dante.services.cmc import DanteCMCService
from netaudio_lib.dante.services.notification import DanteNotificationService
from netaudio_lib.dante.services.settings import DanteSettingsService

logger = logging.getLogger("netaudio")


class DanteApplication:
    def __init__(self, packet_store=None):
        self.devices: dict = {}
        self.dispatcher = DanteEventDispatcher()
        self.arc = DanteARCService(packet_store=packet_store)
        self.settings = DanteSettingsService(packet_store=packet_store)
        self.cmc = DanteCMCService(packet_store=packet_store)
        self.notifications = DanteNotificationService(
            dispatcher=self.dispatcher,
            device_lookup=self._device_by_ip,
            packet_store=packet_store,
        )
        self._browser = None
        self._started = False

    async def startup(self) -> None:
        if self._started:
            return

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
                if "rate" in service_properties:
                    device.sample_rate = int(service_properties["rate"])
                if "latency_ns" in service_properties:
                    device.latency = int(service_properties["latency_ns"])

        await browser.aio_browser.async_cancel()
        await browser.aio_zc.async_close()
        self._browser = None

        device_ips = [
            str(device.ipv4) for device in self.devices.values() if device.ipv4
        ]
        if device_ips:
            await self.cmc.register_all(device_ips)

        populate_tasks = []
        for device in self.devices.values():
            arc_port = self.get_arc_port(device)
            if arc_port:
                populate_tasks.append(
                    self._populate_device_controls(device, arc_port)
                )

        if populate_tasks:
            done, pending = await asyncio.wait(
                [asyncio.create_task(task) for task in populate_tasks],
                timeout=populate_time,
            )
            for task in pending:
                task.cancel()

        await self._query_settings_fields()

        return self.devices

    def register_device(self, server_name: str, device) -> None:
        is_new = server_name not in self.devices
        self.devices[server_name] = device
        device._app = self

        if is_new:
            self.dispatcher.emit_nowait(DanteEvent(
                type=EventType.DEVICE_DISCOVERED,
                device_name=device.name,
                server_name=server_name,
            ))
        else:
            self.dispatcher.emit_nowait(DanteEvent(
                type=EventType.DEVICE_UPDATED,
                device_name=device.name,
                server_name=server_name,
            ))

    def unregister_device(self, server_name: str) -> None:
        device = self.devices.pop(server_name, None)
        if device:
            self.dispatcher.emit_nowait(DanteEvent(
                type=EventType.DEVICE_REMOVED,
                device_name=device.name,
                server_name=server_name,
            ))

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
        BLUETOOTH_MODEL_IDS = {"DIOBT"}
        host_mac = self.cmc._host_mac

        sent_any = False
        for device in self.devices.values():
            device_ip = str(device.ipv4) if device.ipv4 else None
            if not device_ip:
                continue

            if device.model_id in BLUETOOTH_MODEL_IDS:
                self.settings.request_bluetooth_status(device_ip, host_mac=host_mac)
                sent_any = True

        if sent_any:
            await asyncio.sleep(0.5)

    def _device_by_ip(self, ip_str: str):
        for device in self.devices.values():
            if device.ipv4 and str(device.ipv4) == ip_str:
                return device
        return None
