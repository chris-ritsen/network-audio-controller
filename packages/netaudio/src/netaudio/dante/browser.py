from __future__ import annotations

import asyncio
import logging
import time
from queue import Queue
from typing import Any, TypedDict

from zeroconf import (
    DNSService,
    DNSText,
    IPVersion,
    ServiceInfo,
    ServiceStateChange,
    Zeroconf,
)
from zeroconf.asyncio import (
    AsyncServiceBrowser,
    AsyncServiceInfo,
    AsyncZeroconf,
)

from netaudio.common.app_config import settings as app_settings
from netaudio.dante.const import SERVICE_CMC, SERVICES
from netaudio.dante.device import DanteDevice
from netaudio.dante.latency import nanoseconds_to_milliseconds

logger = logging.getLogger("netaudio")


class ZeroconfKwargs(TypedDict, total=False):
    ip_version: IPVersion
    interfaces: list[str]


class DanteBrowser:
    def __init__(self, mdns_timeout: float, queue: Queue | None = None, app=None) -> None:
        self._devices: dict = {}
        self.services: list[asyncio.Future] = []
        self._state_change_tasks: set[asyncio.Task] = set()
        self.queue: Queue | None = queue
        self._mdns_timeout: float = mdns_timeout
        self.aio_browser: AsyncServiceBrowser | None = None
        self.aio_zc: AsyncZeroconf | None = None
        self._app = app

    @property
    def mdns_timeout(self):
        return self._mdns_timeout

    @mdns_timeout.setter
    def mdns_timeout(self, mdns_timeout):
        self._mdns_timeout = mdns_timeout

    @property
    def devices(self):
        return self._devices

    @devices.setter
    def devices(self, devices):
        self._devices = devices

    def sync_parse_state_change(self, zeroconf, service_type, name, state_change):
        info = ServiceInfo(service_type, name)

        if state_change != ServiceStateChange.Removed:
            info_success = info.request(zeroconf, 3000)

            if not info_success:
                return

        service_properties = {}

        for key, value in info.properties.items():
            key = key.decode("utf-8")

            if not key:
                continue

            if isinstance(value, bytes):
                value = value.decode("utf-8")

            service_properties[key] = value

        records = zeroconf.cache.entries_with_name(name)
        addresses = info.parsed_addresses()

        if not addresses:
            return

        for record in records:
            if isinstance(record, DNSService):
                ipv4 = addresses[0]

                message = {
                    "service": {
                        "ipv4": ipv4,
                        "name": name,
                        "port": info.port,
                        "properties": service_properties,
                        "server_name": record.server,
                        "type": service_type,
                    },
                    "state_change": {
                        "name": state_change.name,
                        "value": state_change.value,
                    },
                }

                queue = self.queue
                if queue is not None:
                    queue.put(message)
            elif isinstance(record, DNSText):
                pass

    async def async_parse_state_change(self, zeroconf, service_type, name, state_change):
        info = AsyncServiceInfo(service_type, name)

        if state_change != ServiceStateChange.Removed:
            info_success = await info.async_request(zeroconf, 3000)

            if not info_success:
                return

        service_properties = {}

        for key, value in info.properties.items():
            key = key.decode("utf-8")

            if not key:
                continue

            if isinstance(value, bytes):
                value = value.decode("utf-8")

            service_properties[key] = value

        records = zeroconf.cache.entries_with_name(name)
        addresses = info.parsed_addresses()

        if not addresses:
            return

        for record in records:
            if isinstance(record, DNSService):
                ipv4 = addresses[0]

                message = {
                    "service": {
                        "ipv4": ipv4,
                        "name": name,
                        "port": info.port,
                        "properties": service_properties,
                        "server_name": record.server,
                        "type": service_type,
                    },
                    "state_change": {
                        "name": state_change.name,
                        "value": state_change.value,
                    },
                }

                queue = self.queue
                if queue is not None:
                    queue.put(message)
            elif isinstance(record, DNSText):
                pass

    def async_on_service_state_change(
        self,
        zeroconf: Zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ) -> None:
        if service_type == "_netaudio-chan._udp.local.":
            return

        if self.queue is not None:
            loop = asyncio.get_running_loop()
            state_task = loop.create_task(
                self.async_parse_state_change(zeroconf, service_type, name, state_change),
                name=f"dante-mdns-state:{name}",
            )
            self._state_change_tasks.add(state_task)
            state_task.add_done_callback(self._state_change_done)

        self.services.append(asyncio.ensure_future(self.async_parse_netaudio_service(zeroconf, service_type, name)))

    def _state_change_done(self, task: asyncio.Task) -> None:
        self._state_change_tasks.discard(task)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            logger.error("mDNS state-change task failed", exc_info=exception)

    def sync_on_service_state_change(
        self,
        zeroconf: Zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ) -> None:
        if service_type == "_netaudio-chan._udp.local.":
            return

        self.sync_parse_state_change(zeroconf, service_type, name, state_change)

    def get_zeroconf_kwargs(self) -> ZeroconfKwargs:
        kwargs: ZeroconfKwargs = {"ip_version": IPVersion.V4Only}

        if app_settings.interface_ip:
            logger.info("Using interface IP %s for Zeroconf", app_settings.interface_ip)
            kwargs["interfaces"] = [app_settings.interface_ip]

            if "127.0.0.1" not in app_settings.interface_ip:
                logger.info("Configuring Zeroconf with interface %s", app_settings.interface_ip)
            else:
                logger.warning(
                    "Using loopback interface %s for Zeroconf may not discover network devices",
                    app_settings.interface_ip,
                )

        return kwargs

    async def async_run(self) -> None:
        self.aio_zc = AsyncZeroconf(**self.get_zeroconf_kwargs())
        services = SERVICES

        self.aio_browser = AsyncServiceBrowser(
            self.aio_zc.zeroconf,
            services,
            handlers=[self.async_on_service_state_change],
        )

        if self.mdns_timeout > 0:
            logger.debug(f"mDNS discovery: waiting {self.mdns_timeout}s...")
            await asyncio.sleep(self.mdns_timeout)
            logger.debug("mDNS discovery: timeout reached, closing browser")
            await self._async_close(cancel_service_tasks=False)

    async def async_close(self) -> None:
        await self._async_close(cancel_service_tasks=True)

    async def _async_close(self, cancel_service_tasks: bool) -> None:
        browser = self.aio_browser
        zeroconf = self.aio_zc
        state_change_tasks = list(self._state_change_tasks)
        service_tasks = list(self.services) if cancel_service_tasks else []
        self.aio_browser = None
        self.aio_zc = None
        self._state_change_tasks.clear()
        if cancel_service_tasks:
            self.services = []
        if browser is not None:
            await browser.async_cancel()
        for task in (*state_change_tasks, *service_tasks):
            task.cancel()
        if state_change_tasks or service_tasks:
            await asyncio.gather(*state_change_tasks, *service_tasks, return_exceptions=True)
        if zeroconf is not None:
            await zeroconf.async_close()

    async def get_devices(self) -> dict:
        start_time = time.monotonic()
        try:
            await self.get_services()
            logger.debug(f"mDNS: {len(self.services)} services ({time.monotonic() - start_time:.2f}s)")

            gather_start = time.monotonic()
            await asyncio.gather(*self.services)
            logger.debug(f"Service info gathered ({time.monotonic() - gather_start:.2f}s)")
        finally:
            try:
                self._assemble_completed_services()
            finally:
                await self.async_close()

        return self.devices

    def _assemble_completed_services(self) -> None:
        device_hosts = {}

        for service_task in self.services:
            if not service_task.done() or service_task.cancelled():
                continue
            exception = service_task.exception()
            if exception is not None:
                logger.error("Failed to resolve mDNS service", exc_info=exception)
                continue
            service = service_task.result()
            server_name = None

            if not service:
                continue

            if "server_name" in service:
                server_name = service["server_name"]

            if not server_name in device_hosts:
                device_hosts[server_name] = {}

            device_hosts[server_name][service["name"]] = service

        for hostname, device_services in device_hosts.items():
            try:
                if self._app is not None:
                    device = self._app._apply_discovered_services(hostname, device_services)
                    self.devices[hostname] = device
                    continue

                device = DanteDevice(
                    server_name=hostname,
                    dump_payloads=app_settings.dump_payloads,
                    debug=app_settings.debug,
                    app=self._app,
                )
                device.services = device_services

                for service_name, service in device_services.items():
                    device.services[service_name] = service

                    service_properties = service["properties"]

                    if not device.ipv4:
                        device.ipv4 = service["ipv4"]

                    if "id" in service_properties and service["type"] == SERVICE_CMC:
                        device.mac_address = service_properties["id"]

                    if "model" in service_properties:
                        device.model_id = service_properties["model"]

                    if "mf" in service_properties:
                        device.manufacturer_mdns = service_properties["mf"]
                        if not device.manufacturer:
                            device.manufacturer = service_properties["mf"]

                    if "server_vers" in service_properties and service["type"] == SERVICE_CMC:
                        device.software_version = service_properties["server_vers"]

                    if "router_vers" in service_properties:
                        device.firmware_version = service_properties["router_vers"]

                    if "rate" in service_properties:
                        device.sample_rate = int(service_properties["rate"])

                    if "router_info" in service_properties and service_properties["router_info"] == '"Dante Via"':
                        device.software = "Dante Via"

                    if "latency_ns" in service_properties:
                        device.latency = nanoseconds_to_milliseconds(service_properties["latency_ns"])

                device.services = dict(sorted(device.services.items()))
                self.devices[hostname] = device
            except Exception:
                logger.exception("Failed to assemble discovered Dante device %s", hostname)

    async def get_services(self) -> None:
        try:
            await self.async_run()
        except KeyboardInterrupt:
            await self.async_close()

    async def async_parse_netaudio_service(
        self, zeroconf: Zeroconf, service_type: str, name: str
    ) -> dict[str, Any] | None:
        ipv4 = None
        service_properties = {}
        info = AsyncServiceInfo(service_type, name)
        info_success = await info.async_request(zeroconf, 3000)

        if not info_success:
            return

        host = zeroconf.cache.entries_with_name(name)
        addresses = info.parsed_addresses()

        if not addresses:
            return

        ipv4 = addresses[0]

        try:
            for key, value in info.properties.items():
                key = key.decode("utf-8")

                if isinstance(value, bytes):
                    value = value.decode("utf-8")

                service_properties[key] = value

            for record in host:
                if isinstance(record, DNSService):
                    service = {
                        "ipv4": ipv4,
                        "name": name,
                        "port": info.port,
                        "properties": service_properties,
                        "server_name": record.server,
                        "type": info.type,
                    }

                    return service

        except Exception:
            logger.exception("Failed to parse mDNS service %s", name)
