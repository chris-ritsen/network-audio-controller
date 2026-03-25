import asyncio
import json
import logging
import os
import sys
import time
import traceback
from json import JSONEncoder
from queue import Queue

from zeroconf import (
    DNSService,
    DNSText,
    IPVersion,
    ServiceBrowser,
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

logger = logging.getLogger("netaudio")


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class DanteBrowser:
    def __init__(self, mdns_timeout, queue=None, app=None) -> None:
        self._devices = {}
        self.services = []
        self.queue = queue
        self._mdns_timeout: float = mdns_timeout
        self.aio_browser: AsyncServiceBrowser = None
        self.aio_zc: AsyncZeroconf = None
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

                self.queue.put(message)
            elif isinstance(record, DNSText):
                pass

    async def async_parse_state_change(
        self, zeroconf, service_type, name, state_change
    ):
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

                json_message = json.dumps(message, indent=2)
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

        loop = asyncio.get_running_loop()
        loop.create_task(
            self.async_parse_state_change(zeroconf, service_type, name, state_change)
        )

        self.services.append(
            asyncio.ensure_future(
                self.async_parse_netaudio_service(zeroconf, service_type, name)
            )
        )

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

    def get_zeroconf_kwargs(self):
        kwargs = {"ip_version": IPVersion.V4Only}

        if app_settings.interface_ip:
            print(
                f"Using interface IP {app_settings.interface_ip} for Zeroconf",
                file=sys.stderr,
            )
            kwargs["interfaces"] = [app_settings.interface_ip]

            if "127.0.0.1" not in app_settings.interface_ip:
                print(
                    f"Configuring Zeroconf with interface {app_settings.interface_ip}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"Warning: Using loopback interface {app_settings.interface_ip} for Zeroconf may not discover network devices",
                    file=sys.stderr,
                )

        return kwargs

    def sync_run(self):
        zc = Zeroconf(**self.get_zeroconf_kwargs())
        services = SERVICES

        browser = ServiceBrowser(
            zc,
            services,
            handlers=[self.sync_on_service_state_change],
        )

        browser.run()

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
            await self.async_close()

    async def async_close(self) -> None:
        assert self.aio_zc is not None
        assert self.aio_browser is not None
        await self.aio_browser.async_cancel()
        await self.aio_zc.async_close()

    async def get_devices(self) -> None:
        start_time = time.monotonic()
        await self.get_services()
        logger.debug(
            f"mDNS: {len(self.services)} services ({time.monotonic() - start_time:.2f}s)"
        )

        gather_start = time.monotonic()
        await asyncio.gather(*self.services)
        logger.debug(
            f"Service info gathered ({time.monotonic() - gather_start:.2f}s)"
        )

        device_hosts = {}

        for service in self.services:
            service = service.result()
            server_name = None

            if not service:
                continue

            if "server_name" in service:
                server_name = service["server_name"]

            if not server_name in device_hosts:
                device_hosts[server_name] = {}

            device_hosts[server_name][service["name"]] = service

        for hostname, device_services in device_hosts.items():
            device = DanteDevice(
                server_name=hostname,
                dump_payloads=app_settings.dump_payloads,
                debug=app_settings.debug,
                app=self._app,
            )

            try:
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

                    if "rate" in service_properties:
                        device.sample_rate = int(service_properties["rate"])

                    if (
                        "router_info" in service_properties
                        and service_properties["router_info"] == '"Dante Via"'
                    ):
                        device.software = "Dante Via"

                    if "latency_ns" in service_properties:
                        device.latency = int(service_properties["latency_ns"])

                device.services = dict(sorted(device.services.items()))
            except Exception:
                traceback.print_exc()

            self.devices[hostname] = device

            if self._app is not None:
                self._app.register_device(hostname, device)

        return self.devices

    async def get_services(self) -> None:
        try:
            await self.async_run()
        except KeyboardInterrupt:
            await self.async_close()

    async def async_parse_netaudio_service(
        self, zeroconf: Zeroconf, service_type: str, name: str
    ) -> None:
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
            traceback.print_exc()
