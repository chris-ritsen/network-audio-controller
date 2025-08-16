import asyncio
import traceback
import ipaddress

from zeroconf import DNSService, DNSText

from zeroconf import (
    IPVersion,
    ServiceStateChange,
    ServiceBrowser,
    ServiceInfo,
    Zeroconf,
    InterfaceChoice
)

from zeroconf.asyncio import (
    AsyncServiceBrowser,
    AsyncServiceInfo,
    AsyncZeroconf,
)
from typing import List

from netaudio.utils import get_host_by_name
from .const import SERVICE_CMC, SERVICES
from .device import DanteDevice

class DanteBrowser:
    def __init__(self, mdns_timeout) -> None:
        self._mdns_timeout: float = mdns_timeout
        self.aio_browser: AsyncServiceBrowser = None
        self.aio_zc: AsyncZeroconf = None

        self.devices:dict[str, DanteDevice] = {}
        self.services = []

    @property
    def mdns_timeout(self):
        return self._mdns_timeout

    async def async_parse_state_change(
        self, zeroconf:Zeroconf, service_type:str, name:str, state_change:ServiceStateChange
    ):
        info = AsyncServiceInfo(service_type, name)

        if state_change != ServiceStateChange.Removed:
            info_success = await info.async_request(zeroconf, 3000)

            if not info_success:
                return

        service_properties = {}

        for key, value in info.properties.items():
            key = key.decode("utf-8")

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

                # TODO: this block of code doesn't make much sense
                # json_message = dump_json_formatted(message)
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

    async def async_run(self, interfaces:List[str]) -> None:
        self.aio_zc = AsyncZeroconf(
            ip_version=IPVersion.V4Only,
            interfaces=interfaces
        )
        services = SERVICES

        self.aio_browser = AsyncServiceBrowser(
            self.aio_zc.zeroconf,
            services,
            handlers=[self.async_on_service_state_change],
        )

        if self.mdns_timeout > 0:
            await asyncio.sleep(self.mdns_timeout)
            await self.async_close()

    async def async_close(self) -> None:
        assert self.aio_zc is not None
        assert self.aio_browser is not None
        await self.aio_browser.async_cancel()
        await self.aio_zc.async_close()

    async def get_devices(self,
                          filter_name: str | None = None,
                          filter_host: str | None = None,
                          interfaces: List[str] | None = None
                          ) -> dict[str, DanteDevice]:
        try:
            await self.async_run(interfaces if interfaces else InterfaceChoice.All)
        except KeyboardInterrupt:
            await self.async_close()

        await asyncio.gather(*self.services)

        device_hosts = {}

        for service in self.services:
            service = service.result()
            server_name = None

            if not service:
                continue

            if "server_name" in service:
                server_name = service["server_name"]

            if server_name not in device_hosts:
                device_hosts[server_name] = {}

            device_hosts[server_name][service["name"]] = service

        for hostname, device_services in device_hosts.items():
            for service_name, service in device_services.items():
                # TODO: better approach of getting service IP
                device = DanteDevice(
                    hostname=hostname,
                    ipv4=ipaddress.IPv4Address(service["ipv4"])
                )
                continue

            # try:
            #     for service_name, service in device_services.items():
            #         device.services[service_name] = service

            #         service_properties = service["properties"]

            #         if not device.ipv4:
            #             device.ipv4 = service["ipv4"]

            #         if "id" in service_properties and service["type"] == SERVICE_CMC:
            #             device.mac_address = service_properties["id"]

            #         if "model" in service_properties:
            #             device.model_id = service_properties["model"]

            #         if "rate" in service_properties:
            #             device.sample_rate = int(service_properties["rate"])

            #         if (
            #             "router_info" in service_properties
            #             and service_properties["router_info"] == '"Dante Via"'
            #         ):
            #             device.software = "Dante Via"

            #         if "latency_ns" in service_properties:
            #             device.latency = int(service_properties["latency_ns"])

            #     device.services = dict(sorted(device.services.items()))
            # except Exception:
            #     traceback.print_exc()

            self.devices[hostname] = device

        if filter_name:
            return {
                k: v for k, v in self.devices.items() if filter_name in k
            }

        elif filter_host:
            possible_names = set([filter_host, filter_host + ".local.", filter_host + "."])

            # If the 'filter_host' is a DNS hostname & it's detected as such
            if possible_names.intersection(set(self.devices.keys())):
                return dict(
                    filter(
                        lambda d: d[1].server_name in possible_names, self.devices.items()
                    )
                )
            # else if it's an IP Address 
            else:
                ipv4: ipaddress.IPv4Address | None = None

                # Check if if filter_host is an IP Address
                try:
                    ipv4 = ipaddress.ip_address(filter_host)
                except ValueError:
                    pass

                    # Try to get the IPv4 address for the hostname manually
                    try:
                        ipv4 = get_host_by_name(filter_host)
                    except TimeoutError:
                        pass

                return dict(filter(lambda d: d[1].ipv4 == ipv4, self.devices.items()))

        else:
            return self.devices

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
