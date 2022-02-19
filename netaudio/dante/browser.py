import asyncio
import logging
import traceback

from zeroconf import DNSService
from zeroconf import IPVersion, ServiceStateChange, Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from netaudio.dante.device import DanteDevice

from netaudio.dante.const import SERVICE_CMC, SERVICES

logger = logging.getLogger("netaudio")


class DanteBrowser:
    def __init__(self, mdns_timeout: float) -> None:
        self._devices = {}
        self.services = []
        self._mdns_timeout: float = mdns_timeout
        self.aio_browser: AsyncServiceBrowser = None
        self.aio_zc: AsyncZeroconf = None

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

    def async_on_service_state_change(
        self,
        zeroconf: Zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ) -> None:
        if state_change is not ServiceStateChange.Added:
            return

        self.services.append(
            asyncio.ensure_future(
                self.async_parse_netaudio_service(zeroconf, service_type, name)
            )
        )

    async def async_run(self) -> None:
        self.aio_zc = AsyncZeroconf(ip_version=IPVersion.V4Only)
        services = SERVICES

        self.aio_browser = AsyncServiceBrowser(
            self.aio_zc.zeroconf,
            services,
            handlers=[self.async_on_service_state_change],
        )

        await asyncio.sleep(self.mdns_timeout)
        await self.async_close()

    async def async_close(self) -> None:
        assert self.aio_zc is not None
        assert self.aio_browser is not None
        await self.aio_browser.async_cancel()
        await self.aio_zc.async_close()

    async def get_devices(self) -> None:
        await self.get_services()
        await asyncio.gather(*self.services)

        device_hosts = {}

        for service in self.services:
            service = service.result()

            if not service:
                logger.warning("timed out getting mDNS service")
                continue

            if "server_name" in service:
                server_name = service["server_name"]

            if not server_name in device_hosts:
                device_hosts[server_name] = {}

            device_hosts[server_name][service["name"]] = service

        logger.debug(f"Found {len(device_hosts)} device host(s)")

        for hostname, device_services in device_hosts.items():
            device = DanteDevice(server_name=hostname)

            logger.debug(
                f"Host {hostname} has {len(device_services.keys())} service(s)"
            )

            try:
                for service_name, service in device_services.items():
                    logger.debug(
                        f"  `{service['name']}` on `{service['ipv4']}:{service['port']}`"
                    )
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

                    logger.debug(f"Initialized Dante device {service_name}\n")
            except Exception as e:
                print(e)
                traceback.print_exc()

            self.devices[hostname] = device

        return self.devices

    async def get_services(self) -> None:
        logger.debug("get_services")

        try:
            await self.async_run()
        except KeyboardInterrupt:
            await self.async_close()

        logger.debug(f"Found {len(self.services)} services")

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
            logger.warning("Couldn't parse mDNS service address")
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

        except Exception as e:
            print(e)
            traceback.print_exc()
