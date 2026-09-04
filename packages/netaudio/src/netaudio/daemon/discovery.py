from __future__ import annotations

import logging
from dataclasses import dataclass

from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceInfo

from netaudio.daemon.systemd import notify_systemd
from netaudio.dante.const import MEDIA_SERVICE_TYPES, SERVICE_ARC, SERVICE_CMC
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.latency import nanoseconds_to_milliseconds


logger = logging.getLogger("netaudio")


@dataclass(frozen=True)
class _DiscoveredService:
    address: str
    instance_name: str
    port: int
    properties: dict
    server_name: str
    service_type: str

    @property
    def device_key(self) -> str:
        suffix = f".{self.service_type}"
        logical_name = (
            self.instance_name[: -len(suffix)]
            if self.instance_name.casefold().endswith(suffix.casefold())
            else self.instance_name.rstrip(".")
        )
        return f"{logical_name}.local."

    def device_data(self):
        return {
            "ipv4": self.address,
            "name": self.instance_name,
            "port": self.port,
            "properties": self.properties,
            "server_name": self.server_name,
            "type": self.service_type,
        }


class DanteDiscoveryMixin:
    def on_service_state_change(self, zeroconf, service_type, name, state_change):
        logger.debug(f"mDNS event: {state_change.name} - {service_type} - {name}")

        self._spawn_background(
            self.handle_service_change(zeroconf, service_type, name, state_change),
            name=f"mdns-change:{name}",
        )

    async def handle_service_change(self, zeroconf, service_type, name, state_change):
        try:
            info = AsyncServiceInfo(service_type, name)

            if state_change == ServiceStateChange.Removed:
                self._handle_removed_service(name, service_type)
                return

            service = await self._resolve_service(zeroconf, info, service_type, name)
            if service is None:
                return
            if service.service_type in MEDIA_SERVICE_TYPES:
                self._attach_media_service(service)
                return

            device, is_new = await self._device_for_service(service)
            device_changed = self._attach_service(device, service)
            await self._apply_service_properties(device, service)

            await self._publish_device_to_redis(device)

            if await self._refresh_arc_device(device, service.device_key, is_new):
                device_changed = True

            if device_changed:
                self._emit_device_updated(device, service.device_key)

        except Exception:
            logger.exception(f"Service change error for {name}")

    def _handle_removed_service(self, name, service_type):
        media_services = getattr(self.application, "media_services", None)
        if isinstance(media_services, dict) and media_services.pop(name, None) is not None:
            return
        for server_name in list(self.devices.keys()):
            device = self.devices[server_name]
            if name in device.services:
                logger.info(f"Device offline candidate (mDNS removed): {server_name}")
                self.mark_device_offline(server_name)
                return
        suffix = f".{service_type}"
        if name.casefold().endswith(suffix.casefold()):
            device_key = f"{name[: -len(suffix)]}.local."
            if device_key in self.devices:
                self.mark_device_offline(device_key)

    def _attach_media_service(self, service):
        if not hasattr(self.application, "media_services"):
            self.application.media_services = {}
        self.application.media_services[service.instance_name] = service.device_data()
        attach = getattr(self.application, "_attach_media_services", None)
        if attach is not None:
            for device in self.devices.values():
                attach(device)

    async def _resolve_service(self, zeroconf, info, service_type, name):
        if not await info.async_request(zeroconf, 3000):
            return None

        addresses = info.parsed_addresses()
        if not addresses:
            return None

        server_name = self._service_server_name(zeroconf, name)
        if not server_name:
            return None

        return _DiscoveredService(
            address=addresses[0],
            instance_name=name,
            port=info.port,
            properties=self._decode_service_properties(info.properties),
            server_name=server_name,
            service_type=service_type,
        )

    @staticmethod
    def _service_server_name(zeroconf, name):
        for record in zeroconf.cache.entries_with_name(name):
            if hasattr(record, "server"):
                return record.server
        return None

    @staticmethod
    def _decode_service_properties(properties):
        decoded = {}
        for key, value in properties.items():
            key = key.decode("utf-8") if isinstance(key, bytes) else key
            if not key:
                continue
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            decoded[key] = value
        return decoded

    async def _device_for_service(self, service):
        device_key = service.device_key
        self.clear_offline_candidate(device_key)
        existing = self.devices.get(device_key)
        was_offline = existing is not None and not existing.online
        is_new = existing is None

        if is_new or was_offline:
            new_device = DanteDevice(server_name=device_key)
            new_device.ipv4 = service.address
            self.application.register_device(device_key, new_device)
            state = "discovered" if is_new else "back online"
            logger.info(f"Device {state}: {device_key}")
            online = sum(1 for device in self.devices.values() if device.online)
            notify_systemd(f"STATUS={online} device(s) online")
            if was_offline and self.metering:
                self.metering.reactivate_device(device_key)

        if service.address and service.service_type == SERVICE_CMC:
            await self.application.cmc.register_device(service.address)

        device = self.devices[device_key]
        device.update_last_seen()
        return device, is_new

    @staticmethod
    def _attach_service(device, service):
        old_ip = str(device.ipv4) if device.ipv4 else None
        device_changed = service.service_type == SERVICE_ARC and bool(old_ip and old_ip != service.address)
        if device_changed:
            logger.info(f"Device {service.server_name} IP changed: {old_ip} -> {service.address}")

        if service.service_type == SERVICE_ARC or device.ipv4 is None:
            device.ipv4 = service.address
        if not device.services:
            device.services = {}
        device.services[service.instance_name] = service.device_data()
        return device_changed

    async def _apply_service_properties(self, device, service):
        properties = service.properties
        await self._apply_cmc_identity(device, service)

        if "model" in properties:
            device.model_id = properties["model"]
        if "mf" in properties:
            device.manufacturer_mdns = properties["mf"]
            if not device.manufacturer:
                device.manufacturer = properties["mf"]
        if "server_vers" in properties and service.service_type == SERVICE_CMC:
            device.software_version = properties["server_vers"]
        if "router_vers" in properties:
            device.firmware_version = properties["router_vers"]
        if "rate" in properties:
            device.sample_rate = int(properties["rate"])
        if "latency_ns" in properties:
            device.latency = nanoseconds_to_milliseconds(properties["latency_ns"])

    async def _apply_cmc_identity(self, device, service):
        if service.service_type != SERVICE_CMC or "id" not in service.properties:
            return

        device.mac_address = service.properties["id"]
        if not (device.ipv4 and device.mac_address):
            return

        if not device.dante_model:
            await self.application._send_conmon_query_for_device(device, self.application.send_make_model_request)
        if not device.dante_model_id:
            await self.application._send_conmon_query_for_device(device, self.application.send_dante_model_request)
            self._spawn_background(
                self.state.retry_conmon_query(service.device_key),
                name=f"retry-conmon:{service.device_key}",
            )

    async def _refresh_arc_device(self, device, server_name, is_new):
        if not self.application.get_arc_port(device):
            return False

        device_changed = False
        if not is_new:
            new_name = await device.fetch_device_name()
            if new_name and new_name != device.name:
                logger.info(f"Device name changed for {server_name}: {device.name!r} -> {new_name!r}")
                device.name = new_name
                device_changed = True

        if not device.tx_channels and not device.rx_channels:
            self._spawn_background(
                self.state.fetch_device_controls(server_name),
                name=f"delayed-controls:{server_name}",
            )
        return device_changed

    def _emit_device_updated(self, device, server_name):
        self.application.dispatcher.emit_nowait(
            DanteEvent(
                type=EventType.DEVICE_UPDATED,
                device_name=device.name,
                server_name=server_name,
            )
        )
