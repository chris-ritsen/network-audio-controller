from __future__ import annotations

import logging

from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceInfo

from netaudio.daemon.systemd import notify_systemd
from netaudio.dante.const import SERVICE_CMC
from netaudio.dante.device import DanteDevice
from netaudio.dante.events import DanteEvent, EventType
from netaudio.dante.latency import nanoseconds_to_milliseconds


logger = logging.getLogger("netaudio")


class DanteDiscoveryMixin:
    def on_service_state_change(self, zeroconf, service_type, name, state_change):
        if service_type == "_netaudio-chan._udp.local.":
            return

        logger.debug(f"mDNS event: {state_change.name} - {service_type} - {name}")

        self._spawn_background(
            self.handle_service_change(zeroconf, service_type, name, state_change),
            name=f"mdns-change:{name}",
        )

    async def handle_service_change(self, zeroconf, service_type, name, state_change):
        try:
            info = AsyncServiceInfo(service_type, name)

            if state_change == ServiceStateChange.Removed:
                for server_name in list(self.devices.keys()):
                    if name.startswith(server_name.replace(".local.", "")):
                        logger.info(f"Device offline candidate (mDNS removed): {server_name}")
                        self.mark_device_offline(server_name)

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

            self.clear_offline_candidate(server_name)

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
                online = sum(1 for device in self.devices.values() if device.online)
                notify_systemd(f"STATUS={online} device(s) online")
                if addresses[0]:
                    await self.application.cmc.register_device(addresses[0])
                if was_offline and self.metering:
                    self.metering.reactivate_device(server_name)

            device = self.devices[server_name]
            device.update_last_seen()

            old_ip = str(device.ipv4) if device.ipv4 else None
            new_ip = addresses[0]

            device_changed = bool(old_ip and old_ip != new_ip)
            if device_changed:
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
                        self._spawn_background(
                            self.state.retry_conmon_query(server_name),
                            name=f"retry-conmon:{server_name}",
                        )

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
                device.latency = nanoseconds_to_milliseconds(service_properties["latency_ns"])

            await self._publish_device_to_redis(device)

            arc_port = self.application.get_arc_port(device)
            if arc_port:
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

            if device_changed:
                self.application.dispatcher.emit_nowait(
                    DanteEvent(
                        type=EventType.DEVICE_UPDATED,
                        device_name=device.name,
                        server_name=server_name,
                    )
                )

        except Exception:
            logger.exception(f"Service change error for {name}")
