from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket

from netaudio import core
from netaudio.ddm.controller import ControllerAPIClient, DAPISession, normalize_device_id

logger = logging.getLogger("netaudio")


class ManagedSignalReceiver:
    def __init__(self, application, metering):
        self.application = application
        self.metering = metering
        self.tasks = {}
        self.targets = {}

    def reconcile(self):
        targets = {}
        for device in self.application.devices.values():
            if not device.online or not device.requires_managed_control:
                continue
            key = (device.ddm_server_profile, device.ddm_context, device.ddm_domain_id)
            try:
                identifier = normalize_device_id(device.ddm_device_id)
            except ValueError:
                continue
            targets.setdefault(key, {})[identifier] = device.server_name
        self.targets = targets
        for key in self.tasks.keys() - targets.keys():
            self.tasks.pop(key).cancel()
        for key, devices in targets.items():
            if key not in self.tasks:
                device = self.application.devices[next(iter(devices.values()))]
                self.tasks[key] = asyncio.create_task(self._run(key, device))

    async def stop(self):
        self.targets = {}
        tasks = list(self.tasks.values())
        self.tasks.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self, key, device):
        writer = None
        notification = None
        try:
            transport = self.application.managed_transport(device)
            api = ControllerAPIClient(transport.server, timeout=5)
            endpoints = await asyncio.to_thread(api.endpoints)
            credential = transport._credential()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(api.server, endpoints.service_port, ssl=api.ssl_context), 5
            )
            local_address = writer.get_extra_info("sockname")[0]
            notification = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            notification.bind((local_address, 0))
            writer.write(core.build_dapi_session_open())
            writer.write(core.build_dapi_authentication(credential))
            await writer.drain()
            domain = None
            while domain is None:
                frame = await self._frame(reader, writer)
                description = DAPISession._parse("dapi_session_description", frame)
                if description is not None:
                    domain = description["domain_id"]
                    if domain != key[2].lower():
                        raise ValueError("Managed signal session selected a different domain")
            domain_bytes = bytes.fromhex(domain)
            for identifier in range(2, 6):
                writer.write(core.build_dapi_domain_subscription(domain_bytes, identifier))
            writer.write(
                core.build_dapi_inventory_initialization(
                    domain_bytes,
                    core.next_message_id(),
                    notification.getsockname()[1],
                    ipaddress.IPv4Address(local_address).packed,
                )
            )
            writer.write(core.build_dapi_device_inventory_subscription(domain_bytes))
            await writer.drain()
            while key in self.targets:
                frame = await self._frame(reader, writer)
                publication = DAPISession._parse("dapi_signal_presence_publication", frame)
                if publication is not None:
                    self.accept(key, publication, writer.get_extra_info("peername")[:2])
        except asyncio.CancelledError:
            raise
        except (OSError, ValueError, RuntimeError, asyncio.IncompleteReadError, TimeoutError) as error:
            logger.warning("Managed signal updates disconnected: %s", error)
        finally:
            if notification is not None:
                notification.close()
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError as error:
                    logger.debug("Managed signal connection close failed: %s", error)
            if self.tasks.get(key) is asyncio.current_task():
                self.tasks.pop(key)

    @staticmethod
    async def _frame(reader, writer):
        header = await asyncio.wait_for(reader.readexactly(12), 10)
        description = core.parse_response("dapi_frame_header", header)
        if not description["server_to_client"]:
            raise ValueError("Invalid managed signal frame direction")
        frame = header + await asyncio.wait_for(reader.readexactly(description["payload_length"]), 10)
        if DAPISession._parse("dapi_service_announcement", frame) is not None:
            writer.write(core.build_dapi_service_acknowledgement(frame))
            await writer.drain()
        return frame

    def accept(self, key, publication, source):
        server_name = self.targets.get(key, {}).get(publication.get("device_id"))
        device = self.application.devices.get(server_name)
        if device is None or not device.online:
            return
        if (device.ddm_server_profile, device.ddm_context, device.ddm_domain_id) != key:
            return
        if normalize_device_id(device.ddm_device_id) != publication.get("device_id"):
            return
        for record in publication["records"]:
            self.metering.record_signal_presence(record, source, server_name=server_name)
