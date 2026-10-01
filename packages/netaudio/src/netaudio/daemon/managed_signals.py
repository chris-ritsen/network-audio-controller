from __future__ import annotations

import asyncio
import logging
import socket

from netaudio import core
from netaudio.core import _requests
from netaudio.ddm.controller import ControllerAPIClient, normalize_device_id

logger = logging.getLogger("netaudio")


class ManagedSignalReceiver:
    def __init__(self, application, metering):
        self.application = application
        self.metering = metering
        self.tasks = {}
        self.targets = {}
        self._failed_targets = set()

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
        self._failed_targets.intersection_update(targets)
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
                asyncio.open_connection(api.server, endpoints["service_port"], ssl=api.ssl_context), 5
            )
            local_address = writer.get_extra_info("sockname")[0]
            peer = writer.get_extra_info("peername")[:2]
            notification = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            notification.bind((local_address, 0))
            request: _requests.ManagedSessionRequest = {
                "action": "begin",
                "state": None,
                "credential": credential,
                "local_ipv4": local_address,
                "notification_port": notification.getsockname()[1],
                "expected_domain_id": key[2],
                "operation": {"kind": "monitor_signals"},
            }

            while key in self.targets:
                result = core.advance_managed_session(request)

                for frame in result["outgoing"]:
                    writer.write(bytes(frame))

                await writer.drain()
                publication = result["signal_presence"]

                if publication is not None:
                    if key in self._failed_targets:
                        logger.info("Managed signal updates recovered for %s", key)
                        self._failed_targets.discard(key)
                    self.accept(key, publication, peer)

                request = {
                    "action": "receive",
                    "state": result["state"],
                    "data": list(await asyncio.wait_for(reader.readexactly(result["receive_bytes"]), 10)),
                }
        except asyncio.CancelledError:
            raise
        except (OSError, ValueError, RuntimeError, asyncio.IncompleteReadError, TimeoutError) as error:
            log = logger.debug if key in self._failed_targets else logger.warning
            log("Managed signal updates disconnected for %s: %s", key, error)
            self._failed_targets.add(key)
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

    def accept(self, key, publication, source):
        server_name = self.targets.get(key, {}).get(publication.get("device_id"))
        device = self.application.devices.get(server_name)
        if device is None or not device.online:
            return
        if (device.ddm_server_profile, device.ddm_context, device.ddm_domain_id) != key:
            return
        if normalize_device_id(device.ddm_device_id) != publication.get("device_id"):
            return
        self.metering.record_signal_presence(publication["records"], source, server_name=server_name)
