from __future__ import annotations

import asyncio
import logging

from netaudio.core.binding import NetaudioCoreError
from netaudio.dante.core_transport import CoreTransport

logger = logging.getLogger("netaudio")


class DanteCMCService:
    def __init__(
        self,
        transport: CoreTransport,
        host_media_access_control_address: bytes | None = None,
    ):
        self._transport = transport
        self._registered_devices: set[str] = set()
        self._heartbeat_task: asyncio.Task | None = None
        self._host_media_access_control_address = host_media_access_control_address

    @property
    def host_media_access_control_address(self) -> bytes:
        if self._host_media_access_control_address is None:
            raise RuntimeError("Host MAC depends on the target network path")
        return self._host_media_access_control_address

    def controller_identity(self, device_ip: str) -> tuple[str, bytes]:
        from netaudio import core

        source_address = self._transport.path(device_ip).source_address
        host_mac = self._host_media_access_control_address or core.host_mac_for_ipv4(source_address)
        if host_mac is None:
            raise RuntimeError(f"Could not derive a host MAC for source address {source_address}")
        return source_address, host_mac

    @property
    def registered_devices(self) -> frozenset[str]:
        return frozenset(self._registered_devices)

    @staticmethod
    def _accepted_registration(message_id: int, response: bytes | None) -> dict | None:
        if response is None:
            return None
        from netaudio import core

        try:
            parsed = core.parse_response("cmc_registration", response)
        except core.NetaudioCoreError:
            return None
        return parsed if parsed["sequence"] == message_id and parsed["accepted"] else None

    async def register_device(
        self,
        target,
        host_media_access_control_address: bytes | None = None,
        *,
        control_port: int | None = None,
    ) -> bytes | None:
        """Register with a device object at the CMC port it advertises, or at an address and optional port."""
        from netaudio import core

        message_id = core.next_message_id()
        host_mac = host_media_access_control_address or self._host_media_access_control_address
        specification = {"command": "cmc_register", "message_id": message_id}

        if host_mac is not None:
            specification["host_mac"] = host_mac.hex()

        is_device = hasattr(target, "server_name")
        device_ip = str(target.ipv4) if is_device else str(target)
        registration_key = target.server_name if is_device else device_ip
        if is_device:
            ports = target._service_ports()
        else:
            ports = {} if control_port is None else {"control_port": control_port}

        try:
            response = await self._transport.execute(device_ip, specification, **ports)
        except core.NetaudioCoreError as exception:
            logger.debug(f"CMC registration request failed for {registration_key}: {exception}")
            response = None

        registration = self._accepted_registration(message_id, response)
        if registration is not None:
            self._registered_devices.add(registration_key)
            if is_device and registration.get("settings_port"):
                target.settings_port = registration["settings_port"]
            logger.debug(f"CMC registered with {registration_key}")
            return response

        self._registered_devices.discard(registration_key)
        if response is not None:
            logger.warning(f"CMC registration returned an invalid response from {registration_key}")
        return None

    async def require_registration(
        self,
        target,
        host_media_access_control_address: bytes | None = None,
    ) -> bytes:
        response = await self.register_device(target, host_media_access_control_address)
        if response is None:
            raise RuntimeError(f"CMC registration failed for {getattr(target, 'ipv4', target)}")
        return response

    async def register_all(self, targets: list) -> None:
        tasks = [self.register_device(target) for target in targets]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _heartbeat_loop(self, get_device_ips) -> None:
        while True:
            try:
                await asyncio.sleep(10)
                device_ips = get_device_ips()
                if device_ips:
                    await self.register_all(device_ips)
            except asyncio.CancelledError:
                break
            except (OSError, RuntimeError, NetaudioCoreError) as exception:
                logger.warning(f"CMC heartbeat error: {exception}", exc_info=True)

    async def stop(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        self._registered_devices.clear()

    async def start_metering(
        self,
        device_ip: str,
        device_name: str,
        ipv4,
        mac,
        port: int,
    ) -> None:
        await self._transport.execute(
            str(device_ip),
            {
                "command": "metering_start",
                "device_name": device_name,
                "ipv4": str(ipv4) if ipv4 else "",
                "mac": mac.hex() if isinstance(mac, bytes) else mac,
                "port": port,
            },
        )

    async def stop_metering(
        self,
        device_ip: str,
        device_name: str,
        ipv4,
        mac,
        port: int,
    ) -> None:
        await self._transport.execute(
            str(device_ip),
            {
                "command": "metering_stop",
                "port": port,
                "device_name": device_name,
                "mac": mac.hex() if isinstance(mac, bytes) else mac,
            },
        )
