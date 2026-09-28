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
    def _registration_response_is_successful(message_id: int, response: bytes | None) -> bool:
        if response is None:
            return False
        from netaudio import core

        try:
            parsed = core.parse_response("cmc_registration", response)
        except core.NetaudioCoreError:
            return False
        return parsed["sequence"] == message_id and parsed["accepted"]

    async def register_device(
        self,
        device_ip: str,
        host_media_access_control_address: bytes | None = None,
    ) -> bytes | None:
        from netaudio import core

        message_id = core.next_message_id()
        host_mac = host_media_access_control_address or self._host_media_access_control_address
        specification = {"command": "cmc_register", "message_id": message_id}

        if host_mac is not None:
            specification["host_mac"] = host_mac.hex()

        try:
            response = await self._transport.execute(
                str(device_ip),
                specification,
            )
        except core.NetaudioCoreError as exception:
            logger.debug(f"CMC registration request failed for {device_ip}: {exception}")
            response = None

        if self._registration_response_is_successful(message_id, response):
            self._registered_devices.add(device_ip)
            logger.debug(f"CMC registered with {device_ip}")
            return response

        self._registered_devices.discard(device_ip)
        if response is not None:
            logger.warning(f"CMC registration returned an invalid response from {device_ip}")
        return None

    async def require_registration(
        self,
        device_ip: str,
        host_media_access_control_address: bytes | None = None,
    ) -> bytes:
        response = await self.register_device(device_ip, host_media_access_control_address)
        if response is None:
            raise RuntimeError(f"CMC registration failed for {device_ip}")
        return response

    async def register_all(self, device_ips: list[str]) -> None:
        tasks = [self.register_device(ip) for ip in device_ips]
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
