from __future__ import annotations

import asyncio
import logging
import socket
import struct
import sys

from netaudio.core.binding import NetaudioCoreError
from netaudio.dante.core_transport import CoreTransport

logger = logging.getLogger("netaudio")

SIOCGIFADDR = 0x8915
SIOCGIFHWADDR = 0x8927


def _get_mac_for_interface(interface_name: str) -> bytes | None:
    if sys.platform != "linux":
        return None

    import fcntl

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        mac_info = fcntl.ioctl(s.fileno(), SIOCGIFHWADDR, struct.pack("256s", interface_name.encode()))
        s.close()
        return mac_info[18:24]
    except OSError:
        return None


def _get_host_mac(interface_name: str | None = None) -> bytes:
    if interface_name:
        mac = _get_mac_for_interface(interface_name)
        if mac:
            return mac

    from netaudio import core

    mac = core.host_mac()
    if mac:
        return mac

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("224.0.0.231", 1))
        local_ip = sock.getsockname()[0]
        sock.close()

        if sys.platform == "linux":
            import fcntl

            for _, name in socket.if_nameindex():
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    addr_info = fcntl.ioctl(s.fileno(), SIOCGIFADDR, struct.pack("256s", name.encode()))
                    ip = socket.inet_ntoa(addr_info[20:24])
                    if ip == local_ip:
                        mac_info = fcntl.ioctl(s.fileno(), SIOCGIFHWADDR, struct.pack("256s", name.encode()))
                        s.close()
                        return mac_info[18:24]
                    s.close()
                except OSError:
                    continue

        if sys.platform == "darwin":
            import subprocess

            for interface in ["en0", "en1", "en2", "en3", "en4"]:
                try:
                    result = subprocess.run(
                        ["ifconfig", interface],
                        capture_output=True,
                        text=True,
                        timeout=2,
                    )
                    if result.returncode != 0:
                        continue
                    has_ip = False
                    mac_addr = None
                    for line in result.stdout.splitlines():
                        line = line.strip()
                        if line.startswith("inet ") and local_ip in line:
                            has_ip = True
                        if line.startswith("ether "):
                            mac_addr = line.split()[1]
                    if has_ip and mac_addr:
                        return bytes.fromhex(mac_addr.replace(":", ""))
                except (OSError, subprocess.TimeoutExpired, ValueError) as exception:
                    logger.warning(f"Could not read interface {interface}: {exception}")
                    continue
    except (OSError, ValueError):
        logger.exception("Failed to derive host MAC address from network interfaces")

    import uuid

    return uuid.getnode().to_bytes(6, "big")


class DanteCMCService:
    def __init__(
        self,
        transport: CoreTransport,
        interface_name: str | None = None,
        host_media_access_control_address: bytes | None = None,
    ):
        self._transport = transport
        self._sequence_counter = 0
        self._registered_devices: set[str] = set()
        self._heartbeat_task: asyncio.Task | None = None
        self._host_media_access_control_address = (
            host_media_access_control_address
            if host_media_access_control_address is not None
            else _get_host_mac(interface_name)
        )

    @property
    def host_media_access_control_address(self) -> bytes:
        return self._host_media_access_control_address

    @property
    def registered_devices(self) -> frozenset[str]:
        return frozenset(self._registered_devices)

    @staticmethod
    def _registration_response_is_successful(sequence: int, response: bytes | None) -> bool:
        if response is None:
            return False
        from netaudio import core

        try:
            parsed = core.parse_response("cmc_registration", response)
        except core.NetaudioCoreError:
            return False
        return parsed == {"sequence": sequence, "status": 1}

    async def register_device(
        self,
        device_ip: str,
        host_media_access_control_address: bytes | None = None,
    ) -> bytes | None:
        from netaudio import core

        sequence = self._sequence_counter
        self._sequence_counter = (self._sequence_counter + 1) & 0xFFFF
        host_mac = host_media_access_control_address or self._host_media_access_control_address
        try:
            response = await self._transport.execute(
                str(device_ip),
                {"command": "cmc_register", "host_mac": host_mac.hex(), "sequence": sequence},
            )
        except core.NetaudioCoreError as exception:
            logger.debug(f"CMC registration request failed for {device_ip}: {exception}")
            response = None

        if self._registration_response_is_successful(sequence, response):
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
                "timeout": True,
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
                "device_name": device_name,
                "mac": mac.hex() if isinstance(mac, bytes) else mac,
            },
        )
