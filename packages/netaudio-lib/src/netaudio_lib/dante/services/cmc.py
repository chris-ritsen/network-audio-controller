import asyncio
import logging
import struct

from netaudio_lib.dante.const import DEVICE_CONTROL_PORT
from netaudio_lib.dante.device_commands import DanteDeviceCommands
from netaudio_lib.dante.service import DanteUnicastService

logger = logging.getLogger("netaudio")

CMC_PORT = DEVICE_CONTROL_PORT

PROTOCOL_CMC = 0x1200
CMC_COMMAND_REGISTER = 0x1001


def _get_host_mac() -> bytes:
    try:
        import subprocess
        result = subprocess.run(
            ["ifconfig", "en0"], capture_output=True, text=True, timeout=2
        )
        for line in result.stdout.split("\n"):
            line = line.strip()
            if line.startswith("ether "):
                mac_str = line.split()[1]
                return bytes.fromhex(mac_str.replace(":", ""))
    except Exception:
        pass

    try:
        import uuid
        mac_int = uuid.getnode()
        return mac_int.to_bytes(6, "big")
    except Exception:
        return b"\x00" * 6


class DanteCMCService(DanteUnicastService):
    def __init__(self, packet_store=None):
        super().__init__(packet_store=packet_store)
        self._commands = DanteDeviceCommands()
        self._sequence_counter = 0
        self._registered_devices: set[str] = set()
        self._heartbeat_task: asyncio.Task | None = None
        self._host_mac = _get_host_mac()

    def _build_registration_packet(self, sequence: int) -> bytes:
        payload = struct.pack(">H", sequence)
        payload += struct.pack(">H", CMC_COMMAND_REGISTER)
        payload += b"\x00" * 4
        payload += self._host_mac
        payload += b"\x00\x00"

        length = len(payload) + 4
        header = struct.pack(">HH", PROTOCOL_CMC, length)
        return header + payload

    async def register_device(self, device_ip: str) -> bytes | None:
        sequence = self._sequence_counter
        self._sequence_counter = (self._sequence_counter + 1) & 0xFFFF

        packet = self._build_registration_packet(sequence)
        response = await self.request(
            packet, device_ip, CMC_PORT,
            timeout=1.0,
            logical_command_name="cmc_register",
        )

        if response:
            self._registered_devices.add(device_ip)
            logger.debug(f"CMC registered with {device_ip}")

        return response

    async def register_all(self, device_ips: list[str]) -> None:
        tasks = [self.register_device(ip) for ip in device_ips]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def start_heartbeat(self, get_device_ips) -> None:
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(get_device_ips)
        )

    async def _heartbeat_loop(self, get_device_ips) -> None:
        while True:
            try:
                await asyncio.sleep(5)
                device_ips = get_device_ips()
                if device_ips:
                    await self.register_all(device_ips)
            except asyncio.CancelledError:
                break
            except Exception as exception:
                logger.debug(f"CMC heartbeat error: {exception}")

    async def stop(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        self._registered_devices.clear()
        await super().stop()

    async def start_metering(
        self, device_ip: str, device_name: str, ipv4, mac, port: int,
    ) -> bytes | None:
        command_args = self._commands.command_volume_start(device_name, ipv4, mac, port)
        packet = command_args[0]
        target_port = command_args[2] or CMC_PORT
        return await self.request(
            packet, device_ip, target_port,
            device_name=device_name,
            logical_command_name="volume_start",
        )

    async def stop_metering(
        self, device_ip: str, device_name: str, ipv4, mac, port: int,
    ) -> bytes | None:
        command_args = self._commands.command_volume_stop(device_name, ipv4, mac, port)
        packet = command_args[0]
        target_port = command_args[2] or CMC_PORT
        return await self.request(
            packet, device_ip, target_port,
            device_name=device_name,
            logical_command_name="volume_stop",
        )
