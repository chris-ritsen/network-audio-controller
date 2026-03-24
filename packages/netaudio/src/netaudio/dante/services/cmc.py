import asyncio
import logging
import socket
import struct
import sys

from netaudio.dante.const import DEVICE_CONTROL_PORT
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.service import DanteUnicastService

logger = logging.getLogger("netaudio")

CMC_PORT = DEVICE_CONTROL_PORT

PROTOCOL_CMC = 0x1200
CMC_COMMAND_REGISTER = 0x1001

SIOCGIFADDR = 0x8915
SIOCGIFHWADDR = 0x8927


def _get_mac_for_interface(interface_name: str) -> bytes | None:
    if sys.platform != "linux":
        return None

    import fcntl

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        mac_info = fcntl.ioctl(s.fileno(), SIOCGIFHWADDR, struct.pack('256s', interface_name.encode()))
        s.close()
        return mac_info[18:24]
    except OSError:
        return None


def _get_host_mac(interface_name: str | None = None) -> bytes:
    if interface_name:
        mac = _get_mac_for_interface(interface_name)
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
                    addr_info = fcntl.ioctl(s.fileno(), SIOCGIFADDR, struct.pack('256s', name.encode()))
                    ip = socket.inet_ntoa(addr_info[20:24])
                    if ip == local_ip:
                        mac_info = fcntl.ioctl(s.fileno(), SIOCGIFHWADDR, struct.pack('256s', name.encode()))
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
                        capture_output=True, text=True, timeout=2,
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
                except Exception:
                    continue
    except Exception:
        pass

    import uuid
    return uuid.getnode().to_bytes(6, "big")


class DanteCMCService(DanteUnicastService):
    def __init__(self, packet_store=None, interface_name: str | None = None, dissect=False):
        super().__init__(packet_store=packet_store, dissect=dissect)
        self._commands = DanteDeviceCommands()
        self._sequence_counter = 0
        self._registered_devices: set[str] = set()
        self._heartbeat_task: asyncio.Task | None = None
        self._host_mac = _get_host_mac(interface_name)

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
                await asyncio.sleep(10)
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

    def start_metering(
        self, device_ip: str, device_name: str, ipv4, mac, port: int,
    ) -> None:
        command_args = self._commands.command_metering_start(device_name, ipv4, mac, port)
        packet = command_args[0]
        target_port = command_args[2] or CMC_PORT
        self.send(packet, device_ip, target_port)

    def stop_metering(
        self, device_ip: str, device_name: str, ipv4, mac, port: int,
    ) -> None:
        command_args = self._commands.command_metering_stop(device_name, ipv4, mac, port)
        packet = command_args[0]
        target_port = command_args[2] or CMC_PORT
        self.send(packet, device_ip, target_port)
