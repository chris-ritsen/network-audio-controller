import asyncio
import logging

from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.service import DanteUnicastService

CONMON_SET_REPEAT_COUNT = 3
CONMON_SET_REPEAT_INTERVAL = 0.5

logger = logging.getLogger("netaudio")

SETTINGS_PORT = DEVICE_SETTINGS_PORT


class DanteSettingsService(DanteUnicastService):
    def __init__(self, packet_store=None, dissect=False):
        super().__init__(packet_store=packet_store, dissect=dissect)
        self._commands = DanteDeviceCommands()

    def identify(self, device_ip: str) -> None:
        command_args = self._commands.command_identify()
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)

    async def set_preferred_leader(self, device_ip: str, is_preferred: bool, clock_source: int = 0, host_mac: bytes = None) -> None:
        command_args = self._commands.command_set_preferred_leader(is_preferred, clock_source=clock_source, host_mac=host_mac)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        for attempt in range(CONMON_SET_REPEAT_COUNT):
            self.send(packet, device_ip, port)
            if attempt < CONMON_SET_REPEAT_COUNT - 1:
                await asyncio.sleep(CONMON_SET_REPEAT_INTERVAL)

    def probe_preferred_leader(self, device_ip: str, clock_source: int = 0, host_mac: bytes = None) -> None:
        command_args = self._commands.command_probe_preferred_leader(clock_source=clock_source, host_mac=host_mac)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)

    def probe_interface_status(self, device_ip: str) -> None:
        command_args = self._commands.command_probe_interface_status()
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)

    def set_interface_dhcp(self, device_ip: str) -> None:
        command_args = self._commands.command_set_interface_dhcp()
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)

    def set_interface_static(self, device_ip: str, ip_address: str, netmask: str, dns_server: str, gateway: str) -> None:
        command_args = self._commands.command_set_interface_static(ip_address, netmask, dns_server, gateway)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)

    def probe_aes67(self, device_ip: str, host_mac: bytes = None) -> None:
        command_args = self._commands.command_probe_aes67(host_mac=host_mac)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)

    def request_bluetooth_status(self, device_ip: str, host_mac: bytes = None) -> None:
        command_args = self._commands.command_bluetooth_status(host_mac=host_mac)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)
