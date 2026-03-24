import logging

from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.service import DanteUnicastService

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

    async def set_gain_level(
        self, device_ip: str, channel_number: int, gain_level: int, device_type: str,
    ) -> bytes | None:
        command_args = self._commands.command_set_gain_level(channel_number, gain_level, device_type)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        return await self.request(
            packet, device_ip, port,
            logical_command_name="set_gain_level",
        )

    async def set_sample_rate(self, device_ip: str, sample_rate: int) -> bytes | None:
        command_args = self._commands.command_set_sample_rate(sample_rate)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        return await self.request(
            packet, device_ip, port,
            logical_command_name="set_sample_rate",
        )

    async def set_encoding(self, device_ip: str, encoding: int) -> bytes | None:
        command_args = self._commands.command_set_encoding(encoding)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        return await self.request(
            packet, device_ip, port,
            logical_command_name="set_encoding",
        )

    async def enable_aes67(self, device_ip: str, is_enabled: bool, host_mac: bytes = None) -> bytes | None:
        command_args = self._commands.command_enable_aes67(is_enabled, host_mac=host_mac)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        return await self.request(
            packet, device_ip, port,
            logical_command_name="enable_aes67",
        )

    def request_bluetooth_status(self, device_ip: str, host_mac: bytes = None) -> None:
        command_args = self._commands.command_bluetooth_status(host_mac=host_mac)
        packet = command_args[0]
        port = command_args[2] or SETTINGS_PORT
        self.send(packet, device_ip, port)
