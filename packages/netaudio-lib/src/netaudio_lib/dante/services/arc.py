import logging

from netaudio_lib.dante.device_commands import DanteDeviceCommands
from netaudio_lib.dante.device_parser import DanteDeviceParser
from netaudio_lib.dante.service import DanteUnicastService

logger = logging.getLogger("netaudio")


class DanteARCService(DanteUnicastService):
    def __init__(self, packet_store=None, dissect=False):
        super().__init__(packet_store=packet_store, dissect=dissect)
        self._commands = DanteDeviceCommands()
        self._parser = DanteDeviceParser()

    async def get_device_name(self, device_ip: str, arc_port: int) -> str | None:
        command_args = self._commands.command_device_name(
            transaction_id=self._next_transaction_id()
        )
        packet = command_args[0]
        response = await self.request(
            packet, device_ip, arc_port,
            logical_command_name="get_device_name",
        )
        if response and len(response) > 10:
            return response[10:-1].decode("ascii")
        return None

    async def get_channel_count(self, device_ip: str, arc_port: int) -> tuple[int, int] | None:
        command_args = self._commands.command_channel_count(
            transaction_id=self._next_transaction_id()
        )
        packet = command_args[0]
        response = await self.request(
            packet, device_ip, arc_port,
            logical_command_name="get_channel_count",
        )
        if response and len(response) >= 16:
            tx_count = int.from_bytes(response[13:14], "big")
            rx_count = int.from_bytes(response[15:16], "big")
            return tx_count, rx_count
        return None

    async def get_aes67_config(self, device_ip: str, arc_port: int) -> bool | None:
        command_args = self._commands.command_get_aes67_config(
            transaction_id=self._next_transaction_id()
        )
        packet = command_args[0]
        response = await self.request(
            packet, device_ip, arc_port,
            logical_command_name="get_aes67_config",
        )
        if response:
            if b'\x63\x00\x03' in response:
                return True
            elif b'\x63\x00\x01' in response:
                return False
        return None

    async def get_rx_channels(self, device, arc_port: int):
        device_ip = str(device.ipv4)

        async def command_func(command, service_type=None, port=None, logical_command_name="unknown"):
            return await self.request(
                command, device_ip, arc_port,
                device_name=device.name,
                logical_command_name=logical_command_name,
            )

        return await self._parser.get_rx_channels(device, command_func)

    async def get_tx_channels(self, device, arc_port: int):
        device_ip = str(device.ipv4)

        async def command_func(command, service_type=None, port=None, logical_command_name="unknown"):
            return await self.request(
                command, device_ip, arc_port,
                device_name=device.name,
                logical_command_name=logical_command_name,
            )

        return await self._parser.get_tx_channels(device, command_func)

    async def get_controls(self, device, arc_port: int) -> None:
        device_ip = str(device.ipv4)

        try:
            if not device.name:
                name = await self.get_device_name(device_ip, arc_port)
                if name:
                    device.name = name
                else:
                    logger.debug(f"Failed to get device name for {device.server_name}")

            counts = await self.get_channel_count(device_ip, arc_port)
            if counts:
                device.tx_count = device.tx_count_raw = counts[0]
                device.rx_count = device.rx_count_raw = counts[1]

            if device.aes67_enabled is None:
                try:
                    aes67_status = await self.get_aes67_config(device_ip, arc_port)
                    if aes67_status is not None:
                        device.aes67_enabled = aes67_status
                except Exception as exception:
                    logger.debug(f"Error getting AES67 config: {exception}")

            if device.tx_count:
                tx_channels = await self.get_tx_channels(device, arc_port)
                if tx_channels:
                    device.tx_channels = tx_channels

            if device.rx_count:
                rx_channels, subscriptions = await self.get_rx_channels(device, arc_port)
                if rx_channels:
                    device.rx_channels = rx_channels
                    device.subscriptions = subscriptions

            device.error = None
        except Exception as exception:
            device.error = exception
            logger.debug(f"Error getting controls for {device.server_name}: {exception}")

    async def set_channel_name(
        self, device_ip: str, arc_port: int, channel_type: str, channel_number: int, new_name: str
    ) -> bytes | None:
        command_args = self._commands.command_set_channel_name(channel_type, channel_number, new_name)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="set_channel_name",
        )

    async def reset_channel_name(
        self, device_ip: str, arc_port: int, channel_type: str, channel_number: int
    ) -> bytes | None:
        command_args = self._commands.command_reset_channel_name(channel_type, channel_number)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="reset_channel_name",
        )

    async def add_subscription(
        self, device_ip: str, arc_port: int,
        rx_channel_number: int, tx_channel_name: str, tx_device_name: str,
    ) -> bytes | None:
        command_args = self._commands.command_add_subscription(
            rx_channel_number, tx_channel_name, tx_device_name
        )
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="add_subscription",
        )

    async def remove_subscription(
        self, device_ip: str, arc_port: int, rx_channel_number: int
    ) -> bytes | None:
        command_args = self._commands.command_remove_subscription(rx_channel_number)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="remove_subscription",
        )

    async def set_latency(self, device_ip: str, arc_port: int, latency: float) -> bytes | None:
        command_args = self._commands.command_set_latency(latency)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="set_latency",
        )

    async def set_name(self, device_ip: str, arc_port: int, name: str) -> bytes | None:
        command_args = self._commands.command_set_name(name)
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="set_name",
        )

    async def reset_name(self, device_ip: str, arc_port: int) -> bytes | None:
        command_args = self._commands.command_reset_name()
        return await self.request(
            command_args[0], device_ip, arc_port,
            logical_command_name="reset_name",
        )
