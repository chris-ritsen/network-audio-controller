import logging

logger = logging.getLogger("netaudio")


class DanteDeviceOperations:
    def __init__(self, device):
        self.device = device

    async def set_channel_name(self, channel_type, channel_number, new_channel_name):
        cmd_args = self.device.commands.command_set_channel_name(
            channel_type, channel_number, new_channel_name
        )
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_channel_name"
        )

        return response

    async def identify(self):
        command_identify_args = self.device.commands.command_identify()
        response = await self.device.dante_command(
            *command_identify_args, logical_command_name="identify"
        )

        return response

    async def set_latency(self, latency):
        cmd_args = self.device.commands.command_set_latency(latency)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_latency"
        )

        return response

    async def set_gain_level(self, channel_number, gain_level, device_type):
        cmd_args = self.device.commands.command_set_gain_level(
            channel_number, gain_level, device_type
        )
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_gain_level"
        )

        return response

    async def enable_aes67(self, is_enabled: bool):
        cmd_args = self.device.commands.command_enable_aes67(is_enabled=is_enabled)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="enable_aes67"
        )

        return response

    async def set_encoding(self, encoding):
        cmd_args = self.device.commands.command_set_encoding(encoding)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_encoding"
        )

        return response

    async def set_sample_rate(self, sample_rate):
        cmd_args = self.device.commands.command_set_sample_rate(sample_rate)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_sample_rate"
        )

        return response

    async def add_subscription(self, rx_channel, tx_channel, tx_device):
        tx_channel_name = tx_channel.friendly_name if tx_channel.friendly_name else tx_channel.name
        cmd_args = self.device.commands.command_add_subscription(
            rx_channel.number, tx_channel_name, tx_device.name
        )
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="add_subscription"
        )

        return response

    async def remove_subscription(self, rx_channel):
        cmd_args = self.device.commands.command_remove_subscription(rx_channel.number)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="remove_subscription"
        )

        return response

    async def reset_channel_name(self, channel_type, channel_number):
        cmd_args = self.device.commands.command_reset_channel_name(channel_type, channel_number)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="reset_channel_name"
        )

        return response

    async def set_name(self, name):
        cmd_args = self.device.commands.command_set_name(name)
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="set_name"
        )

        return response

    async def reset_name(self):
        cmd_args = self.device.commands.command_reset_name()
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="reset_name"
        )

        return response

    async def get_device_settings(self):
        cmd_args = self.device.commands.command_device_settings()
        response = await self.device.dante_command(
            *cmd_args, logical_command_name="get_device_settings"
        )

        if response:
            settings = self.device.parser.parse_device_settings(response)
            if "latency" in settings:
                self.device.latency = settings["latency"]
            if "sample_rate" in settings:
                self.device.sample_rate = settings["sample_rate"]
            return settings

        return None
