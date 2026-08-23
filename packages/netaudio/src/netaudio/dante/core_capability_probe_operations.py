from __future__ import annotations


class CoreCapabilityProbeOperations:
    async def probe_sample_rate_status(self, device_ip_address, timeout: float = 2.0):
        from netaudio.dante.device_commands import DanteDeviceCommands

        commands = DanteDeviceCommands(host_mac=self._host_mac)
        return await self._probe_audio_capability(
            "sample_rate",
            device_ip_address,
            commands.command_probe_sample_rate,
            "register_sample_rate_waiter",
            "get_sample_rate_result",
            "unregister_sample_rate_waiter",
            "sample rate",
            timeout,
        )

    async def probe_sample_rate_pullup_status(self, device_ip_address, timeout: float = 2.0):
        from netaudio.dante.device_commands import DanteDeviceCommands

        commands = DanteDeviceCommands(host_mac=self._host_mac)
        return await self._probe_audio_capability(
            "sample_rate_pullup",
            device_ip_address,
            commands.command_probe_sample_rate_pullup,
            "register_sample_rate_pullup_waiter",
            "get_sample_rate_pullup_result",
            "unregister_sample_rate_pullup_waiter",
            "sample rate pull-up",
            timeout,
        )

    async def probe_encoding_status(self, device_ip_address, timeout: float = 2.0):
        from netaudio.dante.device_commands import DanteDeviceCommands

        commands = DanteDeviceCommands(host_mac=self._host_mac)
        return await self._probe_audio_capability(
            "encoding",
            device_ip_address,
            commands.command_probe_encoding,
            "register_encoding_waiter",
            "get_encoding_result",
            "unregister_encoding_waiter",
            "encoding",
            timeout,
        )

    async def probe_link_status(self, device_ip_address, timeout: float = 2.0):
        from netaudio.dante.device_commands import DanteDeviceCommands

        commands = DanteDeviceCommands(host_mac=self._host_mac)
        return await self._probe_audio_capability(
            "link_status",
            device_ip_address,
            commands.command_probe_link_status,
            "register_link_status_waiter",
            "get_link_status_result",
            "unregister_link_status_waiter",
            "link status",
            timeout,
        )

    async def probe_switch_configuration(self, device_ip_address, timeout: float = 2.0):
        from netaudio.dante.device_commands import DanteDeviceCommands

        commands = DanteDeviceCommands(host_mac=self._host_mac)
        return await self._probe_audio_capability(
            "switch_configuration",
            device_ip_address,
            commands.command_probe_switch_configuration,
            "register_switch_configuration_waiter",
            "get_switch_configuration_result",
            "unregister_switch_configuration_waiter",
            "switch configuration",
            timeout,
        )
