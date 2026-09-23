from __future__ import annotations

from netaudio import core


def _mac_to_hex(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    return value


def validate_dante_name(name: str) -> str | None:
    try:
        core.build_command({"command": "set_name", "name": name})
    except core.NetaudioCoreError as error:
        return error.detail or str(error)

    return None


class DanteCommands:
    def _with_message_id(self, specification: dict, host_mac=None) -> dict:
        specification["message_id"] = core.next_message_id()
        return self._with_host_mac(specification, host_mac)

    @staticmethod
    def _with_host_mac(specification: dict, host_mac=None) -> dict:
        if host_mac is not None:
            specification["host_mac"] = _mac_to_hex(host_mac)
        return specification

    def panel_control(self, request: dict, requester: int, sequence: int, host_mac=None) -> dict:
        return self._with_host_mac(
            {
                "command": "panel_control",
                "request": request,
                "requester": requester,
                "sequence": sequence,
                "message_id": core.next_message_id(),
            },
            host_mac,
        )

    def capability_partition_export(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "capability_partition_export"}, host_mac)

    def clear_all_configuration(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "clear_all_configuration"}, host_mac)

    def clear_all_configuration_preserving_internet_protocol_settings(self, host_mac=None) -> dict:
        return self._with_message_id(
            {"command": "clear_all_configuration_preserving_internet_protocol_settings"}, host_mac
        )

    def dante_model(self, mac) -> dict:
        return {"command": "dante_model", "mac": _mac_to_hex(mac)}

    def device_log_export(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "device_log_export"}, host_mac)

    def enable_aes67(self, is_enabled: bool, host_mac=None) -> dict:
        return self._with_message_id({"command": "enable_aes67", "enabled": is_enabled}, host_mac)

    def factory_reset(self, host_mac: bytes | None = None) -> dict:
        return self._with_host_mac({"command": "factory_reset"}, host_mac)

    def identify(self) -> dict:
        return self._with_message_id({"command": "identify"})

    def make_model(self, mac) -> dict:
        return {"command": "make_model", "mac": _mac_to_hex(mac)}

    def probe_aes67(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "probe_aes67"}, host_mac)

    def probe_clear_configuration_status(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "probe_clear_configuration_status"}, host_mac)

    def probe_encoding(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "probe_encoding"}, host_mac)

    def probe_codec_status(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "probe_codec_status"}, host_mac)

    def probe_interface_status(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "probe_interface_status"}, host_mac)

    def probe_interface_statistics(self, host_mac=None, *, extended_073a: bool = False) -> dict:
        return self._with_message_id(
            {"command": "probe_interface_statistics", "extended_073a": extended_073a},
            host_mac,
        )

    def probe_lock_reset_status(self, host_mac=None, request_value: int | None = None) -> dict:
        specification = {"command": "probe_lock_reset_status"}

        if request_value is not None:
            specification["request_value"] = request_value

        return self._with_message_id(specification, host_mac)

    def probe_sample_rate(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "probe_sample_rate"}, host_mac)

    def probe_sample_rate_pullup(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "probe_sample_rate_pullup"}, host_mac)

    def probe_switch_configuration(self, host_mac=None) -> dict:
        return self._with_message_id({"command": "probe_switch_configuration"}, host_mac)

    def set_dante_redundancy(
        self,
        mode: str,
        switch_configuration_choice: int | None = None,
        host_mac=None,
    ) -> dict:
        return self._with_message_id(
            {
                "command": "set_dante_redundancy",
                "mode": mode,
                "switch_configuration_choice": switch_configuration_choice,
            },
            host_mac,
        )

    def query_latency_config(self) -> dict:
        return {"command": "query_latency_config"}

    def reboot(self, host_mac: bytes | None = None) -> dict:
        return self._with_host_mac({"command": "reboot"}, host_mac)

    def clock_control(self, control: dict, host_mac=None) -> dict:
        return self._with_message_id({"command": "clock_control", "control": control}, host_mac)

    def refresh_clock_status(self, record_revision: int, host_mac=None, message_id: int | None = None) -> dict:
        specification = {"command": "refresh_clock_status", "record_revision": record_revision}

        if message_id is None:
            return self._with_message_id(specification, host_mac)

        specification["message_id"] = message_id
        return self._with_host_mac(specification, host_mac)

    def reset_channel_name(self, channel_type: str, channel_number: int) -> dict:
        return {"channel_number": channel_number, "channel_type": channel_type, "command": "reset_channel_name"}

    def reset_name(self) -> dict:
        return {"command": "reset_name"}

    def set_aes67_multicast_prefix(self, prefix: str) -> dict:
        return {"command": "set_aes67_multicast_prefix", "prefix": prefix}

    def set_channel_name(self, channel_type: str, channel_number: int, name: str, protocol_id: int) -> dict:
        return {
            "channel_number": channel_number,
            "channel_type": channel_type,
            "command": "set_channel_name",
            "name": name,
            "protocol_id": protocol_id,
        }

    def set_encoding(self, encoding: int) -> dict:
        return self._with_message_id({"command": "set_encoding", "encoding": encoding})

    def set_gain_level(self, channel_number: int, gain_level: int, device_type: str, host_mac=None) -> dict:
        return self._with_message_id(
            {
                "channel_number": channel_number,
                "command": "set_gain_level",
                "device_type": device_type,
                "gain_level": gain_level,
            },
            host_mac,
        )

    def set_interface_dhcp(self, host_mac=None, *, interface="primary") -> dict:
        return self._with_message_id(
            {
                "command": "set_interface_dhcp",
                "interface": interface,
            },
            host_mac,
        )

    def set_interface_static(
        self,
        ip_address: str,
        netmask: str,
        dns_server: str,
        gateway: str,
        host_mac=None,
        *,
        interface="primary",
    ) -> dict:
        return self._with_message_id(
            {
                "command": "set_interface_static",
                "interface": interface,
                "dns": dns_server,
                "gateway": gateway,
                "ip": ip_address,
                "netmask": netmask,
            },
            host_mac,
        )

    def set_latency(self, latency_milliseconds: float) -> dict:
        return {"command": "set_latency", "latency": latency_milliseconds}

    def set_name(self, name: str) -> dict:
        return {"command": "set_name", "name": name}

    def set_sample_rate(self, sample_rate: int) -> dict:
        return self._with_message_id({"command": "set_sample_rate", "sample_rate": sample_rate})

    def set_sample_rate_pullup(self, raw_value: int, host_mac=None) -> dict:
        return self._with_message_id({"command": "set_sample_rate_pullup", "raw_value": raw_value}, host_mac)
