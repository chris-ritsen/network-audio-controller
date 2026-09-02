from __future__ import annotations

import re
import secrets

from netaudio.dante.const import PROTOCOL_ARC_2809

DANTE_NAME_MAX_LENGTH = 31
DANTE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$")


def _mac_to_hex(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    return value


def channel_status_query_specification(
    channel_type: str,
    protocol_id: int = PROTOCOL_ARC_2809,
    media_type: int = 1,
    starting_channel_identifier: int = 1,
    ending_channel_identifier: int = 0,
) -> dict:
    command = "query_receiver_channel_status_2809" if channel_type == "rx" else "query_transmitter_channel_status_2809"
    return {
        "command": command,
        "ending_channel_identifier": ending_channel_identifier,
        "media_type": media_type,
        "protocol_id": protocol_id,
        "starting_channel_identifier": starting_channel_identifier,
    }


def subscription_records(subscriptions) -> list[dict]:
    return [
        {"rx_channel": rx_channel, "tx_channel": tx_channel, "tx_device": tx_device}
        for rx_channel, tx_channel, tx_device in subscriptions
    ]


def validate_dante_name(name: str) -> str | None:
    if len(name) > DANTE_NAME_MAX_LENGTH:
        return f"Name exceeds {DANTE_NAME_MAX_LENGTH} characters"

    if not DANTE_NAME_PATTERN.match(name):
        if name.startswith("-") or name.endswith("-"):
            return "Name cannot begin or end with a hyphen"
        return "Name must contain only A-Z, a-z, 0-9, and hyphens"

    return None


class DanteCommands:
    def __init__(self):
        self._sequence = secrets.randbelow(0xFFFF)

    def _next_sequence(self) -> int:
        self._sequence = (self._sequence + 1) & 0xFFFF
        if self._sequence == 0:
            self._sequence = 1
        return self._sequence

    def _sequenced(self, specification: dict, host_mac=None) -> dict:
        specification["sequence"] = self._next_sequence()
        return self._with_host_mac(specification, host_mac)

    @staticmethod
    def _with_host_mac(specification: dict, host_mac=None) -> dict:
        if host_mac is not None:
            specification["host_mac"] = _mac_to_hex(host_mac)
        return specification

    def add_subscriptions(self, records) -> dict:
        return {"command": "add_subscriptions", "subscriptions": subscription_records(records)}

    def bluetooth_status(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "bluetooth_status"}, host_mac)

    def capability_partition_export(self, host_mac=None) -> dict:
        return self._sequenced({"command": "capability_partition_export"}, host_mac)

    def clear_all_configuration(self, host_mac=None) -> dict:
        return self._sequenced({"command": "clear_all_configuration"}, host_mac)

    def clear_all_configuration_preserving_internet_protocol_settings(self, host_mac=None) -> dict:
        return self._sequenced({"command": "clear_all_configuration_preserving_internet_protocol_settings"}, host_mac)

    def dante_model(self, mac) -> dict:
        return {"command": "dante_model", "mac": _mac_to_hex(mac)}

    def device_log_export(self, host_mac=None) -> dict:
        return self._sequenced({"command": "device_log_export"}, host_mac)

    def enable_aes67(self, is_enabled: bool, host_mac=None) -> dict:
        return self._sequenced({"command": "enable_aes67", "enabled": bool(is_enabled)}, host_mac)

    def factory_reset(self, host_mac: bytes) -> dict:
        return {"command": "factory_reset", "host_mac": host_mac.hex()}

    def identify(self) -> dict:
        return self._sequenced({"command": "identify"})

    def make_model(self, mac) -> dict:
        return {"command": "make_model", "mac": _mac_to_hex(mac)}

    def probe_aes67(self, host_mac=None) -> dict:
        return self._sequenced({"command": "probe_aes67"}, host_mac)

    def probe_clear_configuration_status(self, host_mac=None) -> dict:
        return self._sequenced({"command": "probe_clear_configuration_status"}, host_mac)

    def probe_encoding(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "probe_encoding"}, host_mac)

    def probe_gain_level(self, host_mac=None) -> dict:
        return self._sequenced({"command": "probe_gain_level"}, host_mac)

    def probe_interface_status(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "probe_interface_status"}, host_mac)

    def probe_link_status(self, host_mac=None) -> dict:
        return self._sequenced({"command": "probe_link_status"}, host_mac)

    def probe_lock_reset_status(self, host_mac=None, request_value: int = 100) -> dict:
        return self._sequenced({"command": "probe_lock_reset_status", "request_value": request_value}, host_mac)

    def probe_preferred_leader(self, clock_source: int = 0, host_mac=None) -> dict:
        return self._sequenced({"clock_source": clock_source, "command": "probe_preferred_leader"}, host_mac)

    def probe_sample_rate(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "probe_sample_rate"}, host_mac)

    def probe_sample_rate_pullup(self, host_mac=None) -> dict:
        return self._with_host_mac({"command": "probe_sample_rate_pullup"}, host_mac)

    def probe_switch_configuration(self, host_mac=None) -> dict:
        return self._sequenced({"command": "probe_switch_configuration"}, host_mac)

    def query_latency_config(self) -> dict:
        return {"command": "query_latency_config"}

    def query_receiver_flow_status_2809(self) -> dict:
        return {"command": "query_receiver_flow_status_2809"}

    def query_transmitter_flow_status_2809(self) -> dict:
        return {"command": "query_tx_flows", "flow_protocol_id": PROTOCOL_ARC_2809, "starting_flow": 1}

    def reboot(self, host_mac: bytes) -> dict:
        return {"command": "reboot", "host_mac": host_mac.hex()}

    def refresh_clock_status(self, host_mac=None, sequence: int = 0x0021) -> dict:
        return self._with_host_mac({"command": "refresh_clock_status", "sequence": sequence}, host_mac)

    def remove_subscriptions(self, channel_numbers) -> dict:
        return {"command": "remove_subscriptions", "rx_channels": list(channel_numbers)}

    def reset_channel_name(self, channel_type: str, channel_number: int) -> dict:
        return {"channel_number": channel_number, "channel_type": channel_type, "command": "reset_channel_name"}

    def reset_name(self) -> dict:
        return {"command": "reset_name"}

    def set_aes67_multicast_prefix(self, prefix: str) -> dict:
        return {"command": "set_aes67_multicast_prefix", "prefix": prefix}

    def set_channel_name(self, channel_type: str, channel_number: int, name: str, protocol_id=None) -> dict:
        specification = {
            "channel_number": channel_number,
            "channel_type": channel_type,
            "command": "set_channel_name",
            "name": name,
        }
        if protocol_id is not None:
            specification["protocol_id"] = protocol_id
        return specification

    def set_clock_source(self, clock_source: int, host_mac=None) -> dict:
        return self._sequenced({"clock_source": clock_source, "command": "set_clock_source"}, host_mac)

    def set_clock_subdomain(self, subdomain, host_mac=None) -> dict:
        subdomain_bytes = subdomain.encode("ascii") if isinstance(subdomain, str) else bytes(subdomain)
        if len(subdomain_bytes) > 16:
            raise ValueError("clock subdomain is longer than 16 bytes")
        return self._sequenced(
            {"command": "set_clock_subdomain", "subdomain": list(subdomain_bytes.ljust(16, b"\x00"))},
            host_mac,
        )

    def set_encoding(self, encoding: int) -> dict:
        return self._sequenced({"command": "set_encoding", "encoding": encoding})

    def set_gain_level(self, channel_number: int, gain_level: int, device_type: str, host_mac=None) -> dict:
        return self._sequenced(
            {
                "channel_number": channel_number,
                "command": "set_gain_level",
                "device_type": device_type,
                "gain_level": gain_level,
            },
            host_mac,
        )

    def set_interface_dhcp(self, host_mac=None) -> dict:
        return self._sequenced({"command": "set_interface_dhcp"}, host_mac)

    def set_interface_static(
        self,
        ip_address: str,
        netmask: str,
        dns_server: str,
        gateway: str,
        host_mac=None,
    ) -> dict:
        return self._sequenced(
            {
                "command": "set_interface_static",
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

    def set_preferred_leader(self, is_preferred: bool, clock_source: int = 0, host_mac=None) -> dict:
        return self._sequenced(
            {"clock_source": clock_source, "command": "set_preferred_leader", "preferred": bool(is_preferred)},
            host_mac,
        )

    def set_sample_rate(self, sample_rate: int) -> dict:
        return self._sequenced({"command": "set_sample_rate", "sample_rate": sample_rate})

    def set_sample_rate_pullup(self, raw_value: int, host_mac=None) -> dict:
        return self._sequenced({"command": "set_sample_rate_pullup", "raw_value": raw_value}, host_mac)
