from __future__ import annotations

import logging
import secrets

from netaudio.dante.core_transport import CoreTransport

logger = logging.getLogger("netaudio")


def _mac_to_hex(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    return value


class DanteSettingsService:
    def __init__(self, transport: CoreTransport):
        self._transport = transport
        self._sequence = secrets.randbelow(0xFFFF)

    @property
    def transport(self) -> CoreTransport:
        return self._transport

    def _next_sequence(self) -> int:
        self._sequence = (self._sequence + 1) & 0xFFFF
        if self._sequence == 0:
            self._sequence = 1
        return self._sequence

    async def _execute(self, device_ip_address, specification: dict, host_mac=None) -> None:
        if host_mac is not None:
            specification["host_mac"] = _mac_to_hex(host_mac)
        await self._transport.execute(str(device_ip_address), specification)

    async def clear_all_configuration(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "clear_all_configuration", "sequence": self._next_sequence()},
            host_mac,
        )

    async def clear_all_configuration_preserving_internet_protocol_settings(
        self,
        device_ip_address,
        host_mac=None,
    ) -> None:
        await self._execute(
            device_ip_address,
            {
                "command": "clear_all_configuration_preserving_internet_protocol_settings",
                "sequence": self._next_sequence(),
            },
            host_mac,
        )

    async def enable_aes67(self, device_ip_address, is_enabled: bool, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "enable_aes67", "enabled": bool(is_enabled), "sequence": self._next_sequence()},
            host_mac,
        )

    async def factory_reset(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "factory_reset", "sequence": self._next_sequence()},
            host_mac,
        )

    async def identify(self, device_ip_address) -> None:
        await self._execute(device_ip_address, {"command": "identify", "sequence": self._next_sequence()})

    async def probe_aes67(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "probe_aes67", "sequence": self._next_sequence()},
            host_mac,
        )

    async def probe_clear_configuration_status(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "probe_clear_configuration_status", "sequence": self._next_sequence()},
            host_mac,
        )

    async def probe_encoding(self, device_ip_address, host_mac=None) -> None:
        await self._execute(device_ip_address, {"command": "probe_encoding"}, host_mac)

    async def probe_gain_level(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "probe_gain_level", "sequence": self._next_sequence()},
            host_mac,
        )

    async def probe_interface_status(self, device_ip_address, host_mac=None) -> None:
        await self._execute(device_ip_address, {"command": "probe_interface_status"}, host_mac)

    async def probe_link_status(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "probe_link_status", "sequence": self._next_sequence()},
            host_mac,
        )

    async def probe_lock_reset_status(self, device_ip_address, host_mac=None, request_value: int = 100) -> None:
        await self._execute(
            device_ip_address,
            {
                "command": "probe_lock_reset_status",
                "request_value": request_value,
                "sequence": self._next_sequence(),
            },
            host_mac,
        )

    async def probe_preferred_leader(self, device_ip_address, clock_source: int = 0, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {
                "clock_source": clock_source,
                "command": "probe_preferred_leader",
                "sequence": self._next_sequence(),
            },
            host_mac,
        )

    async def probe_sample_rate(self, device_ip_address, host_mac=None) -> None:
        await self._execute(device_ip_address, {"command": "probe_sample_rate"}, host_mac)

    async def probe_sample_rate_pullup(self, device_ip_address, host_mac=None) -> None:
        await self._execute(device_ip_address, {"command": "probe_sample_rate_pullup"}, host_mac)

    async def probe_switch_configuration(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "probe_switch_configuration", "sequence": self._next_sequence()},
            host_mac,
        )

    async def reboot(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "reboot", "sequence": self._next_sequence()},
            host_mac,
        )

    async def refresh_clock_status(self, device_ip_address, host_mac=None, sequence: int = 0x0021) -> None:
        await self._execute(
            device_ip_address,
            {"command": "refresh_clock_status", "sequence": sequence},
            host_mac,
        )

    async def request_bluetooth_status(self, device_ip_address, host_mac=None) -> None:
        await self._execute(device_ip_address, {"command": "bluetooth_status"}, host_mac)

    async def request_capability_partition_export(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "capability_partition_export", "sequence": self._next_sequence()},
            host_mac,
        )

    async def request_dante_model(self, device_ip_address, mac) -> None:
        await self._execute(device_ip_address, {"command": "dante_model", "mac": _mac_to_hex(mac)})

    async def request_device_log_export(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "device_log_export", "sequence": self._next_sequence()},
            host_mac,
        )

    async def request_make_model(self, device_ip_address, mac) -> None:
        await self._execute(device_ip_address, {"command": "make_model", "mac": _mac_to_hex(mac)})

    async def set_clock_source(self, device_ip_address, clock_source: int, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"clock_source": clock_source, "command": "set_clock_source", "sequence": self._next_sequence()},
            host_mac,
        )

    async def set_clock_subdomain(self, device_ip_address, subdomain, host_mac=None) -> None:
        subdomain_bytes = subdomain.encode("ascii") if isinstance(subdomain, str) else bytes(subdomain)
        if len(subdomain_bytes) > 16:
            raise ValueError("clock subdomain is longer than 16 bytes")
        await self._execute(
            device_ip_address,
            {
                "command": "set_clock_subdomain",
                "sequence": self._next_sequence(),
                "subdomain": list(subdomain_bytes.ljust(16, b"\x00")),
            },
            host_mac,
        )

    async def set_encoding(self, device_ip_address, encoding: int) -> None:
        await self._execute(
            device_ip_address,
            {"command": "set_encoding", "encoding": encoding, "sequence": self._next_sequence()},
        )

    async def set_gain_level(
        self,
        device_ip_address,
        channel_number: int,
        gain_level: int,
        device_type: str,
        host_mac=None,
    ) -> None:
        await self._execute(
            device_ip_address,
            {
                "channel_number": channel_number,
                "command": "set_gain_level",
                "device_type": device_type,
                "gain_level": gain_level,
                "sequence": self._next_sequence(),
            },
            host_mac,
        )

    async def set_interface_dhcp(self, device_ip_address, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "set_interface_dhcp", "sequence": self._next_sequence()},
            host_mac,
        )

    async def set_interface_static(
        self,
        device_ip_address,
        ip_address: str,
        netmask: str,
        dns_server: str,
        gateway: str,
        host_mac=None,
    ) -> None:
        await self._execute(
            device_ip_address,
            {
                "command": "set_interface_static",
                "dns": dns_server,
                "gateway": gateway,
                "ip": ip_address,
                "netmask": netmask,
                "sequence": self._next_sequence(),
            },
            host_mac,
        )

    async def set_preferred_leader(
        self,
        device_ip_address,
        is_preferred: bool,
        clock_source: int = 0,
        host_mac=None,
    ) -> None:
        await self._execute(
            device_ip_address,
            {
                "clock_source": clock_source,
                "command": "set_preferred_leader",
                "preferred": bool(is_preferred),
                "sequence": self._next_sequence(),
            },
            host_mac,
        )

    async def set_sample_rate(self, device_ip_address, sample_rate: int) -> None:
        await self._execute(
            device_ip_address,
            {"command": "set_sample_rate", "sample_rate": sample_rate, "sequence": self._next_sequence()},
        )

    async def set_sample_rate_pullup(self, device_ip_address, raw_value: int, host_mac=None) -> None:
        await self._execute(
            device_ip_address,
            {"command": "set_sample_rate_pullup", "raw_value": raw_value, "sequence": self._next_sequence()},
            host_mac,
        )
