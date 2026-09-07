from __future__ import annotations

import logging
import os
import sys

import ifaddr

from netaudio.dante.const import DEFAULT_MULTICAST_METERING_PORT

logger = logging.getLogger("netaudio")

DEFAULT_MDNS_TIMEOUT = 5
DEFAULT_DAEMON_PORT = 9000
DEFAULT_INTERFACE = None
DEFAULT_STALE_DEVICE_MINUTES = 60


def get_available_interfaces():
    interfaces = []
    adapters = ifaddr.get_adapters()

    for adapter in adapters:
        for ip in adapter.ips:
            if isinstance(ip.ip, str):
                interfaces.append((adapter.nice_name, ip.ip, ip.network_prefix))

    return sorted(interfaces)


class AppSettings:
    def __init__(self):
        self._mdns_timeout: float = DEFAULT_MDNS_TIMEOUT
        self.dump_payloads: bool = False
        self.debug: bool = False
        self.no_color: bool = False
        self._interface: str = DEFAULT_INTERFACE
        self.refresh: bool = False
        self.metering_port: int = int(os.environ.get("NETAUDIO_METERING_PORT", DEFAULT_MULTICAST_METERING_PORT))
        self.daemon_port: int = int(os.environ.get("NETAUDIO_DAEMON_PORT", DEFAULT_DAEMON_PORT))
        self.dbus_enabled: bool = os.environ.get("NETAUDIO_DBUS", "").lower() in ("1", "true", "yes")
        self.lock_state_timeout: float = float(os.environ.get("NETAUDIO_LOCK_STATE_TIMEOUT", 4))
        self.stale_device_minutes: float = DEFAULT_STALE_DEVICE_MINUTES
        self._device_lock_key: bytes | None = None
        lock_key_value = os.environ.get("NETAUDIO_DEVICE_LOCK_KEY")
        if lock_key_value:
            self._device_lock_key = lock_key_value.encode("ascii")

    @property
    def device_lock_key(self) -> bytes | None:
        return self._device_lock_key

    @device_lock_key.setter
    def device_lock_key(self, value: bytes | None) -> None:
        self._device_lock_key = value

    @property
    def mdns_timeout(self) -> float:
        return self._mdns_timeout

    @mdns_timeout.setter
    def mdns_timeout(self, value: float) -> None:
        if value > 0:
            self._mdns_timeout = value
        else:
            print(
                f"Warning: mDNS timeout must be positive. Received {value}. Using default {DEFAULT_MDNS_TIMEOUT}s instead.",
                file=sys.stderr,
            )

            self._mdns_timeout = DEFAULT_MDNS_TIMEOUT

    @property
    def interface(self) -> str:
        return self._interface

    @interface.setter
    def interface(self, value: str) -> None:
        self._interface = value

    @property
    def interface_ip(self) -> str | None:
        if not self._interface:
            return None

        adapters = ifaddr.get_adapters()

        for adapter in adapters:
            if adapter.nice_name == self._interface:
                ipv4_addresses = [ip.ip for ip in adapter.ips if isinstance(ip.ip, str)]

                if ipv4_addresses:
                    logger.debug(
                        "Using IPv4 address %s for interface %s",
                        ipv4_addresses[0],
                        self._interface,
                    )

                    return ipv4_addresses[0]

                raise RuntimeError(f"Configured interface {self._interface!r} has no IPv4 address")

        raise RuntimeError(f"Configured interface {self._interface!r} was not found")


settings = AppSettings()
