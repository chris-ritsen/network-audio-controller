from __future__ import annotations

import logging
import os

import ifaddr

from netaudio.dante.const import DEFAULT_MULTICAST_METERING_PORT

logger = logging.getLogger("netaudio")

DEFAULT_DAEMON_PORT = 4780
DEFAULT_INTERFACE = None


def get_available_interfaces():
    interfaces = []
    adapters = ifaddr.get_adapters()

    for adapter in adapters:
        for ip in adapter.ips:
            if isinstance(ip.ip, str):
                interfaces.append((adapter.nice_name, ip.ip, ip.network_prefix))

    return sorted(interfaces)


def configured_daemon_port() -> int:
    value = os.environ.get("NETAUDIO_DAEMON_PORT")
    if not value:
        from netaudio.common.config_loader import load_daemon_config

        value = load_daemon_config().get("port", DEFAULT_DAEMON_PORT)
    return int(value)


class AppSettings:
    def __init__(self):
        self.debug: bool = False
        self.no_color: bool = False
        self._interface: str = DEFAULT_INTERFACE
        self.metering_port: int = int(os.environ.get("NETAUDIO_METERING_PORT", DEFAULT_MULTICAST_METERING_PORT))
        self._daemon_port: int | None = None
        self._device_lock_key: bytes | None = None
        lock_key_value = os.environ.get("NETAUDIO_DEVICE_LOCK_KEY")
        if lock_key_value:
            self._device_lock_key = lock_key_value.encode("ascii")

    @property
    def daemon_port(self) -> int:
        if self._daemon_port is None:
            self._daemon_port = configured_daemon_port()
        return self._daemon_port

    @daemon_port.setter
    def daemon_port(self, value: int | None) -> None:
        self._daemon_port = value

    @property
    def device_lock_key(self) -> bytes | None:
        return self._device_lock_key

    @device_lock_key.setter
    def device_lock_key(self, value: bytes | None) -> None:
        self._device_lock_key = value

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
