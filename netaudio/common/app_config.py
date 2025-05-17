"""
Application runtime configuration.
"""

import sys

import ifaddr

DEFAULT_MDNS_TIMEOUT = 5
DEFAULT_INTERFACE = None


def get_available_interfaces():
    """
    Returns a list of available network interfaces with their IP addresses.
    Each entry is a tuple of (interface_name, ip_address, network_prefix)
    Only IPv4 addresses are returned.
    """
    interfaces = []
    adapters = ifaddr.get_adapters()

    for adapter in adapters:
        for ip in adapter.ips:
            # Only include IPv4 addresses (which are represented as strings)
            if isinstance(ip.ip, str):
                interfaces.append((adapter.nice_name, ip.ip, ip.network_prefix))
            # Skip IPv6 addresses (which are represented as tuples)

    return sorted(interfaces)


class AppSettings:
    def __init__(self):
        self._mdns_timeout: float = DEFAULT_MDNS_TIMEOUT
        self.dump_payloads: bool = False
        self._interface: str = DEFAULT_INTERFACE
        self._interface_ip: str = None
        self.refresh: bool = False

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
        self._interface_ip = None

    @property
    def interface_ip(self) -> str:
        """
        Returns the IP address for the configured interface.
        If no interface is explicitly set, returns None which means use the default.
        """
        if not self._interface:
            return None

        if self._interface_ip:
            return self._interface_ip

        adapters = ifaddr.get_adapters()

        for adapter in adapters:
            if adapter.nice_name == self._interface:
                ipv4_addresses = [ip.ip for ip in adapter.ips if isinstance(ip.ip, str)]

                if ipv4_addresses:
                    self._interface_ip = ipv4_addresses[0]

                    print(
                        f"Using IPv4 address {self._interface_ip} for interface {self._interface}",
                        file=sys.stderr,
                    )

                    return self._interface_ip

                print(
                    f"No IPv4 address found for interface {self._interface}",
                    file=sys.stderr,
                )

                return None

        print(
            f"Warning: Could not find interface '{self._interface}'. Using default interface.",
            file=sys.stderr,
        )

        return None


settings = AppSettings()
