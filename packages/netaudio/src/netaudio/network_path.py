from __future__ import annotations

import ipaddress
import re
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

import ifaddr


DANTE_SECONDARY_NETWORK = ipaddress.IPv4Network("172.31.0.0/16")
SERVICE_ORDER_REFRESH_SECONDS = 5.0
_service_order_cache: tuple[float, dict[str, int]] = (0.0, {})
_service_order_lock = threading.Lock()


class NetworkPathError(RuntimeError):
    pass


@dataclass(frozen=True, order=True)
class IPv4Interface:
    priority: int
    name: str
    address: str
    network_prefix: int

    @property
    def network(self) -> ipaddress.IPv4Network:
        return ipaddress.IPv4Network((self.address, self.network_prefix), strict=False)


@dataclass(frozen=True)
class NetworkPath:
    interface: IPv4Interface
    generation: int

    @property
    def source_address(self) -> str:
        return self.interface.address


def _macos_service_order() -> dict[str, int]:
    global _service_order_cache

    if sys.platform != "darwin":
        return {}
    now = time.monotonic()
    with _service_order_lock:
        expires_at, cached = _service_order_cache
        if now < expires_at:
            return cached
    try:
        result = subprocess.run(
            ["networksetup", "-listnetworkserviceorder"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        service_order = {}
    else:
        service_order = (
            {
                match.group(2): int(match.group(1))
                for match in re.finditer(
                    r"^\((\d+)\)\s+.+\n\(Hardware Port: .+, Device: (\S+)\)",
                    result.stdout,
                    re.MULTILINE,
                )
            }
            if result.returncode == 0
            else {}
        )
    with _service_order_lock:
        _service_order_cache = (now + SERVICE_ORDER_REFRESH_SECONDS, service_order)
    return service_order


def invalidate_platform_preferences() -> None:
    global _service_order_cache

    with _service_order_lock:
        _service_order_cache = (0.0, {})


def active_ipv4_interfaces(
    selected_interface: str | None = None,
    *,
    adapters: Iterable | None = None,
    service_order: dict[str, int] | None = None,
    include_loopback: bool = False,
) -> tuple[IPv4Interface, ...]:
    priorities = _macos_service_order() if service_order is None else service_order
    interfaces = set()
    for adapter in ifaddr.get_adapters() if adapters is None else adapters:
        if selected_interface and adapter.nice_name != selected_interface:
            continue
        priority = priorities.get(adapter.nice_name, len(priorities) + 1)
        for adapter_ip in adapter.ips:
            if not isinstance(adapter_ip.ip, str):
                continue
            try:
                address = ipaddress.IPv4Address(adapter_ip.ip)
                network_prefix = int(adapter_ip.network_prefix)
                ipaddress.IPv4Network((address, network_prefix), strict=False)
            except (ipaddress.AddressValueError, TypeError, ValueError):
                continue
            if address.is_unspecified or (address.is_loopback and not include_loopback) or address.is_multicast:
                continue
            interfaces.add(IPv4Interface(priority, adapter.nice_name, str(address), network_prefix))
    return tuple(sorted(interfaces))


class NetworkPathManager:
    def __init__(
        self,
        *,
        interface_provider: Callable[[], Iterable[IPv4Interface]] | None = None,
        socket_factory: Callable[..., socket.socket] = socket.socket,
    ) -> None:
        self._interface_provider = interface_provider or (lambda: active_ipv4_interfaces(include_loopback=True))
        self._socket_factory = socket_factory
        self._lock = threading.Lock()
        self._interfaces: tuple[IPv4Interface, ...] = ()
        self._generation = 0

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def refresh(self) -> tuple[IPv4Interface, ...]:
        current = tuple(sorted(set(self._interface_provider())))
        with self._lock:
            if current != self._interfaces:
                self._interfaces = current
                self._generation += 1
            return self._interfaces

    def interfaces(self, selected_interface: str | None = None) -> tuple[IPv4Interface, ...]:
        interfaces = self.refresh()
        if selected_interface is None:
            return interfaces
        selected = tuple(interface for interface in interfaces if interface.name == selected_interface)
        if selected:
            return selected
        matching_name = any(interface.name == selected_interface for interface in interfaces)
        if matching_name:
            raise NetworkPathError(f"Configured interface {selected_interface!r} has no eligible IPv4 address")
        raise NetworkPathError(f"Configured interface {selected_interface!r} was not found")

    def resolve(self, destination: str, selected_interface: str | None = None) -> NetworkPath:
        try:
            destination_address = ipaddress.IPv4Address(destination)
        except ipaddress.AddressValueError as exception:
            raise NetworkPathError(f"Destination {destination!r} is not an IPv4 address") from exception
        if destination_address.is_unspecified or destination_address.is_multicast:
            raise NetworkPathError(f"Destination {destination!r} is not a unicast IPv4 address")

        candidates = self.interfaces(selected_interface)
        generation = self.generation
        if not candidates:
            raise NetworkPathError("No eligible IPv4 network interfaces are available")

        direct = tuple(interface for interface in candidates if destination_address in interface.network)
        if selected_interface is not None:
            return NetworkPath(self._select_explicit(candidates, direct), generation)

        scope_requires_interface_identity = (
            destination_address.is_link_local or destination_address in DANTE_SECONDARY_NETWORK
        )
        if scope_requires_interface_identity and len({interface.name for interface in direct}) > 1:
            names = ", ".join(interface.name for interface in direct)
            raise NetworkPathError(
                f"Destination {destination} is reachable through multiple interface-scoped networks ({names}); "
                "select an interface explicitly"
            )

        routed_source = self._route_source(destination)
        if routed_source is not None:
            for interface in candidates:
                if interface.address == routed_source:
                    return NetworkPath(interface, generation)

        if len(direct) == 1:
            return NetworkPath(direct[0], generation)
        if direct:
            best_prefix = max(interface.network_prefix for interface in direct)
            best = tuple(interface for interface in direct if interface.network_prefix == best_prefix)
            best_priority = min(interface.priority for interface in best)
            preferred = tuple(interface for interface in best if interface.priority == best_priority)
            if len(preferred) == 1:
                return NetworkPath(preferred[0], generation)
            names = ", ".join(f"{interface.name} ({interface.address})" for interface in preferred)
            raise NetworkPathError(f"Destination {destination} has multiple equally preferred direct paths: {names}")

        if routed_source is not None:
            raise NetworkPathError(
                f"The operating system selected source {routed_source} for {destination}, "
                "but that address is not on an eligible interface"
            )
        raise NetworkPathError(f"No IPv4 network path is available for {destination}")

    @staticmethod
    def _select_explicit(candidates: tuple[IPv4Interface, ...], direct: tuple[IPv4Interface, ...]) -> IPv4Interface:
        if direct:
            best_prefix = max(interface.network_prefix for interface in direct)
            return next(interface for interface in direct if interface.network_prefix == best_prefix)
        return candidates[0]

    def _route_source(self, destination: str) -> str | None:
        route_socket = self._socket_factory(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            route_socket.connect((destination, 9))
            source = route_socket.getsockname()[0]
            return str(ipaddress.IPv4Address(source))
        except (OSError, ipaddress.AddressValueError):
            return None
        finally:
            route_socket.close()


path_manager = NetworkPathManager()


def source_address_for(destination: str, selected_interface: str | None = None) -> str:
    return path_manager.resolve(destination, selected_interface).source_address


__all__ = [
    "IPv4Interface",
    "NetworkPath",
    "NetworkPathError",
    "NetworkPathManager",
    "active_ipv4_interfaces",
    "invalidate_platform_preferences",
    "path_manager",
    "source_address_for",
]
