from __future__ import annotations

import asyncio
import inspect
import logging
import socket
from ipaddress import IPv4Address
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.sap import (
    SAP_MULTICAST_ADDRESS,
    SAP_PORT,
    SapFlowInventory,
    SapInventoryChange,
    SapParseError,
)
from netaudio.network_changes import NetworkChangeMonitor, network_change_monitor
from netaudio.network_path import active_ipv4_interfaces

logger = logging.getLogger("netaudio")

SAP_EXPIRY_CHECK_SECONDS = 30.0


@dataclass(frozen=True, order=True)
class SapInterface:
    name: str
    address: str


def active_sap_interfaces(selected_interface: str | None = None) -> tuple[SapInterface, ...]:
    return tuple(
        sorted(
            SapInterface(interface.name, interface.address) for interface in active_ipv4_interfaces(selected_interface)
        )
    )


class _SapDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, service: SapDiscoveryService, interface: SapInterface) -> None:
        self.service = service
        self.interface = interface

    def datagram_received(self, data: bytes, address) -> None:
        self.service._datagram_received(data, address, self.interface)

    def error_received(self, error: Exception) -> None:
        logger.debug(f"SAP listener error on {self.interface.name} ({self.interface.address}): {error}")


class SapDiscoveryService:
    def __init__(
        self,
        inventory: SapFlowInventory | None = None,
        *,
        interface_name: str | None = None,
        multicast_groups: Iterable[str] = (SAP_MULTICAST_ADDRESS, "224.0.0.56"),
        interface_provider: Callable[[], Iterable[SapInterface]] | None = None,
        socket_factory: Callable[..., socket.socket] = socket.socket,
        endpoint_factory: Callable[..., Awaitable[tuple[asyncio.DatagramTransport, asyncio.DatagramProtocol]]]
        | None = None,
        on_change: Callable[[SapInventoryChange], object] | None = None,
        expiry_check_seconds: float = SAP_EXPIRY_CHECK_SECONDS,
        network_changes: NetworkChangeMonitor = network_change_monitor,
    ) -> None:
        if expiry_check_seconds <= 0:
            raise ValueError("SAP expiry check interval must be positive")
        self.inventory = inventory or SapFlowInventory()
        self.multicast_groups = tuple(dict.fromkeys(str(IPv4Address(group)) for group in multicast_groups))
        if not self.multicast_groups or any(not IPv4Address(group).is_multicast for group in self.multicast_groups):
            raise ValueError("SAP groups must be nonempty IPv4 multicast addresses")
        self.listener_errors: dict[str, str] = {}
        self.received_count = 0
        self.rejected_count = 0
        self.accepted_count = 0
        self.latest_error: str | None = None
        self._interface_provider = interface_provider or (lambda: active_sap_interfaces(interface_name))
        self._socket_factory = socket_factory
        self._endpoint_factory = endpoint_factory
        self._on_change = on_change
        self._expiry_check_seconds = expiry_check_seconds
        self._network_changes = network_changes
        self._transports: dict[SapInterface, asyncio.DatagramTransport] = {}
        self._refresh_lock = DeferredAsyncioLock()
        self._refresh_requested = False
        self._refresh_task: asyncio.Task | None = None
        self._unsubscribe_network_changes: Callable[[], None] | None = None
        self._expiry_task: asyncio.Task | None = None
        self._callback_tasks: set[asyncio.Task] = set()
        self._started = False

    @property
    def listening_interfaces(self) -> tuple[SapInterface, ...]:
        return tuple(sorted(self._transports))

    def set_change_handler(self, on_change: Callable[[SapInventoryChange], object] | None) -> None:
        self._on_change = on_change

    async def _create_endpoint(self, interface: SapInterface):
        multicast_socket = self._socket_factory(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                try:
                    multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except OSError:
                    pass
            multicast_socket.bind(("", SAP_PORT))
            joined = 0
            for group in self.multicast_groups:
                key = f"{interface.name}/{interface.address}/{group}"
                membership = socket.inet_aton(group) + socket.inet_aton(interface.address)
                try:
                    multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
                except OSError as exception:
                    self.listener_errors[key] = str(exception)
                else:
                    self.listener_errors.pop(key, None)
                    joined += 1

            if joined == 0:
                raise OSError("no SAP multicast membership could be joined")
            multicast_socket.setblocking(False)

            def protocol_factory():
                return _SapDatagramProtocol(self, interface)

            if self._endpoint_factory is not None:
                return await self._endpoint_factory(protocol_factory, sock=multicast_socket)
            loop = asyncio.get_running_loop()
            return await loop.create_datagram_endpoint(protocol_factory, sock=multicast_socket)
        except BaseException:
            multicast_socket.close()
            raise

    async def refresh_interfaces(self) -> None:
        async with self._refresh_lock:
            desired = set(self._interface_provider())
            for interface in tuple(self._transports):
                if interface not in desired:
                    self._transports.pop(interface).close()
            for interface in sorted(desired - self._transports.keys()):
                try:
                    transport, _ = await self._create_endpoint(interface)
                except OSError as exception:
                    logger.warning(f"Could not listen for SAP on {interface.name} ({interface.address}): {exception}")
                    continue
                self._transports[interface] = transport

    def _emit(self, change: SapInventoryChange) -> None:
        if self._on_change is None:
            return
        try:
            result = self._on_change(change)
        except Exception:
            logger.exception("SAP inventory callback failed")
            return
        if inspect.isawaitable(result):
            task = asyncio.create_task(result)
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_done)

    def _callback_done(self, task: asyncio.Task) -> None:
        self._callback_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception("SAP inventory callback failed")

    def _datagram_received(self, data: bytes, address, interface: SapInterface) -> None:
        self.received_count += 1
        try:
            packet_source_ipv4, packet_source_port = address[:2]
            change = self.inventory.ingest(
                data,
                announcement_interface=interface.name,
                packet_source_ipv4=packet_source_ipv4,
                packet_source_port=packet_source_port,
            )
        except (SapParseError, ValueError) as exception:
            self.rejected_count += 1
            self.latest_error = str(exception)
            logger.debug(f"Ignored invalid SAP packet on {interface.name}: {exception}")
            return
        self.accepted_count += 1
        if change is not None:
            self._emit(change)

    def diagnostics(self) -> dict:
        return {
            "groups": self.multicast_groups,
            "interfaces": [{"name": item.name, "address": item.address} for item in self.listening_interfaces],
            "listener_errors": dict(self.listener_errors),
            "received": self.received_count,
            "accepted": self.accepted_count,
            "rejected": self.rejected_count,
            "latest_error": self.latest_error,
            "source_count": len(self.inventory.flows()),
        }

    def _interfaces_changed(self) -> None:
        if not self._started:
            return
        self._refresh_requested = True
        if self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._refresh_after_changes(), name="netaudio-sap-refresh")

    async def _refresh_after_changes(self) -> None:
        try:
            while self._refresh_requested:
                self._refresh_requested = False
                try:
                    await self.refresh_interfaces()
                except Exception:
                    logger.exception("Could not refresh SAP multicast interfaces")
        finally:
            self._refresh_task = None
            if self._refresh_requested and self._started:
                self._interfaces_changed()

    async def _expiry_loop(self) -> None:
        while True:
            await asyncio.sleep(self._expiry_check_seconds)
            for change in self.inventory.expire():
                self._emit(change)

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        try:
            self._unsubscribe_network_changes = self._network_changes.subscribe(self._interfaces_changed)
            await self.refresh_interfaces()
            self._expiry_task = asyncio.create_task(self._expiry_loop(), name="netaudio-sap-expiry")
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        self._started = False
        unsubscribe = self._unsubscribe_network_changes
        self._unsubscribe_network_changes = None
        if unsubscribe is not None:
            unsubscribe()
        refresh_task = self._refresh_task
        self._refresh_task = None
        self._refresh_requested = False
        if refresh_task is not None:
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)
        expiry_task = self._expiry_task
        self._expiry_task = None
        if expiry_task is not None:
            expiry_task.cancel()
            await asyncio.gather(expiry_task, return_exceptions=True)
        for transport in self._transports.values():
            transport.close()
        self._transports.clear()
        callback_tasks = tuple(self._callback_tasks)
        for task in callback_tasks:
            task.cancel()
        if callback_tasks:
            await asyncio.gather(*callback_tasks, return_exceptions=True)
        self._callback_tasks.clear()


__all__ = ["SAP_EXPIRY_CHECK_SECONDS", "SapDiscoveryService", "SapInterface", "active_sap_interfaces"]
