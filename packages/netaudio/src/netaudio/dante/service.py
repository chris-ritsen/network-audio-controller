from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import sys
from collections.abc import Awaitable, Callable, Iterable

from netaudio.asynchronous_primitives import DeferredAsyncioLock
from netaudio.dante.transport import DanteMulticastProtocol
from netaudio.network_changes import NetworkChangeMonitor, network_change_monitor
from netaudio.network_path import IPv4Interface, NetworkPathError, NetworkPathManager, path_manager

logger = logging.getLogger("netaudio")

LINUX_IP_MULTICAST_ALL = 49


class DanteMulticastService:
    def __init__(
        self,
        multicast_group: str,
        multicast_port: int,
        packet_store=None,
        interface_name: str | None = None,
        on_packet: Callable[[bytes, tuple[str, int]], None] | None = None,
        dissect: bool = False,
        *,
        network_paths: NetworkPathManager = path_manager,
        interface_provider: Callable[[], Iterable[IPv4Interface]] | None = None,
        socket_factory: Callable[..., socket.socket] = socket.socket,
        endpoint_factory: Callable[..., Awaitable[tuple[asyncio.DatagramTransport, DanteMulticastProtocol]]]
        | None = None,
        network_changes: NetworkChangeMonitor = network_change_monitor,
    ):
        self._multicast_group = multicast_group
        self._multicast_port = multicast_port
        self._packet_store = packet_store
        self._dissect = dissect
        self._session_id: int | None = None
        self._interface_name = interface_name
        self._packet_handler = on_packet
        self._network_paths = network_paths
        self._interface_provider = interface_provider
        self._socket_factory = socket_factory
        self._endpoint_factory = endpoint_factory
        self._network_changes = network_changes
        self._endpoints: dict[IPv4Interface, tuple[asyncio.DatagramTransport, DanteMulticastProtocol]] = {}
        self._refresh_lock = DeferredAsyncioLock()
        self._refresh_requested = False
        self._refresh_task: asyncio.Task | None = None
        self._unsubscribe_network_changes: Callable[[], None] | None = None
        self._started = False

    @property
    def session_id(self) -> int | None:
        return self._session_id

    @session_id.setter
    def session_id(self, value: int | None) -> None:
        self._session_id = value

    @property
    def listening_interfaces(self) -> tuple[IPv4Interface, ...]:
        return tuple(sorted(self._endpoints))

    def _desired_interfaces(self) -> tuple[IPv4Interface, ...]:
        if self._interface_provider is not None:
            return tuple(sorted(set(self._interface_provider())))
        try:
            interfaces = self._network_paths.interfaces(self._interface_name)
        except NetworkPathError as exception:
            logger.warning("Multicast listener is waiting for its configured interface: %s", exception)
            return ()
        if self._interface_name is not None:
            return interfaces
        return tuple(interface for interface in interfaces if not ipaddress.ip_address(interface.address).is_loopback)

    async def _create_endpoint(self, interface: IPv4Interface):
        multicast_socket = self._socket_factory(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if sys.platform.startswith("linux"):
                try:
                    multicast_socket.setsockopt(socket.IPPROTO_IP, LINUX_IP_MULTICAST_ALL, 0)
                except OSError:
                    pass
            if hasattr(socket, "SO_REUSEPORT"):
                try:
                    multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except OSError:
                    pass
            multicast_socket.bind(("", self._multicast_port))
            membership = socket.inet_aton(self._multicast_group) + socket.inet_aton(interface.address)
            multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
            multicast_socket.setblocking(False)

            def protocol_factory():
                return DanteMulticastProtocol(self._handle_packet)

            if self._endpoint_factory is not None:
                return await self._endpoint_factory(protocol_factory, sock=multicast_socket)
            loop = asyncio.get_running_loop()
            return await loop.create_datagram_endpoint(protocol_factory, sock=multicast_socket)
        except BaseException:
            multicast_socket.close()
            raise

    async def refresh_interfaces(self) -> None:
        async with self._refresh_lock:
            desired = set(self._desired_interfaces())
            for interface in tuple(self._endpoints):
                if interface not in desired:
                    transport, _ = self._endpoints.pop(interface)
                    transport.close()
                    logger.info("Multicast listener stopped on %s (%s)", interface.name, interface.address)
            for interface in sorted(desired - self._endpoints.keys()):
                try:
                    endpoint = await self._create_endpoint(interface)
                except OSError as exception:
                    logger.warning(
                        "Could not listen on %s (%s) for %s:%d: %s",
                        interface.name,
                        interface.address,
                        self._multicast_group,
                        self._multicast_port,
                        exception,
                    )
                    continue
                self._endpoints[interface] = endpoint
                logger.info(
                    "Multicast service started on %s:%d (%s, %s)",
                    self._multicast_group,
                    self._multicast_port,
                    interface.name,
                    interface.address,
                )

    def _interfaces_changed(self) -> None:
        if not self._started:
            return
        self._refresh_requested = True
        if self._refresh_task is None:
            self._refresh_task = asyncio.create_task(
                self._refresh_after_changes(), name=f"netaudio-multicast-refresh-{self._multicast_port}"
            )

    async def _refresh_after_changes(self) -> None:
        try:
            while self._refresh_requested:
                self._refresh_requested = False
                try:
                    await self.refresh_interfaces()
                except Exception:
                    logger.exception(
                        "Could not refresh multicast interfaces for %s:%d",
                        self._multicast_group,
                        self._multicast_port,
                    )
        finally:
            self._refresh_task = None
            if self._refresh_requested and self._started:
                self._interfaces_changed()

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        try:
            self._unsubscribe_network_changes = self._network_changes.subscribe(self._interfaces_changed)
            await self.refresh_interfaces()
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
        for transport, _ in self._endpoints.values():
            transport.close()
        self._endpoints.clear()

    def _on_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        pass

    def _handle_packet(self, data: bytes, addr: tuple[str, int]) -> None:
        if self._packet_handler is not None:
            self._packet_handler(data, addr)
        else:
            self._on_packet(data, addr)


__all__ = ["DanteMulticastService"]
