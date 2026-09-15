import asyncio
import socket
from unittest.mock import MagicMock

import pytest

from netaudio.dante.service import DanteMulticastService
from netaudio.network_path import IPv4Interface


class FakeSocket:
    def __init__(self, *arguments):
        self.arguments = arguments
        self.options = []
        self.bound = None
        self.blocking = None
        self.closed = False

    def setsockopt(self, *arguments):
        self.options.append(arguments)

    def bind(self, address):
        self.bound = address

    def setblocking(self, value):
        self.blocking = value

    def close(self):
        self.closed = True


class FakeTransport:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeNetworkChanges:
    def __init__(self):
        self.callback = None
        self.unsubscribed = False

    def subscribe(self, callback):
        self.callback = callback

        def unsubscribe():
            self.callback = None
            self.unsubscribed = True

        return unsubscribe

    def emit(self):
        assert self.callback is not None
        self.callback()


async def deliver_network_change(changes):
    changes.emit()
    await asyncio.sleep(0)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_service_tracks_every_active_interface_and_recovers_changes():
    interfaces = [
        IPv4Interface(1, "eth0", "192.0.2.10", 24),
        IPv4Interface(2, "eth1", "198.51.100.20", 24),
    ]
    sockets = []
    endpoints = []
    changes = FakeNetworkChanges()

    def socket_factory(*arguments):
        multicast_socket = FakeSocket(*arguments)
        sockets.append(multicast_socket)
        return multicast_socket

    async def endpoint_factory(protocol_factory, *, sock):
        transport = FakeTransport()
        protocol = protocol_factory()
        protocol.connection_made(transport)
        endpoints.append((transport, protocol, sock))
        return transport, protocol

    service = DanteMulticastService(
        "224.0.0.231",
        8702,
        interface_provider=lambda: interfaces,
        socket_factory=socket_factory,
        endpoint_factory=endpoint_factory,
        network_changes=changes,
    )
    service._on_packet = MagicMock()

    await service.start()
    try:
        assert service.listening_interfaces == tuple(interfaces)
        assert len(sockets) == 2
        for interface, multicast_socket in zip(interfaces, sockets):
            assert multicast_socket.arguments == (socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            assert multicast_socket.bound == ("", 8702)
            assert multicast_socket.blocking is False
            membership = socket.inet_aton("224.0.0.231") + socket.inet_aton(interface.address)
            assert (socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership) in multicast_socket.options

        endpoints[1][1].datagram_received(b"packet", ("198.51.100.30", 8702))
        service._on_packet.assert_called_once_with(b"packet", ("198.51.100.30", 8702))

        removed_transport = endpoints[0][0]
        interfaces.pop(0)
        await deliver_network_change(changes)
        assert removed_transport.closed is True
        assert service.listening_interfaces == (IPv4Interface(2, "eth1", "198.51.100.20", 24),)

        interfaces.clear()
        await deliver_network_change(changes)
        assert service.listening_interfaces == ()

        interfaces.append(IPv4Interface(1, "eth0", "192.0.2.11", 24))
        await deliver_network_change(changes)
        assert service.listening_interfaces == (IPv4Interface(1, "eth0", "192.0.2.11", 24),)
    finally:
        await service.stop()

    assert all(transport.closed for transport, _, _ in endpoints)
    assert changes.unsubscribed is True


@pytest.mark.asyncio
async def test_service_keeps_other_interfaces_when_one_membership_fails():
    interfaces = [
        IPv4Interface(1, "bad", "192.0.2.10", 24),
        IPv4Interface(2, "good", "198.51.100.20", 24),
    ]
    sockets = []

    def socket_factory(*arguments):
        multicast_socket = FakeSocket(*arguments)
        if not sockets:
            multicast_socket.setsockopt = MagicMock(side_effect=OSError("interface disappeared"))
        sockets.append(multicast_socket)
        return multicast_socket

    async def endpoint_factory(protocol_factory, *, sock):
        protocol = protocol_factory()
        transport = FakeTransport()
        protocol.connection_made(transport)
        return transport, protocol

    service = DanteMulticastService(
        "224.0.0.231",
        8702,
        interface_provider=lambda: interfaces,
        socket_factory=socket_factory,
        endpoint_factory=endpoint_factory,
        network_changes=FakeNetworkChanges(),
    )

    await service.start()
    try:
        assert sockets[0].closed is True
        assert service.listening_interfaces == (IPv4Interface(2, "good", "198.51.100.20", 24),)
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_with_no_interfaces_waits_without_opening_an_unspecified_membership():
    socket_factory = MagicMock()
    service = DanteMulticastService(
        "224.0.0.231",
        8702,
        interface_provider=lambda: (),
        socket_factory=socket_factory,
        network_changes=FakeNetworkChanges(),
    )

    await service.start()
    try:
        assert service.listening_interfaces == ()
        socket_factory.assert_not_called()
    finally:
        await service.stop()
