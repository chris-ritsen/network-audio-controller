import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock

import pytest

from netaudio.dante.service import DanteMulticastService


class TestMulticastService:
    def test_initial_state(self):
        service = DanteMulticastService("224.0.0.231", 8702)
        assert service._multicast_group == "224.0.0.231"
        assert service._multicast_port == 8702
        assert service._protocol is None

    @pytest.mark.asyncio
    async def test_start_stop(self, monkeypatch):
        sock = MagicMock()
        transport = MagicMock()

        async def open_endpoint(factory, *, sock):
            protocol = factory()
            protocol.connection_made(transport)
            return transport, protocol

        endpoint = AsyncMock(side_effect=open_endpoint)
        socket_factory = MagicMock(return_value=sock)
        monkeypatch.setattr(socket, "socket", socket_factory)
        monkeypatch.setattr(asyncio.get_running_loop(), "create_datagram_endpoint", endpoint)
        service = DanteMulticastService("224.0.0.231", 8702, interface_ip="192.0.2.10")
        service._on_packet = MagicMock()
        await service.start()
        try:
            socket_factory.assert_called_once_with(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.bind.assert_called_once_with(("0.0.0.0", 8702))
            sock.setsockopt.assert_any_call(
                socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, b"\xe0\x00\x00\xe7\xc0\x00\x02\x0a"
            )
            endpoint.assert_awaited_once()
            assert endpoint.call_args.kwargs == {"sock": sock}
            service._protocol.datagram_received(b"packet", ("192.0.2.20", 8702))
            service._on_packet.assert_called_once_with(b"packet", ("192.0.2.20", 8702))
        finally:
            await service.stop()

        transport.close.assert_called_once()
        assert service._protocol is None
        await service.stop()
        transport.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_failed_endpoint_creation_closes_socket(self, monkeypatch):
        sock = MagicMock()
        monkeypatch.setattr(socket, "socket", MagicMock(return_value=sock))
        monkeypatch.setattr(
            asyncio.get_running_loop(), "create_datagram_endpoint", AsyncMock(side_effect=OSError("bind failed"))
        )
        service = DanteMulticastService("224.0.0.231", 8702, interface_ip="192.0.2.10")

        with pytest.raises(OSError, match="bind failed"):
            await service.start()

        sock.close.assert_called_once()
        assert service._protocol is None
