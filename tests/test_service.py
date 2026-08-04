import asyncio
import socket
import struct
from unittest.mock import MagicMock

import pytest

from netaudio.dante.service import DanteMulticastService, DanteUnicastService


class TestUnicastService:
    def test_initial_state(self):
        service = DanteUnicastService()
        assert service._protocol is None
        assert service._transaction_counter == 0

    def test_next_transaction_id(self):
        service = DanteUnicastService()
        assert service._next_transaction_id() == 1
        assert service._next_transaction_id() == 2
        assert service._next_transaction_id() == 3

    def test_next_transaction_id_wraps(self):
        service = DanteUnicastService()
        service._transaction_counter = 0xFFFE
        assert service._next_transaction_id() == 0xFFFF
        assert service._next_transaction_id() == 0  # wraps

    def test_extract_transaction_id(self):
        packet = b"\x27\xff\x00\x0a" + struct.pack(">H", 0x5678) + b"\x10\x00"
        assert DanteUnicastService._extract_transaction_id(packet) == 0x5678

    def test_extract_transaction_id_short(self):
        assert DanteUnicastService._extract_transaction_id(b"\x00") == 0

    @pytest.mark.asyncio
    async def test_request_not_started(self):
        service = DanteUnicastService()
        result = await service.request(b"\x00", "192.168.1.1", 4440)
        assert result is None

    def test_send_not_started(self):
        service = DanteUnicastService()
        # Should not raise when protocol is None
        service.send(b"\x00", "192.168.1.1", 4440)

    def test_send_persists_fire_and_forget_request(self):
        packet_store = MagicMock()
        protocol = MagicMock()
        protocol.transport.get_extra_info.return_value = ("192.168.1.10", 54321)
        service = DanteUnicastService(packet_store=packet_store)
        service._protocol = protocol
        service.session_id = 18
        packet = b"\x27\xff\x00\x13"

        service.send(packet, "192.168.1.108", 8702)

        packet_store.store_packet.assert_called_once_with(
            payload=packet,
            source_type="netaudio_request",
            device_ip="192.168.1.108",
            src_ip="192.168.1.10",
            src_port=54321,
            dst_ip="192.168.1.108",
            dst_port=8702,
            direction="request",
            session_id=18,
        )
        protocol.send_fire_and_forget.assert_called_once_with(packet, ("192.168.1.108", 8702))

    @pytest.mark.asyncio
    async def test_start_stop(self):
        service = DanteUnicastService()
        await service.start()
        assert service._protocol is not None
        assert service._protocol.transport is not None

        await service.stop()
        assert service._protocol is None

    @pytest.mark.asyncio
    async def test_requested_source_port_falls_back_to_ephemeral_when_occupied(self):
        occupied_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        occupied_socket.bind(("0.0.0.0", 0))
        occupied_port = occupied_socket.getsockname()[1]
        service = DanteUnicastService(local_port=occupied_port, fallback_to_ephemeral=True)

        try:
            await service.start()
            assert service.local_port is not None
            assert service.local_port != occupied_port
        finally:
            await service.stop()
            occupied_socket.close()


class TestMulticastService:
    def test_initial_state(self):
        service = DanteMulticastService("224.0.0.231", 8702)
        assert service._multicast_group == "224.0.0.231"
        assert service._multicast_port == 8702
        assert service._protocol is None

    @pytest.mark.asyncio
    async def test_start_stop(self):
        service = DanteMulticastService("224.0.0.231", 8702)
        await service.start()
        assert service._protocol is not None
        assert service._protocol.transport is not None
        assert service._protocol.transport.get_extra_info("sockname") == ("224.0.0.231", 8702)

        await service.stop()
        assert service._protocol is None
