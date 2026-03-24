import asyncio
import struct

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
        packet = b'\x27\xFF\x00\x0A' + struct.pack(">H", 0x5678) + b'\x10\x00'
        assert DanteUnicastService._extract_transaction_id(packet) == 0x5678

    def test_extract_transaction_id_short(self):
        assert DanteUnicastService._extract_transaction_id(b'\x00') == 0

    @pytest.mark.asyncio
    async def test_request_not_started(self):
        service = DanteUnicastService()
        result = await service.request(b'\x00', "192.168.1.1", 4440)
        assert result is None

    def test_send_not_started(self):
        service = DanteUnicastService()
        # Should not raise when protocol is None
        service.send(b'\x00', "192.168.1.1", 4440)

    @pytest.mark.asyncio
    async def test_start_stop(self):
        service = DanteUnicastService()
        await service.start()
        assert service._protocol is not None
        assert service._protocol.transport is not None

        await service.stop()
        assert service._protocol is None


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

        await service.stop()
        assert service._protocol is None
