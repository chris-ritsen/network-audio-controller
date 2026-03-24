import asyncio
import struct

import pytest

from netaudio.dante.transport import DanteMulticastProtocol, DanteUnicastProtocol


class TestUnicastProtocol:
    def test_extract_transaction_id(self):
        # Build a minimal packet: 4 bytes prefix + 2 bytes transaction_id
        packet = b'\x27\xFF\x00\x0A' + struct.pack(">H", 0x1234) + b'\x10\x00'
        assert DanteUnicastProtocol._extract_transaction_id(packet) == 0x1234

    def test_extract_transaction_id_short_packet(self):
        assert DanteUnicastProtocol._extract_transaction_id(b'\x00\x01') is None

    def test_extract_transaction_id_empty(self):
        assert DanteUnicastProtocol._extract_transaction_id(b'') is None

    def test_initial_state(self):
        protocol = DanteUnicastProtocol()
        assert protocol.transport is None
        assert protocol._pending == {}

    def test_datagram_received_resolves_future(self):
        protocol = DanteUnicastProtocol()
        loop = asyncio.new_event_loop()

        future = loop.create_future()
        key = ("192.168.1.1", 0x0042)
        protocol._pending[key] = future

        # Build a response with transaction_id = 0x0042
        response = b'\x27\xFF\x00\x10' + struct.pack(">H", 0x0042) + b'\x10\x02\x00\x01'
        protocol.datagram_received(response, ("192.168.1.1", 4440))

        assert future.done()
        assert future.result() == response

        loop.close()

    def test_datagram_received_unmatched(self):
        protocol = DanteUnicastProtocol()

        # No pending futures - should not raise
        response = b'\x27\xFF\x00\x10' + struct.pack(">H", 0x0042) + b'\x10\x02\x00\x01'
        protocol.datagram_received(response, ("192.168.1.1", 4440))

    def test_connection_lost_cancels_futures(self):
        protocol = DanteUnicastProtocol()
        loop = asyncio.new_event_loop()

        future = loop.create_future()
        protocol._pending[("192.168.1.1", 0x01)] = future

        protocol.connection_lost(None)

        assert future.cancelled()
        assert protocol._pending == {}

        loop.close()

    def test_send_fire_and_forget_no_transport(self):
        protocol = DanteUnicastProtocol()
        # Should not raise when transport is None
        protocol.send_fire_and_forget(b'\x00', ("192.168.1.1", 4440))

    def test_close_no_transport(self):
        protocol = DanteUnicastProtocol()
        # Should not raise when transport is None
        protocol.close()


class TestMulticastProtocol:
    def test_callback_invoked(self):
        received = []

        def callback(data, addr):
            received.append((data, addr))

        protocol = DanteMulticastProtocol(callback)
        protocol.datagram_received(b'\x00\x01\x02', ("224.0.0.231", 8702))

        assert len(received) == 1
        assert received[0][0] == b'\x00\x01\x02'
        assert received[0][1] == ("224.0.0.231", 8702)

    def test_callback_error_handled(self):
        def bad_callback(data, addr):
            raise ValueError("test error")

        protocol = DanteMulticastProtocol(bad_callback)
        # Should not propagate the exception
        protocol.datagram_received(b'\x00', ("224.0.0.231", 8702))

    def test_close_no_transport(self):
        protocol = DanteMulticastProtocol(lambda d, a: None)
        protocol.close()  # Should not raise
