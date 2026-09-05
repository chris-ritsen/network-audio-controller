from netaudio.dante.transport import DanteMulticastProtocol


class TestMulticastProtocol:
    def test_callback_invoked(self):
        received = []

        def callback(data, addr):
            received.append((data, addr))

        protocol = DanteMulticastProtocol(callback)
        protocol.datagram_received(b"\x00\x01\x02", ("224.0.0.231", 8702))

        assert len(received) == 1
        assert received[0][0] == b"\x00\x01\x02"
        assert received[0][1] == ("224.0.0.231", 8702)

    def test_callback_error_is_logged_and_later_packets_are_delivered(self, caplog):
        received = []

        def callback(data, addr):
            if data == b"bad":
                raise ValueError("test error")
            received.append((data, addr))

        protocol = DanteMulticastProtocol(callback)
        protocol.datagram_received(b"bad", ("192.0.2.1", 8702))
        protocol.datagram_received(b"good", ("192.0.2.1", 8702))

        assert received == [(b"good", ("192.0.2.1", 8702))]
        [record] = [record for record in caplog.records if record.levelname == "ERROR"]
        assert record.getMessage() == "Error in multicast callback"
        assert record.exc_info[0] is ValueError
        assert str(record.exc_info[1]) == "test error"

    def test_close_no_transport(self):
        protocol = DanteMulticastProtocol(lambda d, a: None)
        protocol.close()  # Should not raise
