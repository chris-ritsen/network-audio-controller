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

    def test_callback_error_handled(self):
        def bad_callback(data, addr):
            raise ValueError("test error")

        protocol = DanteMulticastProtocol(bad_callback)
        # Should not propagate the exception
        protocol.datagram_received(b"\x00", ("224.0.0.231", 8702))

    def test_close_no_transport(self):
        protocol = DanteMulticastProtocol(lambda d, a: None)
        protocol.close()  # Should not raise
