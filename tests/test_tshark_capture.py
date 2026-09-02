import struct
from types import SimpleNamespace

import pytest

from netaudio.dante.tshark_capture import TsharkCapture, _build_bpf_filter
from netaudio.dante.packet_store import PacketQuery, PacketStore


class TestBpfFilter:
    def test_default_filter(self):
        bpf = _build_bpf_filter()
        assert bpf == "udp"

    def test_filter_with_tcp(self):
        bpf = _build_bpf_filter(include_tcp=True)
        assert bpf == "udp or tcp"


class TestParseLine:
    @pytest.fixture
    def capture(self, tmp_path):
        # Create a minimal PacketStore mock
        class MockStore:
            def store_packet(self, record):
                return 1

        return TsharkCapture(packet_store=MockStore())

    def test_valid_line(self, capture):
        # Fake a tshark output line: epoch, src_ip, src_port, dst_ip, dst_port, hex_data
        # Build a minimal Dante packet
        pkt = struct.pack(">HHHH", 0x27FF, 8, 0x0042, 0x1002)
        hex_data = pkt.hex()

        line = f"1716000000.123456\t10.0.0.1\t1029\t10.0.0.2\t8800\t{hex_data}"
        result = capture._parse_line(line)

        assert result is not None
        assert result["src_ip"] == "10.0.0.1"
        assert result["src_port"] == 1029
        assert result["dst_ip"] == "10.0.0.2"
        assert result["dst_port"] == 8800
        assert result["direction"] == "request"
        assert result["device_ip"] == "10.0.0.2"
        assert result["payload"] == pkt

    def test_response_direction(self, capture):
        pkt = struct.pack(">HHHH", 0x27FF, 8, 0x0042, 0x1002)
        hex_data = pkt.hex()

        line = f"1716000000.0\t10.0.0.2\t8800\t10.0.0.1\t1029\t{hex_data}"
        result = capture._parse_line(line)

        assert result["direction"] == "response"
        assert result["device_ip"] == "10.0.0.2"

    def test_multicast_direction(self, capture):
        pkt = struct.pack(">HHHH", 0x27FF, 8, 0x0042, 0x1003)
        hex_data = pkt.hex()

        # Traffic on info port (8702) -- not a control port
        line = f"1716000000.0\t192.168.1.50\t8702\t224.0.0.231\t8702\t{hex_data}"
        result = capture._parse_line(line)

        assert result["direction"] is None
        assert result["device_ip"] == "192.168.1.50"

    def test_multicast_dst_overrides_control_port_src(self, capture):
        """Traffic from a control port (8700) to a multicast address should be multicast, not response."""
        pkt = struct.pack(">HHHH", 0x27FF, 8, 0x0042, 0x1003)
        hex_data = pkt.hex()

        line = f"1716000000.0\t192.168.1.36\t8700\t224.0.0.233\t8708\t{hex_data}"
        result = capture._parse_line(line)

        assert result["direction"] is None
        assert result["device_ip"] == "192.168.1.36"

    def test_colon_separated_hex(self, capture):
        pkt = struct.pack(">HHHH", 0x27FF, 8, 0x0042, 0x1002)
        hex_data = ":".join(f"{b:02x}" for b in pkt)

        line = f"1716000000.0\t10.0.0.1\t1029\t10.0.0.2\t8800\t{hex_data}"
        result = capture._parse_line(line)
        assert result is not None
        assert result["payload"] == pkt

    def test_short_line(self, capture):
        assert capture._parse_line("foo\tbar") is None

    def test_empty_hex(self, capture):
        assert capture._parse_line("1.0\t1.1.1.1\t80\t2.2.2.2\t8800\t") is None

    def test_bad_epoch(self, capture):
        line = "not_a_number\t10.0.0.1\t1029\t10.0.0.2\t8800\tDEADBEEF"
        assert capture._parse_line(line) is None


class TestBuildCommand:
    def test_command_structure(self, tmp_path):
        class MockStore:
            pass

        cap = TsharkCapture(packet_store=MockStore(), interface="eth0")
        cmd = cap._build_command()

        assert cmd[0].endswith("tshark")
        assert "-i" in cmd
        assert cmd[cmd.index("-i") + 1] == "eth0"
        assert "-T" in cmd
        assert "fields" in cmd
        assert "-l" in cmd
        assert "-e" in cmd


class TestReadPcapFrames:
    def test_imports_only_selected_udp_payloads(self, tmp_path, monkeypatch):
        capture_path = tmp_path / "capture.pcap"
        capture_path.write_bytes(b"pcap")
        request = struct.pack(">HHHH", 0x2729, 8, 0x0042, 0x3000)
        response = struct.pack(">HHHHH", 0x2729, 10, 0x0042, 0x3000, 0x0001)
        tshark_output = "\n".join(
            [
                f"10\t1716000000.0\t10.0.0.1\t50000\t10.0.0.2\t4440\t{request.hex()}",
                f"11\t1716000000.1\t10.0.0.2\t4440\t10.0.0.1\t50000\t{response.hex()}",
            ]
        )
        monkeypatch.setattr(TsharkCapture, "_find_tshark", classmethod(lambda cls: "/usr/bin/tshark"))
        monkeypatch.setattr(
            "netaudio.dante.tshark_capture.subprocess.run",
            lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=tshark_output, stderr=""),
        )

        store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
        session_id = store.start_session(name="pcap")
        imported = TsharkCapture.read_pcap_frames(
            packet_store=store,
            pcap_path=capture_path,
            frame_numbers=[11, 10, 11],
            device_ip="10.0.0.2",
            session_id=session_id,
            source_host="lab",
        )
        packets = store.get_session_packets(PacketQuery(session_id=session_id))
        store.close()

        assert [entry["frame_number"] for entry in imported] == [10, 11]
        assert [packet["direction"] for packet in packets] == ["request", "response"]
        assert [packet["payload"] for packet in packets] == [request, response]
        assert all(packet["source_type"] == "pcap_import" for packet in packets)

    def test_missing_selected_frame_does_not_partially_import(self, tmp_path, monkeypatch):
        capture_path = tmp_path / "capture.pcap"
        capture_path.write_bytes(b"pcap")
        request = struct.pack(">HHHH", 0x2729, 8, 0x0042, 0x3000)
        tshark_output = f"10\t1716000000.0\t10.0.0.1\t50000\t10.0.0.2\t4440\t{request.hex()}"
        monkeypatch.setattr(TsharkCapture, "_find_tshark", classmethod(lambda cls: "/usr/bin/tshark"))
        monkeypatch.setattr(
            "netaudio.dante.tshark_capture.subprocess.run",
            lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=tshark_output, stderr=""),
        )

        store = PacketStore(db_path=str(tmp_path / "capture.sqlite"))
        session_id = store.start_session(name="pcap")

        with pytest.raises(ValueError, match="11"):
            TsharkCapture.read_pcap_frames(
                packet_store=store,
                pcap_path=capture_path,
                frame_numbers=[10, 11],
                device_ip="10.0.0.2",
                session_id=session_id,
            )

        assert store.get_session_packet_count(session_id) == 0
        store.close()
