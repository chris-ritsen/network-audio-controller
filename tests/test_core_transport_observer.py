import socket
import threading

import pytest

from netaudio import core
from netaudio.dante.core_transport import CoreTransport


def test_observer_reads_large_native_capture_and_clears_previous_operation():
    observed = []
    transport = CoreTransport(
        observer=lambda payload, ip, port, direction: observed.append((payload, ip, port, direction))
    )
    # Synthetic loopback datagrams exercise capture buffering, not a device command.
    packet = b"\x27\xff\x05\x78\x00\x01" + bytes(1394)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        port = receiver.getsockname()[1]

        with core.CoreClient("127.0.0.1", local_ip="127.0.0.1") as client:

            def send(active):
                for _ in range(100):
                    active.request(packet, port, expect_response=False)
                    assert receiver.recv(65535) == packet

                return "sent"

            assert transport._call_and_observe(client, threading.Lock(), send) == "sent"
            assert observed == [(packet, "127.0.0.1", port, "request")] * 100
            assert len(client.get_wire_captures()) == 100
            observed.clear()
            transport._call_and_observe(client, threading.Lock(), lambda active: None)
            assert observed == []
            assert client.get_wire_captures() == []


def test_timeout_preserves_sent_capture_for_observer():
    observed = []
    transport = CoreTransport(observer=lambda *record: observed.append(record))
    packet = core.build_command({"command": "device_name", "message_id": 1})

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
        receiver.bind(("127.0.0.1", 0))
        port = receiver.getsockname()[1]

        with core.CoreClient("127.0.0.1", local_ip="127.0.0.1", timeout_ms=20, attempts=1) as client:
            with pytest.raises(core.NetaudioCoreError) as failure:
                transport._call_and_observe(client, threading.Lock(), lambda active: active.request(packet, port))

            assert failure.value.status == core.STATUS_TIMEOUT
            assert observed == [(packet, "127.0.0.1", port, "request")]


def test_operation_without_observer_requires_no_capture_interface():
    client = object()
    assert CoreTransport()._call_and_observe(client, threading.Lock(), lambda active: active) is client
