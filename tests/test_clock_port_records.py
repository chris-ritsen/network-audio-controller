import hashlib
from pathlib import Path

from netaudio import core
from tests.status_test_support import application_with_device, receive_packets


CLOCK_STATUS_PACKET = bytes.fromhex(
    "ffff00cc06c60000001dc10812580000417564696e6174650724002000000000000300030000007bfffff9bf001dc1081258"
    "0000001dc119245c0000001dc119245c000000020038000900000003000002b4000000030d40000000020000000000000000"
    "000000000000000000000000005c00040005000000640010000000010102010000000002000900070000000202020100000000"
    "0200060007000200030102010000000003000300070001000401020200000000020003000300010005020202000000000200"
    "030003"
)


def test_clock_status_parser_preserves_every_port_record():
    parsed = core.parse_response("ptp_clock_status", CLOCK_STATUS_PACKET)

    assert parsed["clock_port_state_code"] == 9
    assert parsed["clock_role"] == "Follower"
    assert parsed["clock_port_records"] == [
        {
            "record_flags": 0,
            "link_down": False,
            "record_number": 1,
            "ptp_version": 1,
            "record_format_code": 2,
            "transport_path_code": 1,
            "transport_path": "multicast",
            "reserved_byte": 0,
            "network_interface_index": 2,
            "state_code": 9,
            "role": "Follower",
            "status_flags": 7,
        },
        {
            "record_flags": 0,
            "link_down": False,
            "record_number": 2,
            "ptp_version": 2,
            "record_format_code": 2,
            "transport_path_code": 1,
            "transport_path": "multicast",
            "reserved_byte": 0,
            "network_interface_index": 2,
            "state_code": 6,
            "role": "Leader",
            "status_flags": 7,
        },
        {
            "record_flags": 2,
            "link_down": True,
            "record_number": 3,
            "ptp_version": 1,
            "record_format_code": 2,
            "transport_path_code": 1,
            "transport_path": "multicast",
            "reserved_byte": 0,
            "network_interface_index": 3,
            "state_code": 3,
            "role": None,
            "status_flags": 7,
        },
        {
            "record_flags": 1,
            "link_down": False,
            "record_number": 4,
            "ptp_version": 1,
            "record_format_code": 2,
            "transport_path_code": 2,
            "transport_path": "unicast",
            "reserved_byte": 0,
            "network_interface_index": 2,
            "state_code": 3,
            "role": None,
            "status_flags": 3,
        },
        {
            "record_flags": 1,
            "link_down": False,
            "record_number": 5,
            "ptp_version": 2,
            "record_format_code": 2,
            "transport_path_code": 2,
            "transport_path": "unicast",
            "reserved_byte": 0,
            "network_interface_index": 2,
            "state_code": 3,
            "role": None,
            "status_flags": 3,
        },
    ]


def test_notification_and_device_serialization_preserve_every_clock_port_record():
    expected = core.parse_response("ptp_clock_status", CLOCK_STATUS_PACKET)["clock_port_records"]
    application, device = application_with_device("clock-port-test", "192.168.1.108", name="clock-port-test")

    receive_packets(application, [CLOCK_STATUS_PACKET], ("192.168.1.108", 1034))

    assert device.clock_port_records == expected
    assert device.to_json()["clock_port_records"] == expected


def test_avio_capture_updates_device_clock_ports():
    packet = bytes.fromhex(
        (Path(__file__).parent / "fixtures/clock_status/avio-aes3.hex").read_text()
    )
    assert hashlib.sha256(packet).hexdigest() == "321d937b413ace35bab9e5bd08d9ea53a64e2e334c47bcbf355266433a686b0d"
    application, device = application_with_device("avio-aes3-1", "192.168.1.18")
    receive_packets(application, [packet], ("192.168.1.18", 8700))
    ports = device.to_json()["clock_port_records"]
    assert [(port["ptp_version"], port["transport_path"], port["state_code"]) for port in ports] == [
        (1, "multicast", 9),
        (2, "unicast", 3),
        (2, "multicast", 3),
    ]
