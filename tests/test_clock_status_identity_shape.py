from netaudio import core
from tests.status_test_support import application_with_device, receive_packets
from tests.test_clock_port_records import CLOCK_STATUS_PACKET


IDENTITY_FIELDS = ("ptpv1_device_uuid", "ptpv1_master_uuid", "ptpv1_grandmaster_uuid")


def test_clock_status_keeps_core_identity_bytes_while_device_fields_are_canonical():
    expected = core.parse_response("ptp_clock_status", CLOCK_STATUS_PACKET)
    application, device = application_with_device("clock-identity-test", "192.168.1.108")

    receive_packets(application, [CLOCK_STATUS_PACKET], ("192.168.1.108", 1034))

    assert device.ptpv1_device_uuid == "001dc1081258"
    assert device.ptpv1_master_uuid == "001dc119245c"
    assert device.ptpv1_grandmaster_uuid == "001dc119245c"
    for field in IDENTITY_FIELDS:
        assert device.clock_status[field] == expected[field]
        assert all(isinstance(byte, int) for byte in device.clock_status[field])
        assert device.to_json()["clock_status"][field] == expected[field]
