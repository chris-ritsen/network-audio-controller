import pytest

from netaudio import core
from netaudio.dante.device import DanteDevice
from tests.protocol_test_fixtures import load_protocol_packet


@pytest.mark.parametrize(
    ("media_type_code", "expected_label"),
    [
        (3, "audio"),
        (4, "video"),
        (5, "ancillary"),
    ],
)
def test_modern_arc_channel_media_type_labels(media_type_code, expected_label):
    packet = bytearray(load_protocol_packet("receiver_channel_status", "protocol_2809_opcode_3400_id_28729.bin"))
    packet[74:76] = media_type_code.to_bytes(2, "big")
    page = core.parse_response("modern_arc_receiver_channel_status_page", bytes(packet))
    assert page["records"][0]["media_type"] == expected_label

    device = DanteDevice("receiver.local.")
    device.apply_receiver_channel_inventory(page)
    channel = device.rx_channels[1]

    assert channel.media_type_code == media_type_code
    assert channel.media_type == expected_label
    assert device.media_types == [expected_label]
