from unittest.mock import MagicMock

from netaudio import core
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.services.notification import DanteNotificationService
from tests.protocol_test_fixtures import load_protocol_packet


MODEL_PACKET_FILENAMES = {
    1: "protocol_FFFF_message_00C1_id_1.bin",
    2: "protocol_FFFF_message_00C0_id_2.bin",
    3: "protocol_FFFF_message_00C1_id_3.bin",
    4: "protocol_FFFF_message_00C0_id_4.bin",
    5: "protocol_FFFF_message_0061_id_5.bin",
    6: "protocol_FFFF_message_0060_id_6.bin",
    7: "protocol_FFFF_message_0061_id_7.bin",
    8: "protocol_FFFF_message_0060_id_8.bin",
    10: "protocol_FFFF_message_00C0_id_10.bin",
    12: "protocol_FFFF_message_0060_id_12.bin",
}


def _packet(packet_identifier: int) -> bytes:
    return load_protocol_packet("model_refresh", MODEL_PACKET_FILENAMES[packet_identifier])


def test_model_query_builders_are_byte_identical_to_shipping_controller():
    commands = DanteDeviceCommands()

    for packet_identifier, device_mac_address in [(1, "000eddfd4e13"), (3, "001dc1081258")]:
        expected = _packet(packet_identifier)
        assert core.build_command({"command": "make_model", "mac": device_mac_address}) == expected
        assert commands.command_make_model(device_mac_address) == expected

    for packet_identifier, device_mac_address in [(5, "000eddfd4e13"), (7, "001dc1081258")]:
        expected = _packet(packet_identifier)
        assert core.build_command({"command": "dante_model", "mac": device_mac_address}) == expected
        assert commands.command_dante_model(device_mac_address) == expected


def test_make_model_parser_preserves_all_four_version_octets_and_unmapped_field():
    assert core.parse_response("make_model", _packet(2)) == {
        "manufacturer": "Shure Inc.",
        "manufacturer_field_hexadecimal": _packet(2)[76:204].hex(),
        "unmapped_field_at_byte_offset_74": 0x0011,
        "product_name": "AD4D",
        "product_version": "0.0.0.1",
        "product_version_components": [0, 0, 0, 1],
    }
    assert core.parse_response("make_model", _packet(4)) == {
        "manufacturer": "Digigram",
        "manufacturer_field_hexadecimal": _packet(4)[76:204].hex(),
        "unmapped_field_at_byte_offset_74": 0,
        "product_name": "LX-DANTE",
        "product_version": "1.0.0.0",
        "product_version_components": [1, 0, 0, 0],
    }
    assert core.parse_response("make_model", _packet(10)) == {
        "manufacturer": "Ferrofish GmbH",
        "manufacturer_field_hexadecimal": _packet(10)[76:204].hex(),
        "unmapped_field_at_byte_offset_74": 1,
        "product_name": "A32 Dante AD/DA Converter",
        "product_version": "1.0.0.0",
        "product_version_components": [1, 0, 0, 0],
    }


def test_board_model_parser_matches_physical_and_authentic_virtual_devices():
    assert core.parse_response("dante_model", _packet(6)) == {
        "board_codename": "Bklyn2",
        "board_name": "Brooklyn II",
    }
    assert core.parse_response("dante_model", _packet(8)) == {
        "board_codename": "PCIe",
        "board_name": "Dante PCIe IF",
    }
    assert core.parse_response("dante_model", _packet(12)) == {
        "board_codename": "Bklyn2",
        "board_name": "Brooklyn II",
    }


def test_notification_service_applies_and_serializes_controller_visible_identity():
    device_ip_address = "10.0.2.15"
    device = DanteDevice(server_name="virtual-a32.local.")
    device.name = "virtual-a32"
    device.ipv4 = device_ip_address
    service = DanteNotificationService(
        dispatcher=MagicMock(),
        device_lookup=lambda ip_address: device if ip_address == device_ip_address else None,
    )

    service._on_packet(_packet(10), (device_ip_address, 8702))
    service._on_packet(_packet(12), (device_ip_address, 8702))

    assert device.manufacturer == "Ferrofish GmbH"
    assert device.dante_model == "A32 Dante AD/DA Converter"
    assert device.product_version == "1.0.0.0"
    assert device.dante_model_id == "Bklyn2"
    assert device.board_name == "Brooklyn II"
    serialized = DanteDeviceSerializer.to_json(device)
    assert serialized["manufacturer"] == "Ferrofish GmbH"
    assert serialized["dante_model"] == "A32 Dante AD/DA Converter"
    assert serialized["product_version"] == "1.0.0.0"
    assert serialized["dante_model_id"] == "Bklyn2"
    assert serialized["board_name"] == "Brooklyn II"
