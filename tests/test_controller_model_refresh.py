import pytest

from netaudio import core
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_serializer import DanteDeviceSerializer
from tests.protocol_test_fixtures import load_protocol_packet
from tests.status_test_support import application_with_device, receive_packets


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


def test_make_model_parser_decodes_packed_versions_and_preserves_raw_record():
    expected = {
        2: ("Shure Inc.", "AD4D", "0.0.1", "11.0.0.17"),
        4: ("Digigram", "LX-DANTE", "1.0.0", None),
        10: ("Ferrofish GmbH", "A32 Dante AD/DA Converter", "1.0.0", "1.0.2.1"),
    }
    for identifier, (manufacturer, product, product_version, manufacturer_firmware) in expected.items():
        parsed = core.parse_response("make_model", _packet(identifier))
        assert parsed["manufacturer"] == manufacturer
        assert parsed["product_name"] == product
        assert parsed["product_version"] == product_version
        assert parsed["product_version_components"] == [int(part) for part in product_version.split(".")]
        assert parsed["manufacturer_firmware_version"] == manufacturer_firmware
        assert parsed["raw_record_hexadecimal"] == _packet(identifier)[0x18:].hex()


def test_board_model_parser_matches_physical_and_authentic_virtual_devices():
    expected = {
        6: ("Bklyn2", "Brooklyn II", 0x072E, 0x8E7CD4CB, 0, 0x1B, 0x41, 7, 7),
        8: ("PCIe", "Dante PCIe IF", 0x0724, 0x0E68D3C9, 0, 0x1B, 1, 3, 3),
        12: ("Bklyn2", "Brooklyn II", 0x0724, 0x8E78F65A, 0, 0x1B, 1, 3, 3),
    }
    fields = (
        "platform_model_identifier",
        "platform_model_name",
        "record_protocol_version",
        "primary_capabilities",
        "read_only_capabilities",
        "monitoring_capabilities",
        "secondary_capabilities",
        "domain_capability_values",
        "domain_capability_validity",
    )
    for packet_identifier, values in expected.items():
        parsed = core.parse_response("dante_model", _packet(packet_identifier))
        assert tuple(parsed[field] for field in fields) == values


def test_board_model_aes67_capability_bit_follows_the_pinned_mask():
    aes67_capability_mask = 0x04000000
    for packet_identifier in (6, 8, 12):
        parsed = core.parse_response("dante_model", _packet(packet_identifier))
        expected = bool(parsed["primary_capabilities"] & aes67_capability_mask)
        assert parsed["aes67_configuration_supported"] == expected
        assert expected is True

    truncated = _packet(12)[:0x34]
    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("dante_model", truncated)


def test_board_model_monitoring_capability_bits_follow_the_pinned_masks():
    for packet_identifier in (6, 8, 12):
        parsed = core.parse_response("dante_model", _packet(packet_identifier))
        assert parsed["detailed_metering_supported"] == bool(parsed["primary_capabilities"] & 0x00008000)
        assert parsed["monitoring_capabilities"] == 0x1B
        assert parsed["interface_statistics_supported"] is True
        assert parsed["clock_monitoring_supported"] is True
        assert parsed["per_channel_signal_presence_supported"] is False
        assert parsed["rx_flow_maximum_latency_monitoring_supported"] is True
        assert parsed["rx_flow_late_packet_monitoring_supported"] is True


def test_state_service_applies_and_serializes_controller_visible_identity():
    device_ip_address = "10.0.2.15"
    application, device = application_with_device("virtual-a32.local.", device_ip_address)

    receive_packets(application, [_packet(10), _packet(12)], (device_ip_address, 8702))

    assert device.manufacturer == "Ferrofish GmbH"
    assert device.product_name == "A32 Dante AD/DA Converter"
    assert device.product_version == "1.0.0"
    assert device.platform_model_identifier == "Bklyn2"
    assert device.platform_model_name == "Brooklyn II"
    assert device.platform_software_version == "4.0.8.2"
    assert device.platform_hardware_version == "4.0.2.7"
    assert device.platform_api_version == "4.0.3"
    assert device.rom_boot_version == "1.3.64"
    assert device.dante_model_record_protocol_version == 0x0724
    assert device.dante_model_primary_capabilities == 0x8E78F65A
    assert device.dante_model_read_only_capabilities == 0
    assert device.dante_model_monitoring_capabilities == 0x1B
    assert device.aes67_configuration_supported is True
    assert device.detailed_metering_supported is True
    assert device.per_channel_signal_presence_supported is False
    serialized = DanteDeviceSerializer.to_json(device)
    assert serialized["aes67_configuration_supported"] is True
    assert serialized["dante_model_record_protocol_version"] == 0x0724
    assert serialized["dante_model_primary_capabilities"] == 0x8E78F65A
    assert serialized["dante_model_monitoring_capabilities"] == 0x1B
    assert serialized["detailed_metering_supported"] is True
    assert serialized["per_channel_signal_presence_supported"] is False
    assert serialized["manufacturer"] == "Ferrofish GmbH"
    assert serialized["product_name"] == "A32 Dante AD/DA Converter"
    assert serialized["product_version"] == "1.0.0"
    assert serialized["platform_model_identifier"] == "Bklyn2"
    assert serialized["platform_model_name"] == "Brooklyn II"
    assert serialized["field_sources"]["platform_software_version"] == "conmon_platform_record"
    assert serialized["field_sources"]["product_version"] == "conmon_manufacturer_record"


def test_later_versions_response_overwrites_aes67_configuration_support_and_primary_capabilities():
    device_ip_address = "10.0.2.15"
    application, device = application_with_device("virtual-a32.local.", device_ip_address)

    supported = _packet(12)
    receive_packets(application, [supported], (device_ip_address, 8702))
    assert device.aes67_configuration_supported is True
    assert device.detailed_metering_supported is True
    assert device.dante_model_primary_capabilities == 0x8E78F65A

    cleared = bytearray(supported)
    cleared[0x34] &= ~0x04
    receive_packets(application, [bytes(cleared)], (device_ip_address, 8702))
    assert device.aes67_configuration_supported is False
    assert device.detailed_metering_supported is True
    assert device.dante_model_primary_capabilities == 0x8E78F65A & ~0x04000000

    detailed_cleared = bytearray(supported)
    detailed_cleared[0x36] &= ~0x80
    receive_packets(application, [bytes(detailed_cleared)], (device_ip_address, 8702))
    assert device.aes67_configuration_supported is True
    assert device.detailed_metering_supported is False
    assert device.dante_model_primary_capabilities == 0x8E78F65A & ~0x00008000


def test_later_conmon_records_refresh_canonical_versions_without_erasing_other_namespaces():
    device_ip_address = "10.0.2.15"
    application, device = application_with_device("virtual-a32.local.", device_ip_address)
    receive_packets(application, [_packet(10), _packet(12)], (device_ip_address, 8702))
    device.cmc_server_version = "dns-service"
    device.ddm_dante_version = "ddm-platform"

    platform = bytearray(_packet(12))
    platform[0x20:0x24] = (0x0506_0007).to_bytes(4, "big")
    manufacturer = bytearray(_packet(10))
    manufacturer[0x14C:0x150] = (0x0203_0004).to_bytes(4, "big")
    receive_packets(application, [bytes(platform), bytes(manufacturer)], (device_ip_address, 8702))

    assert device.platform_software_version == "5.6.7.2"
    assert device.product_version == "2.3.4"
    assert device.cmc_server_version == "dns-service"
    assert device.ddm_dante_version == "ddm-platform"
    assert device.field_sources["platform_software_version"] == "conmon_platform_record"
    assert device.field_sources["product_version"] == "conmon_manufacturer_record"


def test_malformed_versions_response_leaves_existing_capability_unchanged():
    device_ip_address = "10.0.2.15"
    application, device = application_with_device("virtual-a32.local.", device_ip_address)

    receive_packets(application, [_packet(12)], (device_ip_address, 8702))
    assert device.aes67_configuration_supported is True
    assert device.detailed_metering_supported is True

    receive_packets(application, [_packet(12)[:0x34]], (device_ip_address, 8702))
    assert device.aes67_configuration_supported is True
    assert device.detailed_metering_supported is True
    assert device.dante_model_primary_capabilities == 0x8E78F65A
