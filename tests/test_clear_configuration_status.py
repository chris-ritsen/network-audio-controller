import json
from unittest.mock import MagicMock, call

from netaudio import core
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.events import EventType
from netaudio.dante.services.notification import DanteNotificationService
from netaudio.dante.services.settings import DanteSettingsService


CONTROLLER_REQUEST = bytes.fromhex("ffff002401ed0000fec9ca09a6d50000417564696e617465073e00770000006400000000")
MODE_ZERO_STATUS = bytes.fromhex("ffff0028001b00000200000000010000417564696e61746507240078000000000000000300000000")
MODE_ONE_STATUS = bytes.fromhex("ffff0028000f00000200000000010000417564696e61746507240078000000000000000300000001")
MODE_TWO_STATUS = bytes.fromhex("ffff0028001d00000200000000010000417564696e61746507240078000000000000000300000002")
SOURCE_BIT_TWO_STATUS = bytes.fromhex(
    "ffff0028001d00000200000000010000417564696e61746507240078000000000000000200000000"
)
SOURCE_BIT_THREE_STATUS = bytes.fromhex(
    "ffff0028000e00000200000000010000417564696e61746507240078000000000000000100000000"
)


def test_status_probe_builder_matches_shipping_controller_request():
    specification = {
        "command": "probe_clear_configuration_status",
        "host_mac": "fec9ca09a6d5",
        "sequence": 0x01ED,
    }
    assert core.build_command(specification) == CONTROLLER_REQUEST

    packet, service, port = DanteDeviceCommands().command_probe_clear_configuration_status(
        host_mac=bytes.fromhex("fec9ca09a6d5"),
        sequence=0x01ED,
    )
    assert packet == CONTROLLER_REQUEST
    assert service is None
    assert port == 8700


def test_action_builders_match_authentic_mode_one_and_mode_two_requests():
    host_mac = bytes.fromhex("fec9ca09a6d5")
    commands = DanteDeviceCommands(settings_sequence=0x01EC)

    clear_all_packet, _, clear_all_port = commands.command_clear_all_configuration(host_mac=host_mac)
    preserve_network_packet, _, preserve_network_port = (
        commands.command_clear_all_configuration_preserving_internet_protocol_settings(
            host_mac=host_mac,
        )
    )

    assert clear_all_port == 8700
    assert preserve_network_port == 8700
    assert clear_all_packet == bytes.fromhex("ffff002401ed0000fec9ca09a6d50000417564696e617465073e00770000006400000001")
    assert preserve_network_packet == bytes.fromhex(
        "ffff002401ee0000fec9ca09a6d50000417564696e617465073e00770000006400000002"
    )


def test_parser_preserves_authentic_action_masks_and_result_codes():
    expected_values = [
        (MODE_ZERO_STATUS, 3, 0),
        (MODE_ONE_STATUS, 3, 1),
        (MODE_TWO_STATUS, 3, 2),
        (SOURCE_BIT_TWO_STATUS, 2, 0),
        (SOURCE_BIT_THREE_STATUS, 1, 0),
    ]
    for packet, available_actions_mask, action_result_code in expected_values:
        assert core.parse_response("clear_configuration_status", packet) == {
            "record_protocol_identifier": 0x0724,
            "unmapped_first_word": 0,
            "available_actions_mask": available_actions_mask,
            "action_result_code": action_result_code,
        }


def test_notification_service_applies_and_serializes_clear_configuration_status_once():
    device_ip_address = "10.0.2.15"
    device = DanteDevice(server_name="virtual-a32.local.")
    device.name = "virtual-a32"
    device.ipv4 = device_ip_address
    dispatcher = MagicMock()
    service = DanteNotificationService(
        dispatcher=dispatcher,
        device_lookup=lambda ip_address: device if ip_address == device_ip_address else None,
    )
    expected = core.parse_response("clear_configuration_status", MODE_ONE_STATUS)

    service._on_packet(MODE_ONE_STATUS, (device_ip_address, 8702))
    service._on_packet(MODE_ONE_STATUS, (device_ip_address, 8702))

    assert device.clear_configuration_status == expected
    emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
    assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
    serialized = DanteDeviceSerializer.to_json(device)
    assert serialized["clear_configuration_status"] == expected
    restored = DanteDeviceSerializer.device_from_json(json.loads(json.dumps(serialized)))
    assert restored.clear_configuration_status == expected


def test_settings_service_sends_typed_status_probe():
    service = DanteSettingsService()
    service._commands.command_probe_clear_configuration_status = MagicMock(return_value=(b"probe", None, 8700))
    service.send = MagicMock()
    host_mac = b"\x10\x20\x30\x40\x50\x60"

    service.probe_clear_configuration_status("192.168.1.108", host_mac=host_mac)

    service._commands.command_probe_clear_configuration_status.assert_called_once_with(host_mac=host_mac)
    service.send.assert_called_once_with(b"probe", "192.168.1.108", 8700)


def test_settings_service_sends_both_typed_clear_configuration_actions():
    service = DanteSettingsService()
    service._commands.command_clear_all_configuration = MagicMock(return_value=(b"clear-all", None, 8700))
    service._commands.command_clear_all_configuration_preserving_internet_protocol_settings = MagicMock(
        return_value=(b"preserve-network", None, 8700)
    )
    service.send = MagicMock()
    host_mac = b"\x10\x20\x30\x40\x50\x60"

    service.clear_all_configuration("192.168.1.108", host_mac=host_mac)
    service.clear_all_configuration_preserving_internet_protocol_settings(
        "192.168.1.108",
        host_mac=host_mac,
    )

    service._commands.command_clear_all_configuration.assert_called_once_with(host_mac=host_mac)
    service._commands.command_clear_all_configuration_preserving_internet_protocol_settings.assert_called_once_with(
        host_mac=host_mac
    )
    assert service.send.call_args_list == [
        call(b"clear-all", "192.168.1.108", 8700),
        call(b"preserve-network", "192.168.1.108", 8700),
    ]
