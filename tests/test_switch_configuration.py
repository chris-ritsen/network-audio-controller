from pathlib import Path
from unittest.mock import MagicMock

import pytest

from netaudio import core
from netaudio._common import CapabilityProbeTimeout, CoreCommandSender
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import DanteNotificationService


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "switch_configuration" / "ad4d-switched-0014.hex"
PACKET = bytes.fromhex(FIXTURE_PATH.read_text().strip())


def test_switch_configuration_probe_matches_shipping_controller_request():
    packet, service, port = DanteDeviceCommands().command_probe_switch_configuration(
        host_mac=bytes.fromhex("3e42274cff24"),
        sequence=0x97BE,
    )

    assert packet.hex() == "ffff002497be00003e42274cff240000417564696e617465073a00150000006400000000"
    assert service is None
    assert port == DEVICE_SETTINGS_PORT


def test_switch_configuration_parser_preserves_choices_and_unmapped_fields():
    parsed = core.parse_response("switch_configuration_status", PACKET)

    assert parsed["record_protocol_identifier"] == 0x072E
    assert parsed["choice_count"] == 2
    assert parsed["choice_table_pointer"] == 0x0018
    assert parsed["referenced_value_pointer"] == 0x0010
    assert parsed["referenced_value_size"] == 4
    assert parsed["referenced_value_hexadecimal"] == "0000007f"
    assert parsed["mode_codes_at_record_offsets_20_and_22"] == [1, 1]
    assert [(choice["code"], choice["label"]) for choice in parsed["choices"]] == [
        (1, "Switched"),
        (2, "Split/Redundant"),
    ]
    assert parsed["choices"][0]["unmapped_trailing_words"] == [0x7F, 0, 0, 0]
    assert parsed["choices"][1]["unmapped_trailing_words"] == [0x28, 0x24, 0x53, 0]


def test_notification_waiter_is_source_matched():
    device_ip_address = "192.0.2.10"
    service = DanteNotificationService(dispatcher=DanteEventDispatcher())
    waiter = service.register_switch_configuration_waiter(device_ip_address)

    service._on_packet(PACKET, ("192.0.2.11", 8702))

    assert not waiter.is_set()
    assert service.get_switch_configuration_result(device_ip_address) is None

    service._on_packet(PACKET, (device_ip_address, 8702))

    assert waiter.is_set()
    assert service.get_switch_configuration_result(device_ip_address)["choices"][0]["label"] == "Switched"
    service.unregister_switch_configuration_waiter(device_ip_address)


@pytest.mark.asyncio
async def test_application_probe_waits_for_switch_configuration_publication():
    application = DanteApplication()
    device_ip_address = "192.0.2.10"
    application.settings.probe_switch_configuration = MagicMock(
        side_effect=lambda address: application.notifications._on_packet(PACKET, (address, 8702))
    )

    result = await application.probe_switch_configuration(device_ip_address)

    assert result["mode_codes_at_record_offsets_20_and_22"] == [1, 1]
    application.settings.probe_switch_configuration.assert_called_once_with(device_ip_address)
    assert not application.notifications._waiters.is_registered("switch_configuration", device_ip_address)


@pytest.mark.asyncio
async def test_core_command_sender_returns_switch_configuration_publication():
    class ResponseCoreCommandSender(CoreCommandSender):
        async def __call__(self, packet, device_ip_address, port, **request_options):
            self._notifications._on_packet(PACKET, (str(device_ip_address), 8702))
            return None

    device_ip_address = "192.0.2.10"
    sender = ResponseCoreCommandSender()
    sender._notifications = DanteNotificationService(dispatcher=DanteEventDispatcher())

    result = await sender.probe_switch_configuration(device_ip_address)

    assert result["choices"][1]["label"] == "Split/Redundant"
    assert not sender._notifications._waiters.is_registered("switch_configuration", device_ip_address)


@pytest.mark.asyncio
async def test_core_command_sender_timeout_is_fail_closed():
    class NoResponseCoreCommandSender(CoreCommandSender):
        async def __call__(self, packet, device_ip_address, port, **request_options):
            return None

    device_ip_address = "192.0.2.10"
    sender = NoResponseCoreCommandSender()
    sender._notifications = DanteNotificationService(dispatcher=DanteEventDispatcher())

    with pytest.raises(CapabilityProbeTimeout, match="switch configuration readback timed out"):
        await sender.probe_switch_configuration(device_ip_address, timeout=0.01)

    assert not sender._notifications._waiters.is_registered("switch_configuration", device_ip_address)
    assert sender._notifications.get_switch_configuration_result(device_ip_address) is None
