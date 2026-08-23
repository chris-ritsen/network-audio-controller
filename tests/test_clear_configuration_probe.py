from unittest.mock import MagicMock

import pytest

from netaudio._common import CoreCommandSender
from netaudio.dante.application import DanteApplication
from netaudio.dante.services.notification import DanteNotificationService


TREATMENT_STATUS = bytes.fromhex("ffff0028000e00000200000000010000417564696e61746507240078000000000000000300000000")
MODE_ONE_STATUS = bytes.fromhex("ffff0028000f00000200000000010000417564696e61746507240078000000000000000300000001")
MODE_TWO_STATUS = bytes.fromhex("ffff0028001d00000200000000010000417564696e61746507240078000000000000000300000002")


@pytest.mark.asyncio
async def test_probe_clear_configuration_status_waits_for_the_matching_conmon_response():
    application = DanteApplication()
    device_ip_address = "10.0.2.15"
    expected_status = {
        "record_protocol_identifier": 0x0724,
        "unmapped_first_word": 0,
        "available_actions_mask": 3,
        "action_result_code": 0,
    }
    application.settings.probe_clear_configuration_status = MagicMock(
        side_effect=lambda ip_address: application.notifications._on_packet(
            TREATMENT_STATUS,
            (ip_address, 8702),
        )
    )

    result = await application.probe_clear_configuration_status(device_ip_address)

    assert result == expected_status
    application.settings.probe_clear_configuration_status.assert_called_once_with(device_ip_address)
    assert not application.notifications._waiters.is_registered(
        "clear_configuration_status",
        device_ip_address,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("preserve_internet_protocol_settings", "status_packet", "expected_result_code", "method_name"),
    [
        (False, MODE_ONE_STATUS, 1, "clear_all_configuration"),
        (
            True,
            MODE_TWO_STATUS,
            2,
            "clear_all_configuration_preserving_internet_protocol_settings",
        ),
    ],
)
async def test_clear_configuration_waits_for_the_requested_action_result(
    preserve_internet_protocol_settings,
    status_packet,
    expected_result_code,
    method_name,
):
    application = DanteApplication()
    device_ip_address = "10.0.2.15"

    def publish_status(ip_address):
        application.notifications._on_packet(TREATMENT_STATUS, (ip_address, 8702))
        application.notifications._on_packet(status_packet, (ip_address, 8702))

    command = MagicMock(side_effect=publish_status)
    setattr(application.settings, method_name, command)

    result = await application.clear_configuration(
        device_ip_address,
        preserve_internet_protocol_settings,
    )

    assert result["action_result_code"] == expected_result_code
    command.assert_called_once_with(device_ip_address)
    assert application.notifications._clear_configuration_status_waiters == {}


@pytest.mark.asyncio
async def test_clear_configuration_rejects_a_nonmatching_status_result():
    application = DanteApplication()
    device_ip_address = "10.0.2.15"
    application.settings.clear_all_configuration = MagicMock(
        side_effect=lambda ip_address: application.notifications._on_packet(
            TREATMENT_STATUS,
            (ip_address, 8702),
        )
    )

    with pytest.raises(RuntimeError, match="returned result 0 instead of 1"):
        await application.clear_configuration(
            device_ip_address,
            preserve_internet_protocol_settings=False,
            timeout=0.01,
        )

    assert application.notifications._clear_configuration_status_waiters == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("preserve_internet_protocol_settings", "status_packet", "expected_result_code"),
    [(False, MODE_ONE_STATUS, 1), (True, MODE_TWO_STATUS, 2)],
)
async def test_core_command_sender_sends_once_and_waits_for_the_matching_result(
    preserve_internet_protocol_settings,
    status_packet,
    expected_result_code,
):
    device_ip_address = "10.0.2.15"
    notifications = DanteNotificationService(
        dispatcher=MagicMock(),
        device_lookup=lambda _device_ip_address: None,
    )

    class ClearConfigurationSender(CoreCommandSender):
        def __init__(self):
            super().__init__()
            self._host_mac = bytes.fromhex("fec9ca09a6d5")
            self.sent = []

        async def _ensure_notifications(self):
            return notifications

        async def __call__(
            self,
            packet,
            destination,
            port,
            *,
            expect_response=True,
            repeat=1,
            interval_ms=0,
        ):
            self.sent.append((packet, destination, port, expect_response, repeat, interval_ms))
            notifications._on_packet(TREATMENT_STATUS, (destination, 8702))
            notifications._on_packet(status_packet, (destination, 8702))
            return None

    sender = ClearConfigurationSender()

    result = await sender.clear_configuration(
        device_ip_address,
        preserve_internet_protocol_settings,
    )

    assert result["action_result_code"] == expected_result_code
    assert len(sender.sent) == 1
    packet, destination, port, expect_response, repeat, interval_milliseconds = sender.sent[0]
    assert destination == device_ip_address
    assert port == 8700
    assert expect_response is False
    assert repeat == 1
    assert interval_milliseconds == 0
    assert int.from_bytes(packet[-4:], "big") == expected_result_code
    assert notifications._clear_configuration_status_waiters == {}
