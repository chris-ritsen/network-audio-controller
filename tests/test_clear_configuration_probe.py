from unittest.mock import AsyncMock

import pytest

from netaudio.dante.application import DanteApplication
from netaudio.dante.application import CapabilityProbeTimeout


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
    application.settings.probe_clear_configuration_status = AsyncMock(
        side_effect=lambda ip_address: application.notifications._on_packet(
            TREATMENT_STATUS,
            (ip_address, 8702),
        )
    )

    result = await application.probe_clear_configuration_status(device_ip_address)

    assert result == expected_status
    application.settings.probe_clear_configuration_status.assert_awaited_once_with(device_ip_address)
    assert not application.notifications.is_waiting("clear_configuration_status", device_ip_address)


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

    command = AsyncMock(side_effect=publish_status)
    setattr(application.settings, method_name, command)

    result = await application.clear_configuration(
        device_ip_address,
        preserve_internet_protocol_settings,
    )

    assert result["action_result_code"] == expected_result_code
    command.assert_awaited_once_with(device_ip_address)
    assert not application.notifications.is_waiting("clear_configuration_status", device_ip_address)


@pytest.mark.asyncio
async def test_clear_configuration_rejects_a_nonmatching_status_result():
    application = DanteApplication()
    device_ip_address = "10.0.2.15"
    application.settings.clear_all_configuration = AsyncMock(
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

    assert not application.notifications.is_waiting("clear_configuration_status", device_ip_address)


@pytest.mark.asyncio
async def test_clear_configuration_times_out_without_any_status():
    application = DanteApplication()
    application.settings.clear_all_configuration = AsyncMock()

    with pytest.raises(CapabilityProbeTimeout, match="clear-configuration status timed out"):
        await application.clear_configuration("10.0.2.15", preserve_internet_protocol_settings=False, timeout=0.01)

    assert not application.notifications.is_waiting("clear_configuration_status", "10.0.2.15")
