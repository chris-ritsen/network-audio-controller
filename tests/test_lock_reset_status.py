import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import netaudio._common as common_module
from netaudio import core
from netaudio.dante.application import DanteApplication
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.device_serializer import DanteDeviceSerializer
from netaudio.dante.events import EventType
from netaudio.dante.services.notification import DanteNotificationService


LOCK_RESET_STATUS_QUERY = bytes.fromhex("ffff002018c100003e42274cff240000417564696e617465073a100800000064")
LOCK_RESET_STATUS_ONE = bytes.fromhex(
    "ffff003008390000001dc1fffe5279b6417564696e617465073810090000000000000001000000080000000000000000"
)
LOCK_RESET_STATUS_FOUR = bytes.fromhex(
    "ffff00500bf50000001dc1fffe53ef37417564696e617465073810090000000000000004000400080018000000000000"
    "001dc1fffe081258001dc1fffe510295001dc1fffe50cac5001dc1fffe5279b6"
)
LOCK_RESET_STATUS_ZERO = bytes.fromhex(
    "ffff0030003800000200000000010000417564696e617465072410090000000000000000000000080000000000000000"
)

A32_UNLOCKED_LOCK_RESET_STATUS = bytes.fromhex(
    "ffff00301d9800000200000000040000417564696e617465072410090000000000000000000000080000000000000000"
)
A32_LOCKED_LOCK_RESET_STATUS = bytes.fromhex(
    "ffff00501c7e00000200000000040000417564696e617465"
    "072410090000000000010004000400080018000000000000"
    "001dc1fffe081258000eddfffefd4e13001dc1fffe507b8d001dc1fffe61bac5"
)
AVIO_UNLOCKED_LOCK_RESET_STATUS = bytes.fromhex(
    "ffff0050d1580000001dc1fffe53ef37417564696e617465"
    "073810090000000000000004000400080018000000000000"
    "001dc1fffe61bac5001dc1fffe081258001dc1fffe50368b001dc1fffe510295"
)
AVIO_LOCKED_LOCK_RESET_STATUS = bytes.fromhex(
    "ffff0050d15a0000001dc1fffe53ef37417564696e617465"
    "073810090000000000010000000400080018000000000000"
    "001dc1fffe61bac5001dc1fffe081258001dc1fffe50368b001dc1fffe510295"
)


def test_controller_query_builder_is_byte_identical():
    controller_request = LOCK_RESET_STATUS_QUERY
    specification = {
        "command": "probe_lock_reset_status",
        "host_mac": "3e42274cff24",
        "sequence": 0x18C1,
        "request_value": 100,
    }

    assert core.build_command(specification) == controller_request
    packet, service, port = DanteDeviceCommands().command_probe_lock_reset_status(
        host_mac=bytes.fromhex("3e42274cff24"),
        sequence=0x18C1,
        request_value=100,
    )
    assert packet == controller_request
    assert service is None
    assert port == 8700


def test_parser_preserves_status_zero_and_one_without_speculative_labels():
    status_zero = core.parse_response("lock_reset_status", LOCK_RESET_STATUS_ZERO)
    status_one = core.parse_response("lock_reset_status", LOCK_RESET_STATUS_ONE)

    assert status_zero == {
        "record_protocol_identifier": 0x0724,
        "unmapped_prefix_word": 0,
        "lock_state_code": 0,
        "is_locked": False,
        "status_code": 0,
        "lock_identifier_count": 0,
        "lock_identifier_width": 8,
        "lock_identifier_data_offset": 0,
        "unmapped_trailer_words": [0, 0, 0],
        "lock_identifiers": [],
        "raw_record_hexadecimal": "072410090000000000000000000000080000000000000000",
    }
    assert status_one == {
        "record_protocol_identifier": 0x0738,
        "unmapped_prefix_word": 0,
        "lock_state_code": 0,
        "is_locked": False,
        "status_code": 1,
        "lock_identifier_count": 0,
        "lock_identifier_width": 8,
        "lock_identifier_data_offset": 0,
        "unmapped_trailer_words": [0, 0, 0],
        "lock_identifiers": [],
        "raw_record_hexadecimal": "073810090000000000000001000000080000000000000000",
    }


def test_parser_preserves_the_authentic_status_four_identifier_array():
    parsed = core.parse_response("lock_reset_status", LOCK_RESET_STATUS_FOUR)

    assert parsed["record_protocol_identifier"] == 0x0738
    assert parsed["lock_state_code"] == 0
    assert parsed["is_locked"] is False
    assert parsed["status_code"] == 4
    assert parsed["lock_identifier_count"] == 4
    assert parsed["lock_identifier_width"] == 8
    assert parsed["lock_identifier_data_offset"] == 24
    assert parsed["lock_identifiers"] == [
        "001dc1fffe081258",
        "001dc1fffe510295",
        "001dc1fffe50cac5",
        "001dc1fffe5279b6",
    ]
    assert parsed["raw_record_hexadecimal"] == LOCK_RESET_STATUS_FOUR[24:].hex()


def test_passive_lock_state_uses_presence_word_independently_of_status_code():
    cases = [
        (A32_UNLOCKED_LOCK_RESET_STATUS, 0, 0, False),
        (A32_LOCKED_LOCK_RESET_STATUS, 1, 4, True),
        (AVIO_UNLOCKED_LOCK_RESET_STATUS, 0, 4, False),
        (AVIO_LOCKED_LOCK_RESET_STATUS, 1, 0, True),
    ]

    for packet, presence_code, status_code, expected_locked in cases:
        parsed = core.parse_response("lock_reset_status", packet)
        assert parsed["lock_state_code"] == presence_code
        assert parsed["status_code"] == status_code
        assert parsed["is_locked"] is expected_locked

    unknown_state = bytearray(AVIO_LOCKED_LOCK_RESET_STATUS)
    unknown_state[32:34] = (2).to_bytes(2, "big")
    parsed_unknown = core.parse_response("lock_reset_status", bytes(unknown_state))
    assert parsed_unknown["lock_state_code"] == 2
    assert parsed_unknown["is_locked"] is None


def test_notification_service_tracks_passive_lock_transitions():
    device_ip_address = "192.168.1.18"
    device = DanteDevice(server_name="avio-aes3.local.")
    device.name = "avio-aes3"
    device.ipv4 = device_ip_address
    dispatcher = MagicMock()
    service = DanteNotificationService(
        dispatcher=dispatcher,
        device_lookup=lambda ip_address: device if ip_address == device_ip_address else None,
    )

    service._on_packet(AVIO_UNLOCKED_LOCK_RESET_STATUS, (device_ip_address, 8702))
    assert device.is_locked is False

    service._on_packet(AVIO_LOCKED_LOCK_RESET_STATUS, (device_ip_address, 8702))
    assert device.is_locked is True

    service._on_packet(A32_UNLOCKED_LOCK_RESET_STATUS, (device_ip_address, 8702))
    assert device.is_locked is False

    emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
    assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 3


@pytest.mark.asyncio
async def test_lock_status_probe_waits_for_publication_observed_after_request():
    application = DanteApplication()
    device_ip_address = "10.0.2.15"
    application.settings.probe_lock_reset_status = MagicMock(
        side_effect=lambda ip_address: application.notifications._on_packet(
            A32_UNLOCKED_LOCK_RESET_STATUS,
            (ip_address, 8702),
        )
    )

    result = await application.probe_lock_status(device_ip_address)

    assert result.is_locked is False
    assert result.lock_state_code == 0
    assert datetime.fromisoformat(result.observed_at).tzinfo is not None
    application.settings.probe_lock_reset_status.assert_called_once_with(device_ip_address)
    assert not application.notifications._waiters.is_registered("lock_status", device_ip_address)


@pytest.mark.asyncio
async def test_lock_status_probe_timeout_never_returns_a_previous_observation():
    application = DanteApplication()
    device_ip_address = "10.0.2.15"
    application.notifications._waiters._results[("lock_status", device_ip_address)] = object()
    application.settings.probe_lock_reset_status = MagicMock()

    result = await application.probe_lock_status(device_ip_address, timeout=0.001)

    assert result is None
    assert not application.notifications._waiters.is_registered("lock_status", device_ip_address)
    assert application.notifications.get_lock_status_result(device_ip_address) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("probe_result", [None, RuntimeError("socket unavailable")])
async def test_enrich_lock_states_clears_cached_state_without_fresh_observation(monkeypatch, probe_result):
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.ipv4 = "192.0.2.10"
    device.is_locked = True

    application = SimpleNamespace(
        devices={},
        startup=AsyncMock(),
        shutdown=AsyncMock(),
        probe_lock_status=AsyncMock(
            side_effect=probe_result if isinstance(probe_result, BaseException) else None,
            return_value=None if isinstance(probe_result, BaseException) else probe_result,
        ),
    )
    monkeypatch.setattr(common_module, "_make_dante_application", lambda: application)

    await common_module._enrich_lock_states({device.server_name: device})

    assert device.is_locked is None
    application.probe_lock_status.assert_awaited_once_with("192.0.2.10", timeout=4.0)
    application.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_enrich_lock_states_applies_unknown_fresh_observation_without_packet_handler_side_effect(monkeypatch):
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.ipv4 = "192.0.2.10"
    device.is_locked = True
    observation = SimpleNamespace(is_locked=None)
    application = SimpleNamespace(
        devices={},
        startup=AsyncMock(),
        shutdown=AsyncMock(),
        probe_lock_status=AsyncMock(return_value=observation),
    )
    monkeypatch.setattr(common_module, "_make_dante_application", lambda: application)

    await common_module._enrich_lock_states({device.server_name: device})

    assert device.is_locked is None
    application.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_single_lock_status_probe_fails_closed_and_shuts_down_after_startup_error(monkeypatch):
    application = SimpleNamespace(
        startup=AsyncMock(side_effect=RuntimeError("bind failed")),
        shutdown=AsyncMock(),
        probe_lock_status=AsyncMock(),
    )
    monkeypatch.setattr(common_module, "_make_dante_application", lambda: application)

    result = await common_module._probe_lock_status_once("192.0.2.10")

    assert result is None
    application.probe_lock_status.assert_not_awaited()
    application.shutdown.assert_awaited_once()


def test_notification_service_applies_lock_reset_status_once_and_serializes_it():
    device_ip_address = "192.168.1.18"
    device = DanteDevice(server_name="virtual-a32.local.")
    device.name = "virtual-a32"
    device.ipv4 = device_ip_address
    dispatcher = MagicMock()
    service = DanteNotificationService(
        dispatcher=dispatcher,
        device_lookup=lambda ip_address: device if ip_address == device_ip_address else None,
    )
    response = LOCK_RESET_STATUS_FOUR
    expected = core.parse_response("lock_reset_status", response)

    service._on_packet(response, (device_ip_address, 8702))
    service._on_packet(response, (device_ip_address, 8702))

    assert device.lock_reset_status == expected
    assert device.is_locked is False
    emitted_events = [call.args[0] for call in dispatcher.emit_nowait.call_args_list]
    assert sum(event.type == EventType.DEVICE_UPDATED for event in emitted_events) == 1
    serialized = DanteDeviceSerializer.to_json(device)
    assert serialized["lock_reset_status"] == expected
    restored = DanteDeviceSerializer.device_from_json(json.loads(json.dumps(serialized)))
    assert restored.lock_reset_status == expected


def test_notification_service_applies_status_received_before_discovery():
    device_ip_address = "10.0.2.15"
    service = DanteNotificationService(dispatcher=MagicMock())
    service._on_packet(LOCK_RESET_STATUS_ZERO, (device_ip_address, 8702))
    device = DanteDevice(server_name="virtual-a32.local.")
    device.ipv4 = device_ip_address

    service.apply_pending_for_device(device)

    assert device.lock_reset_status == core.parse_response("lock_reset_status", LOCK_RESET_STATUS_ZERO)
    assert device.is_locked is False
