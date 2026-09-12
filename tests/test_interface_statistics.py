import hashlib
import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.application import CapabilityProbeTimeout, DanteApplication
from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.interface_statistics import (
    InterfaceStatisticsErrorBaselines,
    InterfaceStatisticsObservation,
)
from netaudio.dante.services.notification import DanteNotificationService


FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "interface_statistics"
A32_PACKET = (FIXTURE_DIRECTORY / "a32-authentic-vm-0040.bin").read_bytes()
LX_DANTE_PACKET = (FIXTURE_DIRECTORY / "lx-dante-0040.bin").read_bytes()
AVIO_PACKET = (FIXTURE_DIRECTORY / "avio-0040.bin").read_bytes()
AD4D_PACKET = (FIXTURE_DIRECTORY / "ad4d-0040.bin").read_bytes()


def test_interface_statistics_fixtures_match_recorded_digests():
    provenance = json.loads((FIXTURE_DIRECTORY / "provenance.json").read_text())

    for fixture_name, record in provenance["fixtures"].items():
        payload = (FIXTURE_DIRECTORY / fixture_name).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == record["sha256"]


def _device(address: str = "192.0.2.10") -> DanteDevice:
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.ipv4 = address
    return device


def _observation(packet: bytes, source: str = "192.0.2.10", monotonic: float = 10.0):
    parsed = core.parse_response("interface_statistics_status", packet)
    return InterfaceStatisticsObservation.from_core(
        parsed,
        source,
        received_at="2026-09-12T12:00:00.000000Z",
        received_monotonic=monotonic,
        freshness_seconds=5.0,
    )


def test_interface_statistics_probe_preserves_generic_and_extended_073a_variants():
    commands = DanteDeviceCommands()
    generic, service, port = commands.command_probe_interface_statistics(
        host_mac=bytes.fromhex("52550a000202"),
        sequence=0x0047,
    )
    extended, _, _ = commands.command_probe_interface_statistics(
        host_mac=bytes.fromhex("52550a000202"),
        sequence=0x0047,
        extended_073a=True,
    )

    assert generic.hex() == "ffff00220047000052550a0002020000417564696e617465073a0041000000000000"
    assert extended.hex() == (
        "ffff00380047000052550a0002020000417564696e617465073a0041"
        "00000000000000000000000000000000000000000000000000000000"
    )
    assert service is None
    assert port == DEVICE_SETTINGS_PORT


@pytest.mark.parametrize(
    ("packet", "group_count", "speed"),
    [
        (A32_PACKET, 1, 1000),
        (LX_DANTE_PACKET, 2, 1000),
        (AVIO_PACKET, 1, 100),
        (AD4D_PACKET, 1, 1000),
    ],
)
def test_interface_statistics_parser_accepts_each_retained_pointer_graph(packet, group_count, speed):
    parsed = core.parse_response("interface_statistics_status", packet)

    assert parsed["interface_group_count"] == group_count
    assert len(parsed["interface_groups"]) == group_count
    assert len(bytes.fromhex(parsed["header_record_hexadecimal"])) == parsed["header_record_size_bytes"]
    assert bytes.fromhex(parsed["raw_body_hexadecimal"]) == packet[0x18:]
    selected = parsed["interface_groups"][0]["selected_stats"]
    assert selected["record_size_bytes"] == 24
    assert selected["speed_megabits_per_second"] == speed
    assert selected["transmit_bits_per_second"] == selected["transmit_raw_bytes_per_second"] * 8
    assert selected["receive_bits_per_second"] == selected["receive_raw_bytes_per_second"] * 8
    assert len(bytes.fromhex(selected["raw_record_hexadecimal"])) == selected["record_size_bytes"]


def test_lx_packet_retains_two_independent_interface_groups():
    parsed = core.parse_response("interface_statistics_status", LX_DANTE_PACKET)

    assert parsed["interface_group_pointers"] == [0x0024, 0x0040]
    assert [group["record_pointers"] for group in parsed["interface_groups"]] == [[0x0028], [0x0044]]
    assert [group["selected_stats"]["transmit_raw_bytes_per_second"] for group in parsed["interface_groups"]] == [
        0x00206738,
        0,
    ]


def test_selection_uses_first_record_with_zero_discriminator_bytes():
    parsed = core.parse_response("interface_statistics_status", A32_PACKET)
    group = parsed["interface_groups"][0]

    assert group["selected_stats"]["record_pointer"] == 0x002C
    assert [record["discriminator_status_word"] for record in group["raw_records"]] == [
        0x00000001,
        0x01000001,
        0x01010000,
    ]
    assert all("link_up" not in record for record in group["raw_records"])


def test_observation_retains_source_receive_time_transport_and_freshness():
    observation = _observation(AVIO_PACKET)

    assert observation.to_dict(observed_monotonic=14.999)["fresh"] is True
    stale = observation.to_dict(observed_monotonic=15.0)
    assert stale["fresh"] is False
    selected = stale["interface_groups"][0]["selected_stats"]
    assert len(bytes.fromhex(stale["header_record_hexadecimal"])) == stale["header_record_size_bytes"]
    assert bytes.fromhex(stale["raw_body_hexadecimal"]) == AVIO_PACKET[0x18:]
    assert selected["packet_source"] == "192.0.2.10"
    assert selected["received_at"] == "2026-09-12T12:00:00.000000Z"
    assert selected["transport_source"] == "conmon_0x0040"
    assert selected["fresh"] is False


def _with_errors(observation, transmit: int, receive: int):
    group = observation.interface_groups[0]
    selected = replace(
        group.selected_stats,
        cumulative_transmit_errors=transmit,
        cumulative_receive_errors=receive,
    )
    return replace(observation, interface_groups=(replace(group, selected_stats=selected),))


def test_local_error_baselines_start_at_zero_and_reset_after_counter_decrease():
    baselines = InterfaceStatisticsErrorBaselines()
    observation = _observation(AVIO_PACKET)

    first = baselines.apply(_with_errors(observation, 10, 20)).interface_groups[0].selected_stats
    increased = baselines.apply(_with_errors(observation, 13, 25)).interface_groups[0].selected_stats
    decreased = baselines.apply(_with_errors(observation, 2, 4)).interface_groups[0].selected_stats
    after_decrease = baselines.apply(_with_errors(observation, 5, 9)).interface_groups[0].selected_stats

    assert (first.transmit_errors_since_local_reset, first.receive_errors_since_local_reset) == (0, 0)
    assert (increased.transmit_errors_since_local_reset, increased.receive_errors_since_local_reset) == (3, 5)
    assert (decreased.transmit_errors_since_local_reset, decreased.receive_errors_since_local_reset) == (0, 0)
    assert (after_decrease.transmit_errors_since_local_reset, after_decrease.receive_errors_since_local_reset) == (3, 5)


def test_notification_waiter_returns_typed_interface_statistics_with_local_baseline():
    device_ip_address = "192.0.2.10"
    service = DanteNotificationService(
        dispatcher=DanteEventDispatcher(),
        device_lookup=lambda address: _device(address),
    )
    waiter = service.register_waiter("interface_statistics", device_ip_address)

    service._on_packet(LX_DANTE_PACKET, (device_ip_address, 8702))

    assert waiter.is_set()
    result = waiter.latest_result
    assert isinstance(result, InterfaceStatisticsObservation)
    assert result.interface_group_count == 2
    assert result.interface_groups[0].selected_stats.transmit_errors_since_local_reset == 0
    service.unregister_waiter(waiter)


def test_notification_waiter_ignores_interface_statistics_from_another_source():
    device_ip_address = "192.0.2.10"
    service = DanteNotificationService(
        dispatcher=DanteEventDispatcher(),
        device_lookup=lambda address: _device(address),
    )
    waiter = service.register_waiter("interface_statistics", device_ip_address)

    service._on_packet(AD4D_PACKET, ("192.0.2.11", 8702))

    assert not waiter.is_set()
    assert waiter.latest_result is None
    service.unregister_waiter(waiter)


@pytest.mark.asyncio
async def test_application_probe_waits_for_interface_statistics_publication():
    application = DanteApplication()
    device_ip_address = "192.0.2.10"
    device = _device(device_ip_address)
    application.devices[device.server_name] = device
    application.send_probe_interface_statistics = AsyncMock(
        side_effect=lambda address: application.notifications._on_packet(A32_PACKET, (address, 8702))
    )

    result = await application.probe_interface_statistics(device_ip_address)

    assert isinstance(result, InterfaceStatisticsObservation)
    assert result.interface_groups[0].selected_stats.record_pointer == 0x002C
    application.send_probe_interface_statistics.assert_awaited_once_with(device_ip_address)
    assert device.interface_statistics["transport_source"] == "conmon_0x0040"
    assert not application.notifications.is_waiting("interface_statistics", device_ip_address)


@pytest.mark.asyncio
async def test_application_probe_timeout_raises_and_unregisters_waiter():
    application = DanteApplication()
    device_ip_address = "192.0.2.10"
    application.notifications._on_packet(A32_PACKET, (device_ip_address, 8702))
    application.send_probe_interface_statistics = AsyncMock()

    with pytest.raises(CapabilityProbeTimeout, match="interface statistics readback timed out"):
        await application.probe_interface_statistics(device_ip_address, timeout=0.01)

    application.send_probe_interface_statistics.assert_awaited_once_with(device_ip_address)
    assert not application.notifications.is_waiting("interface_statistics", device_ip_address)
