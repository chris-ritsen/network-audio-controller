from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio.dante.application import CapabilityProbeTimeout
from netaudio import core
from netaudio.dante.application import DanteApplication
from netaudio.dante.const import DEVICE_SETTINGS_PORT
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_commands import DanteDeviceCommands
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.link_status import LinkStatusObservation
from netaudio.dante.services.notification import DanteNotificationService


FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "link_status"
A32_PACKET = (FIXTURE_DIRECTORY / "a32-authentic-vm-0040.bin").read_bytes()
LX_DANTE_PACKET = (FIXTURE_DIRECTORY / "lx-dante-0040.bin").read_bytes()
AVIO_PACKET = (FIXTURE_DIRECTORY / "avio-0040.bin").read_bytes()
AD4D_PACKET = (FIXTURE_DIRECTORY / "ad4d-0040.bin").read_bytes()


def _device(model: str, address: str = "192.0.2.10") -> DanteDevice:
    device = DanteDevice(server_name="device.local.")
    device.name = "device"
    device.ipv4 = address
    device.dante_model = model
    return device


def test_link_status_probe_matches_retained_request():
    packet, service, port = DanteDeviceCommands().command_probe_link_status(
        host_mac=bytes.fromhex("52550a000202"),
        sequence=0x0047,
    )

    assert packet.hex() == (
        "ffff00380047000052550a0002020000417564696e617465073a0041"
        "00000000000000000000000000000000000000000000000000000000"
    )
    assert service is None
    assert port == DEVICE_SETTINGS_PORT


@pytest.mark.parametrize(
    ("packet", "record_count", "record_size_bytes", "link_speed"),
    [
        (A32_PACKET, 3, 24, 1000),
        (LX_DANTE_PACKET, 1, 52, 1000),
        (AVIO_PACKET, 1, 24, 100),
        (AD4D_PACKET, 1, 24, 1000),
    ],
)
def test_link_status_parser_accepts_each_retained_layout(packet, record_count, record_size_bytes, link_speed):
    parsed = core.parse_response("unmapped_0040_status", packet)

    assert parsed["record_count"] == record_count
    assert parsed["records"][0]["record_size_bytes"] == record_size_bytes
    assert parsed["records"][0]["link_speed_megabits_per_second"] == link_speed
    assert len(bytes.fromhex(parsed["records"][0]["raw_record_hexadecimal"])) == record_size_bytes


def test_lx_dante_extension_is_preserved_without_semantic_decoding():
    parsed = core.parse_response("unmapped_0040_status", LX_DANTE_PACKET)

    assert parsed["records"][0]["unmapped_trailing_hexadecimal"] == (
        "00010044000000000000000000000000000000000000000000000000"
    )


def test_a32_labels_require_the_model_and_exact_three_record_layout():
    a32_layout = core.parse_response("unmapped_0040_status", A32_PACKET)
    avio_layout = core.parse_response("unmapped_0040_status", AVIO_PACKET)
    a32 = LinkStatusObservation.from_core(a32_layout, _device("A32 Dante AD/DA Converter"))
    lx_dante = LinkStatusObservation.from_core(a32_layout, _device("LX-DANTE"))
    a32_model_with_avio_layout = LinkStatusObservation.from_core(
        avio_layout,
        _device("A32 Dante AD/DA Converter"),
    )

    assert [record.label for record in a32.records] == ["selected_link", "switch_port_0", "switch_port_3"]
    assert [record.label for record in lx_dante.records] == [None, None, None]
    assert [record.label for record in a32_model_with_avio_layout.records] == [None]


def test_notification_waiter_returns_typed_link_status():
    device_ip_address = "192.0.2.10"
    device = _device("LX-DANTE", device_ip_address)
    service = DanteNotificationService(
        dispatcher=DanteEventDispatcher(),
        device_lookup=lambda address: device if address == device_ip_address else None,
    )
    waiter = service.register_waiter("link_status", device_ip_address)

    service._on_packet(LX_DANTE_PACKET, (device_ip_address, 8702))

    assert waiter.is_set()
    result = waiter.latest_result
    assert isinstance(result, LinkStatusObservation)
    assert result.records[0].record_size_bytes == 52
    assert result.records[0].label is None
    service.unregister_waiter(waiter)


def test_notification_waiter_ignores_link_status_from_another_source():
    device_ip_address = "192.0.2.10"
    service = DanteNotificationService(
        dispatcher=DanteEventDispatcher(),
        device_lookup=lambda address: _device("AD4D", address),
    )
    waiter = service.register_waiter("link_status", device_ip_address)

    service._on_packet(AD4D_PACKET, ("192.0.2.11", 8702))

    assert not waiter.is_set()
    assert waiter.latest_result is None
    service.unregister_waiter(waiter)


@pytest.mark.asyncio
async def test_application_probe_waits_for_link_status_publication():
    application = DanteApplication()
    device_ip_address = "192.0.2.10"
    device = _device("A32 Dante AD/DA Converter", device_ip_address)
    application.devices[device.server_name] = device
    application.settings.probe_link_status = AsyncMock(
        side_effect=lambda address: application.notifications._on_packet(A32_PACKET, (address, 8702))
    )

    result = await application.probe_link_status(device_ip_address)

    assert isinstance(result, LinkStatusObservation)
    assert result.records[0].label == "selected_link"
    assert result.records[1].label == "switch_port_0"
    assert result.records[2].label == "switch_port_3"
    application.settings.probe_link_status.assert_awaited_once_with(device_ip_address)
    assert not application.notifications.is_waiting("link_status", device_ip_address)


@pytest.mark.asyncio
async def test_application_probe_timeout_raises_and_unregisters_waiter():
    application = DanteApplication()
    device_ip_address = "192.0.2.10"
    application.notifications._on_packet(A32_PACKET, (device_ip_address, 8702))
    application.settings.probe_link_status = AsyncMock()

    with pytest.raises(CapabilityProbeTimeout, match="link status readback timed out"):
        await application.probe_link_status(device_ip_address, timeout=0.01)

    application.settings.probe_link_status.assert_awaited_once_with(device_ip_address)
    assert not application.notifications.is_waiting("link_status", device_ip_address)
