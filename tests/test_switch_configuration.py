from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from netaudio import core
from netaudio.dante.application import CapabilityProbeTimeout
from netaudio.dante.application import DanteApplication
from netaudio.dante.events import DanteEventDispatcher
from netaudio.dante.services.notification import DanteNotificationService
from netaudio.dante.network_configuration import switch_configuration_fields


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "switch_configuration" / "ad4d-switched-0014.hex"
PACKET = bytes.fromhex(FIXTURE_PATH.read_text().strip())


def test_switch_configuration_probe_matches_shipping_controller_request():
    packet = core.build_command(
        {"command": "probe_switch_configuration", "host_mac": "3e42274cff24", "message_id": 0x97BE}
    )

    assert packet.hex() == "ffff002497be00003e42274cff240000417564696e617465073a00150000006400000000"


@pytest.mark.parametrize(
    "label,mode",
    [("Switched", "switched"), ("Redundant", "redundant"), ("Split/Redundant", "split_redundant"), ("Unmapped", None)],
)
def test_native_choice_meaning_reaches_the_device_adapter_without_reinterpretation(label, mode):
    packet = bytearray(PACKET)
    packet[52:180] = label.encode().ljust(128, b"\0")
    parsed = core.parse_response("switch_configuration_status", bytes(packet))

    assert parsed["choices"][0]["mode"] == mode
    assert parsed["redundancy"]["current"] == mode

    fields = switch_configuration_fields(parsed)
    assert fields["dante_redundancy"]["available_modes"] == parsed["choices"]
    assert fields["dante_redundancy"]["supported"] == parsed["redundancy"]["supported"]
    assert fields["dante_redundancy"]["current_mode_evidence"]["mode"] == mode


def test_notification_waiter_is_source_matched():
    device_ip_address = "192.0.2.10"
    service = DanteNotificationService(dispatcher=DanteEventDispatcher())
    waiter = service.register_waiter("switch_configuration", device_ip_address)

    service._on_packet(PACKET, ("192.0.2.11", 8702))

    assert not waiter.is_set()
    assert waiter.latest_result is None

    service._on_packet(PACKET, (device_ip_address, 8702))

    assert waiter.is_set()
    assert waiter.latest_result["choices"][0]["label"] == "Switched"
    service.unregister_waiter(waiter)


@pytest.mark.asyncio
async def test_application_probe_waits_for_switch_configuration_publication():
    application = DanteApplication()
    device_ip_address = "192.0.2.10"
    application.send_probe_switch_configuration = AsyncMock(
        side_effect=lambda address: application.notifications._on_packet(PACKET, (address, 8702))
    )

    result = await application.probe_switch_configuration(device_ip_address)

    assert result["current_mode_evidence"]["mode"] == "switched"
    application.send_probe_switch_configuration.assert_awaited_once_with(device_ip_address)
    assert not application.notifications.is_waiting("switch_configuration", device_ip_address)


@pytest.mark.asyncio
async def test_application_probe_timeout_is_fail_closed():
    application = DanteApplication()
    device_ip_address = "192.0.2.10"
    application.send_probe_switch_configuration = AsyncMock()

    with pytest.raises(CapabilityProbeTimeout, match="switch configuration readback timed out"):
        await application.probe_switch_configuration(device_ip_address, timeout=0.01)

    assert not application.notifications.is_waiting("switch_configuration", device_ip_address)
