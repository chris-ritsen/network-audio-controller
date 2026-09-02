import pytest

from netaudio import core
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_serializer import DanteDeviceSerializer

CONTROLLER_WRITE_239_238 = "28090014040311010000010180600010efee0000"
CONTROLLER_WRITE_239_69 = "2809001400c011010000010180600010ef450000"
AES3_BEFORE = "28090094180011000001171702010001820400688205006c021000100211001000008218000082198301007083020074830600780310001003110010030300028021007c000000f08060008c002200010063000100000064000000650222138c0212003083210090000f4240000f4240000f42400135f1b4000f424000000000000000000000000000000000ef450000001e8480"
AES3_AFTER = "28090094180211000001171702010001820400688205006c021000100211001000008218000082198301007083020074830600780310001003110010030300028021007c000000f08060008c002200010063000100000064000000650222138c0212003083210090000f4240000f4240000f42400135f1b4000f424000000000000000000000000000000000efee0000001e8480"


def test_builder_matches_controller_prefix_writes():
    assert (
        core.build_command(
            {
                "command": "set_aes67_multicast_prefix",
                "prefix": "239.238.0.0",
                "transaction_id": 0x0403,
            }
        ).hex()
        == CONTROLLER_WRITE_239_238
    )
    assert (
        core.build_command(
            {
                "command": "set_aes67_multicast_prefix",
                "prefix": "239.69.0.0",
                "transaction_id": 0x00C0,
            }
        ).hex()
        == CONTROLLER_WRITE_239_69
    )


def test_device_settings_parser_reads_prefix_without_changing_aes67_enable():
    before = core.parse_response("device_settings", bytes.fromhex(AES3_BEFORE))
    after = core.parse_response("device_settings", bytes.fromhex(AES3_AFTER))
    assert before["aes67_multicast_prefix"] == "239.69.0.0"
    assert after["aes67_multicast_prefix"] == "239.238.0.0"
    assert core.parse_response("aes67_configured", bytes.fromhex(AES3_BEFORE)) is False
    assert core.parse_response("aes67_configured", bytes.fromhex(AES3_AFTER)) is False


def test_device_stores_and_serializes_multicast_prefix():
    device = DanteDevice()
    device.apply_controls({"aes67_multicast_prefix": "239.69.0.0"})
    assert device.aes67_multicast_prefix == "239.69.0.0"
    assert DanteDeviceSerializer.to_json(device)["aes67_multicast_prefix"] == "239.69.0.0"


def test_serializer_writes_clock_subdomain_as_integer_list():
    device = DanteDevice()
    device.clock_subdomain = b"_DFLT" + bytes(11)
    assert DanteDeviceSerializer.to_json(device)["clock_subdomain"] == list(b"_DFLT" + bytes(11))


def test_device_advertises_aes67_multicast_prefix_from_value_or_directory():
    from netaudio.dante.device import device_advertises_aes67_multicast_prefix

    device = DanteDevice()
    assert device_advertises_aes67_multicast_prefix(device) is False
    device.settings_properties = [{"property_id": 0x8060, "flags": 1}]
    assert device.advertises_aes67_multicast_prefix is True
    device.settings_properties = [{"property_id": 0x0063, "flags": 1}]
    assert device.advertises_aes67_multicast_prefix is False
    device.aes67_multicast_prefix = "239.69.0.0"
    assert device.advertises_aes67_multicast_prefix is True


@pytest.mark.asyncio
async def test_set_aes67_multicast_prefix_state_writes_and_reads_back():
    from unittest.mock import AsyncMock

    from netaudio.dante.application import DanteApplication

    device = DanteDevice()
    device.ipv4 = "192.0.2.10"
    device.aes67_multicast_prefix = "239.69.0.0"

    async def readback(_device):
        device.aes67_multicast_prefix = "239.238.0.0"
        return False

    device.execute = AsyncMock()
    application = DanteApplication()
    application.get_aes67_configured = AsyncMock(side_effect=readback)

    observed = await application.set_aes67_multicast_prefix(device, "239.238.0.0")
    device.execute.assert_awaited_once_with({"command": "set_aes67_multicast_prefix", "prefix": "239.238.0.0"})
    assert observed == "239.238.0.0"
