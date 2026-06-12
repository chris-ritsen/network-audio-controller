import pytest

from netaudio import core
from netaudio.dante.const import SERVICE_ARC, DEVICE_SETTINGS_PORT, DEVICE_CONTROL_PORT
from netaudio.dante.device_commands import DanteDeviceCommands


pytestmark = pytest.mark.skipif(not core.available(), reason="netaudio-core not built")


def test_arc_command_returns_packet_and_service():
    packet, service = DanteDeviceCommands().command_set_name("Studio-AVIO")
    assert service == SERVICE_ARC
    assert packet == core.build_command({"command": "set_name", "name": "Studio-AVIO"})


def test_settings_command_targets_settings_port():
    packet, _, port = DanteDeviceCommands().command_identify()
    assert port == DEVICE_SETTINGS_PORT


def test_control_command_targets_control_port():
    packet, _, port = DanteDeviceCommands().command_metering_stop("dev", None, "001dc1502368", 0)
    assert port == DEVICE_CONTROL_PORT


def test_query_latency_config_is_arc_with_none_tail():
    packet, service, tail = DanteDeviceCommands().command_query_latency_config()
    assert service == SERVICE_ARC and tail is None


def test_model_commands_return_bare_packet():
    packet = DanteDeviceCommands().command_make_model("001dc1502368")
    assert isinstance(packet, bytes)


def test_host_mac_bytes_normalized():
    mac = bytes.fromhex("001dc1502368")
    packet, _, _ = DanteDeviceCommands().command_reboot(host_mac=mac)
    assert packet[8:14] == mac
