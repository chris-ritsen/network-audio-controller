import json
from pathlib import Path

import pytest

from netaudio import core
from netaudio.dante.const import SERVICE_ARC, DEVICE_SETTINGS_PORT, DEVICE_CONTROL_PORT
from netaudio.dante.device_commands import DanteDeviceCommands


pytestmark = pytest.mark.skipif(not core.available(), reason="netaudio-core not built")
GOLDEN = json.loads((Path(__file__).parent / "fixtures/core_commands_golden.json").read_text())


def test_arc_command_returns_packet_and_service():
    packet, service = DanteDeviceCommands().command_set_name("Studio-AVIO")
    assert service == SERVICE_ARC
    assert packet == bytes.fromhex(GOLDEN["set_name:4"]["hex"])


def test_settings_command_targets_settings_port():
    packet, _, port = DanteDeviceCommands().command_identify()
    assert port == DEVICE_SETTINGS_PORT


def test_control_command_targets_control_port():
    packet, _, port = DanteDeviceCommands().command_metering_stop("dev", None, "001dc1502368", 0)
    assert port == DEVICE_CONTROL_PORT


def test_query_latency_config_is_arc_with_none_tail():
    packet, service, tail = DanteDeviceCommands().command_query_latency_config()
    assert service == SERVICE_ARC and tail is None


@pytest.mark.parametrize(
    ("method", "fixture"),
    [("command_make_model", "make_model:41"), ("command_dante_model", "dante_model:42")],
)
def test_model_commands_return_expected_bare_packet(method, fixture):
    packet = getattr(DanteDeviceCommands(), method)("001dc1502368")
    assert packet == bytes.fromhex(GOLDEN[fixture]["hex"])


def test_host_mac_bytes_normalized():
    mac = bytes.fromhex("001dc1502368")
    packet, _, _ = DanteDeviceCommands().command_reboot(host_mac=mac)
    assert packet[8:14] == mac
