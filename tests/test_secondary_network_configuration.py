from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from netaudio import core
from netaudio.dante.commands import DanteCommands
from netaudio.dante.device_commands import DanteDeviceCommands

EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "secondary_network_configuration.json").read_text())


def packet(case):
    record = EVIDENCE["cases"][case]
    data = bytes.fromhex(record["hexadecimal"])
    assert hashlib.sha256(data).hexdigest() == record["sha256"]
    return data


def test_secondary_static_request_matches_the_causal_wing_observation():
    observed = packet("static_request")
    spec = DanteCommands().set_interface_static(
        "198.51.100.102",
        "255.255.255.0",
        "203.0.113.53",
        "203.0.113.2",
        host_mac="020000000062",
        interface="secondary",
        record_protocol_identifier=0x073D,
    )
    spec["sequence"] = int.from_bytes(observed[4:6], "big")
    assert core.build_command(spec) == observed
    device_packet, _, _ = DanteDeviceCommands().command_set_interface_static(
        "198.51.100.102",
        "255.255.255.0",
        "203.0.113.53",
        "203.0.113.2",
        host_mac=bytes.fromhex("020000000062"),
        sequence=spec["sequence"],
        interface="secondary",
        record_protocol_identifier=0x073D,
    )
    assert device_packet == observed
    before = core.parse_response("interface_status", packet("before"))
    after = core.parse_response("interface_status", packet("after"))
    expected = deepcopy(before)
    expected["interfaces"][1]["configured"]["dns_server"] = "203.0.113.53"
    assert after == expected
    assert before["redundancy"] == after["redundancy"]


def test_secondary_dhcp_changes_only_the_interface_word_from_primary():
    spec = {
        "command": "set_interface_dhcp",
        "host_mac": "020000000062",
        "sequence": 23,
        "record_protocol_identifier": 0x073D,
    }
    primary = core.build_command({**spec, "interface": "primary"})
    secondary = core.build_command({**spec, "interface": "secondary"})
    assert secondary[:40] == primary[:40]
    assert secondary[40:42] == b"\x00\x01"
    assert primary[40:42] == b"\x00\x00"
    assert secondary[42:] == primary[42:]


def test_installed_secondary_dhcp_readback_preserves_primary_and_redundancy():
    records = EVIDENCE["dhcp_verification"]["records"]
    for record in records.values():
        encoded = json.dumps(record["json"], sort_keys=True, separators=(",", ":")).encode()
        assert hashlib.sha256(encoded).hexdigest() == record["sha256"]
    before = records["before"]["json"]
    after = records["after"]["json"]
    expected = deepcopy(before)
    expected["interfaces"][1]["configured"] = {"mode": "dynamic"}
    expected["interfaces"][1]["reboot_required"] = False
    expected["reboot_required"] = bool(expected["redundancy"]["reboot_required"])
    assert after == expected


@pytest.mark.parametrize("command", ["set_interface_dhcp", "set_interface_static"])
@pytest.mark.parametrize("protocol", [None, 0x0724, 0x0738, 0x07FF])
def test_secondary_encoding_does_not_depend_on_the_reported_revision(command, protocol):
    def build(record_protocol_identifier):
        spec = {
            "command": command,
            "interface": "secondary",
            "host_mac": "020000000062",
            "sequence": 1,
            "record_protocol_identifier": record_protocol_identifier,
        }
        if command == "set_interface_static":
            spec.update(ip="198.51.100.102", netmask="255.255.255.0", dns="203.0.113.53", gateway="203.0.113.2")
        return core.build_command(spec)

    assert build(protocol) == build(0x073D)


@pytest.mark.parametrize("interface", ["tertiary", "", 1])
def test_invalid_interface_does_not_select_primary(interface):
    with pytest.raises(core.NetaudioCoreError):
        core.build_command(
            {"command": "set_interface_dhcp", "interface": interface, "host_mac": "020000000062", "sequence": 1}
        )
