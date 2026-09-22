from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from netaudio import core
from netaudio.dante.application import DanteApplication

EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "secondary_network_configuration.json").read_text())


def packet(case):
    record = EVIDENCE["cases"][case]
    data = bytes.fromhex(record["hexadecimal"])
    assert hashlib.sha256(data).hexdigest() == record["sha256"]
    return data


async def application_request(command, *arguments, **options):
    requests = []

    async def execute(address, specification, **transport_options):
        assert address == "192.0.2.10"
        requests.append(core.build_command(specification))

    application = DanteApplication()
    application.transport = SimpleNamespace(execute=execute)
    await getattr(application, f"send_{command}")("192.0.2.10", *arguments, **options)
    [request] = requests
    return request


@pytest.mark.asyncio
async def test_secondary_static_request_matches_the_causal_wing_observation():
    observed = packet("static_request")
    request = await application_request(
        "set_interface_static",
        "198.51.100.102",
        "255.255.255.0",
        "203.0.113.53",
        "203.0.113.2",
        host_mac="020000000062",
        interface="secondary",
    )
    assert int.from_bytes(request[4:6], "big") != 0
    assert request[:4] + request[6:] == observed[:4] + observed[6:]
    before = core.parse_response("interface_status", packet("before"))
    after = core.parse_response("interface_status", packet("after"))
    expected = deepcopy(before)
    expected["interfaces"][1]["configured"]["dns_server"] = "203.0.113.53"
    assert after["raw_record_hexadecimal"] != before["raw_record_hexadecimal"]
    expected["raw_record_hexadecimal"] = after["raw_record_hexadecimal"]
    assert after == expected
    assert before["redundancy"] == after["redundancy"]


@pytest.mark.asyncio
async def test_secondary_dhcp_changes_only_the_interface_word_from_primary():
    primary = await application_request("set_interface_dhcp", host_mac="020000000062", interface="primary")
    secondary = await application_request("set_interface_dhcp", host_mac="020000000062", interface="secondary")
    assert primary[4:6] != secondary[4:6]
    assert secondary[:4] + secondary[6:40] == primary[:4] + primary[6:40]
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


@pytest.mark.parametrize(
    "spec",
    [
        {"command": "set_interface_dhcp", "interface": "secondary"},
        {
            "command": "set_interface_static",
            "interface": "secondary",
            "ip": "198.51.100.102",
            "netmask": "255.255.255.0",
        },
        {"command": "set_dante_redundancy", "mode": "redundant"},
    ],
)
def test_network_setters_do_not_accept_an_unused_revision(spec):
    specification = {**spec, "host_mac": "020000000062", "message_id": 1}
    assert core.build_command(specification)

    with pytest.raises(core.NetaudioCoreError):
        core.build_command({**specification, "record_protocol_identifier": 0x073D})


@pytest.mark.parametrize("interface", ["tertiary", "", 1])
def test_invalid_interface_does_not_select_primary(interface):
    with pytest.raises(core.NetaudioCoreError):
        core.build_command(
            {"command": "set_interface_dhcp", "interface": interface, "host_mac": "020000000062", "message_id": 1}
        )
