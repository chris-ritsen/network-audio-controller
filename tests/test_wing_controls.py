import hashlib
import json
from pathlib import Path

import pytest

from netaudio import core
from netaudio.dante.commands import DanteCommands


EVIDENCE = json.loads((Path(__file__).parent / "fixtures" / "wing_controls.json").read_text())
NETWORK_FIELDS = json.loads((Path(__file__).parent / "fixtures" / "wing_network_fields.json").read_text())


def test_wing_network_fields_match_controller_labels_and_pending_redundancy():
    data = bytes.fromhex(NETWORK_FIELDS["hexadecimal"])
    assert hashlib.sha256(data).hexdigest() == NETWORK_FIELDS["sha256"]
    status = core.parse_response("interface_status", data)
    for interface in status["interfaces"]:
        assert interface["configured"] == NETWORK_FIELDS["configured_fields"][interface["interface"]]
    assert status["redundancy"] == NETWORK_FIELDS["redundancy"]


def test_wing_network_status_does_not_name_unknown_redundancy_flags():
    data = bytearray.fromhex(NETWORK_FIELDS["hexadecimal"])
    data[96:98] = b"\x00\x05"
    assert core.parse_response("interface_status", bytes(data))["redundancy"] is None


def packet(name):
    case = EVIDENCE["cases"][name]
    data = bytes.fromhex(case["hexadecimal"])
    assert hashlib.sha256(data).hexdigest() == case["sha256"]
    assert case["source_timestamp"]
    return data


@pytest.mark.parametrize("direction", ["tx", "rx"])
def test_wing_native_channel_name_request_and_acknowledgement(direction):
    observed = packet(f"{direction}_name_request")
    specification = {
        "command": "set_channel_name",
        "protocol_id": 0x280F,
        "channel_type": direction,
        "channel_number": 1,
        "name": f"parity-{direction}",
        "sequence": 0,
    }
    assert core.build_command(specification) == observed
    assert core.parse_response("result_code", packet(f"{direction}_name_response")) == 1
    specification["protocol_id"] = 0x2810
    with pytest.raises(core.NetaudioCoreError):
        core.build_command(specification)


@pytest.mark.parametrize(
    "case,configured,pending",
    [
        ("redundancy_before", "redundant", False),
        ("redundancy_pending", "switched", True),
        ("redundancy_restored", "redundant", False),
    ],
)
def test_wing_redundancy_distinguishes_current_and_configured(case, configured, pending):
    status = core.parse_response("switch_configuration_status", packet(case))["redundancy"]
    assert status == {
        "current": "redundant",
        "configured": configured,
        "supported": ["switched", "redundant"],
        "reboot_required": pending,
    }


@pytest.mark.parametrize("case,mode", [("redundancy_set", "switched"), ("redundancy_restore", "redundant")])
def test_wing_redundancy_uses_verified_interface_mode_command(case, mode):
    observed = packet(case)
    assert (
        core.build_command(
            {
                "command": "set_dante_redundancy",
                "record_protocol_identifier": 0x073D,
                "mode": mode,
                "sequence": int.from_bytes(observed[4:6], "big"),
                "host_mac": "020000000062",
            }
        )
        == observed
    )


@pytest.mark.parametrize(
    "case,mode,pending",
    [
        ("interface_before", "dynamic", False),
        ("interface_pending", "static", True),
        ("interface_restored", "dynamic", False),
    ],
)
def test_wing_network_status_retains_pending_primary_configuration(case, mode, pending):
    status = core.parse_response("interface_status", packet(case))
    primary, secondary = status["interfaces"]
    assert primary["mode"] == "dynamic"
    assert primary["configured"]["mode"] == mode
    assert primary["reboot_required"] is pending
    assert secondary["configured"] == {"mode": "dynamic"}
    assert status["reboot_required"] is pending


def test_wing_network_status_decodes_by_structure_and_fails_closed_on_bad_descriptors():
    original = packet("interface_pending")
    other_revision = bytearray(original)
    other_revision[24:26] = b"\x07\xfe"
    expected = core.parse_response("interface_status", original)
    expected["record_protocol_identifier"] = 0x07FE
    assert core.parse_response("interface_status", bytes(other_revision)) == expected
    invalid = bytearray(original)
    invalid[94:96] = b"\x00\x00"
    with pytest.raises(core.NetaudioCoreError):
        core.parse_response("interface_status", bytes(invalid))


def test_wing_directory_preserves_opaque_flags_and_zero_maximum_latency():
    directory = core.parse_response("property_directory", packet("directory"))
    assert len(directory["properties"]) == 35
    flags = {entry["property_id"]: entry["flags"] for entry in directory["properties"]}
    assert flags[0x0064] == 1
    assert flags[0x0066] == 3
    settings = core.parse_response("device_settings", packet("settings"))
    assert settings["max_latency_ns"] == 0
    assert settings["active_latency_ns"] == 250000
    assert settings["configured_latency_ns"] == 1000000


@pytest.mark.parametrize("suffix,rate", [("44100", 44100), ("restored", 48000)])
def test_wing_both_advertised_rates_retain_channel_capacity(suffix, rate):
    settings = core.parse_response("device_settings", packet(f"settings_{suffix}"))
    counts = core.parse_response("channel_count", packet(f"capacity_{suffix}"))
    assert settings["sample_rate"] == rate
    assert (counts["rx_count"], counts["tx_count"]) == (64, 64)


def test_interface_probes_get_distinct_sequences():
    commands = DanteCommands()
    first = commands.probe_interface_status("020000000062")
    second = commands.probe_interface_status("020000000062")
    assert first["sequence"] != second["sequence"]
    for specification in (first, second):
        data = core.build_command(specification)
        assert int.from_bytes(data[4:6], "big") == specification["sequence"]
