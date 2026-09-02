import json
from pathlib import Path

import netaudio.cli_support.execution as common_module
import pytest
import typer
from netaudio import core
from netaudio.cli import OutputFormat, state
from netaudio.commands.device.clock import _matching_leader_name, clock
from netaudio.dante.application import DanteApplication
from netaudio.dante.clock_identity import canonical_clock_identity
from netaudio.dante.device import DanteDevice
from typer.testing import CliRunner

from tests.status_test_support import receive_packets

runner = CliRunner()
clock_app = typer.Typer()
clock_app.command("clock")(clock)
FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "clock_leader_association"


def clock_device(server_name, name, role, clock_identity=None, leader_clock_identity=None):
    device = DanteDevice(server_name=server_name)
    device.name = name
    device.ipv4 = "192.168.1.10"
    device.clock_role = role
    device.clock_identity = clock_identity
    device.leader_clock_identity = leader_clock_identity
    return device


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "001dc150692",
        "not-hexadecimal",
        bytes(6),
        bytes(5),
        6,
        [0, 29, 193, 80, 105, 256],
        [0, 29, 193, 80, 105, -1],
        [0, 29, 193, 80, 105, True],
    ],
)
def test_invalid_clock_identities_fail_closed(value):
    assert canonical_clock_identity(value) is None


def test_identity_association_distinguishes_two_simultaneous_leaders(monkeypatch):
    devices = {
        "leader-a.local.": clock_device("leader-a.local.", "leader-a", "Leader", "001dc150692e"),
        "leader-b.local.": clock_device("leader-b.local.", "leader-b", "Leader", "001dc1507b8d"),
        "follower-a.local.": clock_device(
            "follower-a.local.", "follower-a", "Follower", "001dc1510295", "001dc150692e"
        ),
        "follower-b.local.": clock_device(
            "follower-b.local.", "follower-b", "Follower", "001dc153ef37", "001dc1507b8d"
        ),
    }

    async def load_display_devices(application):
        return devices

    monkeypatch.setattr(common_module, "_load_display_devices", load_display_devices)
    monkeypatch.setattr(state, "output_format", OutputFormat.json)

    result = runner.invoke(clock_app, [])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["follower-a.local."]["leader"] == "leader-a"
    assert payload["follower-b.local."]["leader"] == "leader-b"
    assert payload["follower-a.local."]["leader_clock_identity"] == "001dc150692e"
    assert payload["leader-a.local."]["leader"] is None


def test_missing_identity_never_selects_the_only_leader():
    entries = [
        {"name": "leader", "clock_role": "Leader", "clock_identity": "001dc150692e"},
        {"name": "follower", "clock_role": "Follower", "leader_clock_identity": None},
    ]

    assert _matching_leader_name(entries, entries[1]) is None


def test_unknown_identity_does_not_fall_back_to_role_or_subdomain():
    entries = [
        {
            "name": "leader",
            "clock_role": "Leader",
            "clock_identity": "001dc150692e",
            "clock_subdomain": [65, 0],
        },
        {
            "name": "follower",
            "clock_role": "Follower",
            "leader_clock_identity": "001dc1507b8d",
            "clock_subdomain": [65, 0],
        },
    ]

    assert _matching_leader_name(entries, entries[1]) is None


def test_duplicate_identity_is_ambiguous():
    entries = [
        {"name": "leader-a", "clock_role": "Leader", "clock_identity": "001dc150692e"},
        {"name": "leader-b", "clock_role": "Leader", "clock_identity": "001dc150692e"},
        {"name": "follower", "clock_role": "Follower", "leader_clock_identity": "001dc150692e"},
    ]

    assert _matching_leader_name(entries, entries[2]) is None


def test_physical_follower_publications_track_selected_leader_identity():
    domain_a_packet = (FIXTURE_DIRECTORY / "follower-domain-a-0020.bin").read_bytes()
    domain_b_packet = (FIXTURE_DIRECTORY / "follower-domain-b-0020.bin").read_bytes()

    domain_a = core.parse_response("ptp_clock_status", domain_a_packet)
    domain_b = core.parse_response("ptp_clock_status", domain_b_packet)

    assert domain_a["clock_identity"] == domain_b["clock_identity"] == [0, 29, 193, 81, 2, 149]
    assert domain_a["leader_clock_identity"] == [0, 29, 193, 80, 105, 46]
    assert domain_b["leader_clock_identity"] == [0, 29, 193, 80, 123, 141]
    assert bytes(domain_a["clock_subdomain"]).rstrip(b"\0") == b"NA-CLOCK-A"
    assert bytes(domain_b["clock_subdomain"]).rstrip(b"\0") == b"NA-CLOCK-B"


def test_notification_state_tracks_the_physical_follower_transition():
    device = clock_device("follower.local.", "follower", "Follower")
    device.ipv4 = "192.168.1.94"
    application = DanteApplication()
    application.attach_devices({device.server_name: device})

    receive_packets(
        application,
        [(FIXTURE_DIRECTORY / "follower-domain-a-0020.bin").read_bytes()],
        ("192.168.1.94", 8700),
    )
    assert device.clock_identity == "001dc1510295"
    assert device.leader_clock_identity == "001dc150692e"

    receive_packets(
        application,
        [(FIXTURE_DIRECTORY / "follower-domain-b-0020.bin").read_bytes()],
        ("192.168.1.94", 8700),
    )
    assert device.clock_identity == "001dc1510295"
    assert device.leader_clock_identity == "001dc1507b8d"
