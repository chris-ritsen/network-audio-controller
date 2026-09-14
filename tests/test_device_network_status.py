from __future__ import annotations

import time
from pathlib import Path

from netaudio import core
from netaudio.commands.device.network_status import (
    NETWORK_STATUS_DISSECT_HEADERS,
    NETWORK_STATUS_HEADERS,
    _should_probe_switch_configuration,
    network_status_rows,
)
from netaudio.dante.interface_statistics import (
    InterfaceStatisticsErrorBaselines,
    InterfaceStatisticsObservation,
)

FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures" / "interface_statistics"
SWITCH_CONFIGURATION_FIXTURE = Path(__file__).parent / "fixtures" / "switch_configuration" / "ad4d-switched-0014.hex"


def observation(fixture_name: str, source: str = "192.168.1.247") -> InterfaceStatisticsObservation:
    packet = (FIXTURE_DIRECTORY / fixture_name).read_bytes()
    parsed = core.parse_response("interface_statistics_status", packet)
    value = InterfaceStatisticsObservation.from_core(
        parsed,
        source,
        received_at="2026-09-12T12:00:00.000000Z",
        received_monotonic=time.monotonic(),
    )
    return InterfaceStatisticsErrorBaselines().apply(value)


def switch_configuration() -> dict:
    return core.parse_response(
        "switch_configuration_status", bytes.fromhex(SWITCH_CONFIGURATION_FIXTURE.read_text().strip())
    )


def test_network_status_rows_keep_raw_fields_behind_dissect():
    rows = network_status_rows(
        "avio-usb-1",
        "192.168.1.247",
        observation("avio-0040.bin"),
        switch_configuration(),
        dissect=True,
    )

    row = dict(zip(NETWORK_STATUS_HEADERS + NETWORK_STATUS_DISSECT_HEADERS, rows[0]))
    assert row["Selected"] == "yes"
    assert row["Group Pointer"] == "0x0024"
    assert row["Record"] == "0"
    assert row["Discriminator"] == "0x00000001"
    assert row["Size"] == "24"
    assert row["Pointer"] == "0x0028"
    assert row["Transport"] == "conmon_0x0040"
    assert row["Packet Source"] == "192.168.1.247"
    assert row["Switch Mode Codes"] == "0x0001 0x0001"
    assert row["Available Switch Modes"] == "0x0001 Switched, 0x0002 Split/Redundant"
    assert row["Raw Record"] == "00085fd80009926d00000000000000000000000100000064"


def test_network_status_rows_label_missing_responses():
    rows = network_status_rows("avio-usb-1", "192.168.1.247", None, None, dissect=False)

    assert rows == [["avio-usb-1", "192.168.1.247", "", "no response", "", "", "", "", "", "not reported", ""]]


def test_network_status_rows_render_two_interface_groups_without_invented_port_names():
    rows = network_status_rows(
        "lx-dante",
        "192.168.1.34",
        observation("lx-dante-0040.bin", "192.168.1.34"),
        None,
        dissect=False,
    )

    assert [row[2] for row in rows] == ["1", "2"]
    assert rows[0][3] == "16.9886 Mbps"
    assert rows[0][7] == "1 Gbps"
    assert rows[0][9] == "not reported"
    assert rows[1][9] == ""


def test_network_status_rows_report_rates_errors_speed_and_switch_mode():
    rows = network_status_rows(
        "avio-usb-1",
        "192.168.1.247",
        observation("avio-0040.bin"),
        switch_configuration(),
        dissect=False,
    )

    assert rows == [
        [
            "avio-usb-1",
            "192.168.1.247",
            "1",
            "4.39059 Mbps",
            "5.01847 Mbps",
            "0",
            "0",
            "100 Mbps",
            "yes",
            "Switched",
            "Switched, Split/Redundant",
        ]
    ]


def test_managed_single_interface_with_unknown_capability_keeps_observational_probe():
    device = type(
        "Device",
        (),
        {"requires_managed_control": True, "num_networks": None, "interfaces": [{"address": "192.0.2.1"}]},
    )()

    assert _should_probe_switch_configuration(device) is True
    rows = network_status_rows(
        "managed-device",
        "192.0.2.1",
        observation("avio-0040.bin", "192.0.2.1"),
        None,
        dissect=False,
        switch_configuration_applicable=True,
    )
    assert rows[0][9] == "not reported"


def test_managed_multi_interface_device_keeps_switch_probe():
    device = type(
        "Device",
        (),
        {
            "requires_managed_control": True,
            "num_networks": 2,
            "interfaces": [{"address": "192.0.2.1"}, {"address": "192.0.2.2"}],
        },
    )()

    assert _should_probe_switch_configuration(device) is True


def test_two_interfaces_do_not_override_explicit_unsupported_capability():
    device = type(
        "Device",
        (),
        {
            "switch_redundancy_supported": False,
            "redundancy_advertised_support_source": {"fresh": True, "field_reported": True},
            "interfaces": [{"address": "192.0.2.1"}, {"address": "192.0.2.2"}],
        },
    )()

    assert _should_probe_switch_configuration(device) is False


def test_network_status_uses_flag_based_redundancy_without_a_choice_table():
    redundancy = {
        "current_mode": "switched",
        "configured_mode": "switched",
        "available_modes": [
            {"mode": "switched", "label": "Switched"},
            {"mode": "redundant", "label": "Redundant"},
        ],
    }
    rows = network_status_rows("a32", "192.168.1.34", None, None, False, True, redundancy)
    assert rows[0][9] == "Switched"
    assert rows[0][10] == "Switched, Redundant"
