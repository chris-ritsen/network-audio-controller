from pathlib import Path

from netaudio import core
from netaudio.commands.device.network_status import (
    NETWORK_STATUS_DISSECT_HEADERS,
    NETWORK_STATUS_HEADERS,
    _should_probe_switch_configuration,
    _without_port_column,
    network_status_rows,
)
from netaudio.dante.link_status import LinkStatusObservation, LinkStatusRecord

SWITCH_CONFIGURATION_FIXTURE = Path(__file__).parent / "fixtures" / "switch_configuration" / "ad4d-switched-0014.hex"


def make_link_status(records: list[LinkStatusRecord]) -> LinkStatusObservation:
    return LinkStatusObservation(
        record_count=len(records),
        record_pointers=tuple(record.record_pointer for record in records),
        records=tuple(records),
    )


def make_record(
    record_index: int,
    record_pointer: int,
    link_up: bool,
    speed: int,
    label: str | None = None,
) -> LinkStatusRecord:
    return LinkStatusRecord(
        record_index=record_index,
        record_pointer=record_pointer,
        record_size_bytes=24,
        label=label,
        unmapped_prefix_words=(0x00085CF2, 0x0009A005, 0, 0),
        raw_link_status_word=1 if link_up else 0,
        link_up=link_up,
        link_speed_megabits_per_second=speed,
        unmapped_trailing_hexadecimal="",
        raw_record_hexadecimal="00085cf20009a00500000000000000000000000100000064",
    )


def switch_configuration() -> dict:
    return core.parse_response(
        "switch_configuration_status", bytes.fromhex(SWITCH_CONFIGURATION_FIXTURE.read_text().strip())
    )


def test_network_status_rows_keep_raw_fields_behind_dissect():
    link_status = make_link_status([make_record(0, 0x0028, True, 100)])

    rows = network_status_rows("avio-usb-1", "192.168.1.247", link_status, switch_configuration(), dissect=True)

    row = dict(zip(NETWORK_STATUS_HEADERS + NETWORK_STATUS_DISSECT_HEADERS, rows[0]))
    assert row["Record"] == "0"
    assert row["Status Word"] == "0x00000001"
    assert row["Size"] == "24"
    assert row["Pointer"] == "0x0028"
    assert row["Prefix Words"] == "0x00085CF2 0x0009A005 0x00000000 0x00000000"
    assert row["Switch Mode Codes"] == "0x0001 0x0001"
    assert row["Available Switch Modes"] == "0x0001 Switched, 0x0002 Split/Redundant"
    assert row["Raw Record"] == "00085cf20009a00500000000000000000000000100000064"


def test_network_status_rows_label_missing_responses():
    rows = network_status_rows("avio-usb-1", "192.168.1.247", None, None, dissect=False)

    assert rows == [["avio-usb-1", "192.168.1.247", "", "no response", "", "no response", ""]]


def test_network_status_rows_render_switch_ports_in_words():
    link_status = make_link_status(
        [
            make_record(0, 0x002C, True, 1000, "selected_link"),
            make_record(1, 0x0044, True, 1000, "switch_port_0"),
            make_record(2, 0x005C, False, 0, "switch_port_3"),
        ]
    )

    rows = network_status_rows("a32", "192.168.1.34", link_status, None, dissect=False)

    assert [row[2:5] for row in rows] == [
        ["selected link", "up", "1 Gbps"],
        ["switch port 0", "up", "1 Gbps"],
        ["switch port 3", "down", "0 Mbps"],
    ]
    assert rows[0][5] == "no response"
    assert rows[1][5] == ""


def test_network_status_rows_report_link_and_switch_mode_in_words():
    link_status = make_link_status([make_record(0, 0x0028, True, 100)])

    rows = network_status_rows("avio-usb-1", "192.168.1.247", link_status, switch_configuration(), dissect=False)

    assert rows == [
        ["avio-usb-1", "192.168.1.247", "", "up", "100 Mbps", "Switched", "Switched, Split/Redundant"],
    ]


def test_managed_single_interface_device_skips_unavailable_switch_probe():
    device = type(
        "Device",
        (),
        {"requires_managed_control": True, "num_networks": None, "interfaces": [{"address": "192.0.2.1"}]},
    )()

    assert _should_probe_switch_configuration(device) is False
    link_status = make_link_status([make_record(0, 0x0028, True, 100)])
    rows = network_status_rows(
        "managed-device",
        "192.0.2.1",
        link_status,
        None,
        dissect=False,
        switch_configuration_applicable=False,
    )
    assert rows[0][5] == "N/A"


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


def test_without_port_column_drops_the_column_only_when_no_row_names_a_port():
    headers = list(NETWORK_STATUS_HEADERS)
    unlabeled_rows = [["avio-usb-1", "192.168.1.247", "", "up", "100 Mbps", "", ""]]
    labeled_rows = [["a32", "192.168.1.34", "selected link", "up", "1 Gbps", "", ""]]

    kept_headers, kept_rows = _without_port_column(headers, unlabeled_rows)
    assert "Port" not in kept_headers
    assert kept_rows == [["avio-usb-1", "192.168.1.247", "up", "100 Mbps", "", ""]]

    kept_headers, kept_rows = _without_port_column(headers, labeled_rows)
    assert kept_headers == headers
    assert kept_rows == labeled_rows
