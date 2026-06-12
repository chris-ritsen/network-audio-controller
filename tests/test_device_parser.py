from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

from netaudio import core
from netaudio.dante.device import DanteDevice
from netaudio.dante.device_parser import DanteDeviceParser


TX_RAW_4CH_48K = bytes.fromhex(
    "27ff0048aaaa20000001000000010000002c0030"
    "00020000002c003600030000002c003c00040000"
    "002c00420000bb8063682d30310063682d303200"
    "63682d30330063682d303400"
)

TX_FRIENDLY_4CH = bytes.fromhex(
    "27ff005ebbbb20100001000000000001002400000002003100000003004100000004"
    "00526d69632d6d69782d68696768006c696e75782d6d61696e3a6c656674006c69"
    "6e75782d6d61696e3a7269676874006d69632d6d69782d6c6f7700"
)


def make_device(name):
    device = DanteDevice(server_name=f"{name}.local.")
    device.name = name
    return device


@dataclass
class RxParserTestCase:
    device_id: str
    fixture: str
    rx_count: int
    expected_channels: List[Dict[str, Any]]
    expected_subscriptions: List[Dict[str, Any]]


rx_parser_test_cases = [
    RxParserTestCase(
        device_id="avio-usb-2",
        fixture="20250517_200646_499097_avio-usb-2_get_receivers_response.bin",
        rx_count=2,
        expected_channels=[
            {"number": 1, "name": "mic-mix-1", "status_code": 257},
            {"number": 2, "name": "mic-mix-2", "status_code": 257},
        ],
        expected_subscriptions=[
            {"rx": "mic-mix-1", "tx_ch": "mic-mix-high", "tx_dev": "lx-dante", "status": 9},
            {"rx": "mic-mix-2", "tx_ch": "mic-mix-high", "tx_dev": "lx-dante", "status": 9},
        ],
    ),
    RxParserTestCase(
        device_id="avio-usb-1",
        fixture="20250517_200646_463580_avio-usb-1_get_receivers_response.bin",
        rx_count=2,
        expected_channels=[
            {"number": 1, "name": "mic-mix-1", "status_code": 257},
            {"number": 2, "name": "mic-mix-2", "status_code": 257},
        ],
        expected_subscriptions=[
            {"rx": "mic-mix-1", "tx_ch": "mic-mix-high", "tx_dev": "lx-dante", "status": 9},
            {"rx": "mic-mix-2", "tx_ch": "mic-mix-high", "tx_dev": "lx-dante", "status": 9},
        ],
    ),
    RxParserTestCase(
        device_id="avio-aes3-1",
        fixture="20250517_200646_429145_avio-aes3-1_get_receivers_response.bin",
        rx_count=2,
        expected_channels=[
            {"number": 1, "name": "unused-1", "status_code": 0},
            {"number": 2, "name": "unused-2", "status_code": 0},
        ],
        expected_subscriptions=[
            {"rx": "unused-1", "tx_ch": "linux-mic-mix:high", "tx_dev": "lx-dante", "status": 1},
            {"rx": "unused-2", "tx_ch": "linux-mic-mix:high", "tx_dev": "lx-dante", "status": 1},
        ],
    ),
    RxParserTestCase(
        device_id="avio-usb-3",
        fixture="20250517_200646_408078_avio-usb-3_get_receivers_response.bin",
        rx_count=2,
        expected_channels=[
            {"number": 1, "name": "mic-mix-1", "status_code": 257},
            {"number": 2, "name": "mic-mix-2", "status_code": 257},
        ],
        expected_subscriptions=[
            {"rx": "mic-mix-1", "tx_ch": "mic-mix-high", "tx_dev": "lx-dante", "status": 9},
            {"rx": "mic-mix-2", "tx_ch": "mic-mix-high", "tx_dev": "lx-dante", "status": 9},
        ],
    ),
    RxParserTestCase(
        device_id="avio-bt-1",
        fixture="20250517_200646_385043_avio-bt-1_get_receivers_response.bin",
        rx_count=1,
        expected_channels=[
            {"number": 1, "name": "mic-mix", "status_code": 0},
        ],
        expected_subscriptions=[
            {"rx": "mic-mix", "tx_ch": "shelford-channel", "tx_dev": "a32", "status": 1},
        ],
    ),
    RxParserTestCase(
        device_id="lx-dante",
        fixture="20250517_200646_289003_lx-dante_get_receivers_response.bin",
        rx_count=16,
        expected_channels=[
            {"number": 1, "name": "wireless-mic:1", "status_code": 0},
            {"number": 2, "name": "wireless-mic:2", "status_code": 0},
            {"number": 7, "name": "windows-gaming:left", "status_code": 257},
            {"number": 8, "name": "windows-gaming:right", "status_code": 257},
            {"number": 11, "name": "macbook-personal:left", "status_code": 257},
            {"number": 12, "name": "macbook-personal:right", "status_code": 257},
            {"number": 15, "name": "vrroom:left", "status_code": 257},
            {"number": 16, "name": "vrroom:right", "status_code": 257},
        ],
        expected_subscriptions=[
            {"rx": "wireless-mic:1", "tx_ch": "01", "tx_dev": "ad4d", "status": 1},
            {"rx": "windows-gaming:left", "tx_ch": "windows-gaming:left", "tx_dev": "avio-usb-1", "status": 9},
            {"rx": "macbook-personal:left", "tx_ch": "macbook-personal:left", "tx_dev": "avio-usb-2", "status": 10},
            {"rx": "vrroom:left", "tx_ch": "vrroom:left", "tx_dev": "avio-aes3-1", "status": 9},
        ],
    ),
]


@pytest.mark.parametrize(
    "test_case",
    rx_parser_test_cases,
    ids=[tc.device_id for tc in rx_parser_test_cases],
)
def test_build_rx_channels_from_core_records(load_fixture, test_case: RxParserTestCase):
    response_data = load_fixture(test_case.fixture)
    device = make_device(test_case.device_id)

    records = core.parse_page("rx", response_data, 1)
    rx_channels, subscriptions = device._build_rx_from_records(records)

    assert len(rx_channels) == test_case.rx_count

    for expected in test_case.expected_channels:
        ch = rx_channels[expected["number"]]
        assert ch.name == expected["name"], (
            f"{test_case.device_id} ch{expected['number']}: "
            f"name {ch.name!r} != {expected['name']!r}"
        )
        assert ch.number == expected["number"]
        assert ch.channel_type == "rx"
        assert ch.status_code == expected["status_code"]
        assert ch.device is device

    sub_by_rx = {s.rx_channel_name: s for s in subscriptions}
    for expected in test_case.expected_subscriptions:
        sub = sub_by_rx[expected["rx"]]
        assert sub.tx_channel_name == expected["tx_ch"], (
            f"{test_case.device_id} sub {expected['rx']}: "
            f"tx_channel {sub.tx_channel_name!r} != {expected['tx_ch']!r}"
        )
        assert sub.tx_device_name == expected["tx_dev"], (
            f"{test_case.device_id} sub {expected['rx']}: "
            f"tx_device {sub.tx_device_name!r} != {expected['tx_dev']!r}"
        )
        assert sub.status_code == expected["status"]
        assert sub.rx_device_name == test_case.device_id


def test_build_rx_self_subscription_resolves_dot():
    device = make_device("self-device")
    records = [
        {
            "number": 1,
            "rx_channel_name": "loop",
            "tx_channel_name": "loop",
            "tx_device_name": ".",
            "rx_status_code": 257,
            "subscription_status_code": 9,
        }
    ]

    _, subscriptions = device._build_rx_from_records(records)

    assert subscriptions[0].tx_device_name == "self-device"


def _merged_tx_records(raw_response, friendly_response):
    friendly = {}
    if friendly_response is not None:
        for number, friendly_name in core.parse_page("tx_friendly", friendly_response, 1):
            if friendly_name:
                friendly[number] = friendly_name
    records = core.parse_page("tx_info", raw_response, 1)
    for record in records:
        record["friendly_name"] = friendly.get(record["number"])
    return records


def test_build_tx_channels_from_core_records():
    device = make_device("test-device")

    records = _merged_tx_records(TX_RAW_4CH_48K, TX_FRIENDLY_4CH)
    tx_channels = device._build_tx_from_records(records)

    assert len(tx_channels) == 4

    assert tx_channels[1].name == "ch-01"
    assert tx_channels[1].friendly_name == "mic-mix-high"
    assert tx_channels[1].channel_type == "tx"
    assert tx_channels[1].number == 1

    assert tx_channels[2].name == "ch-02"
    assert tx_channels[2].friendly_name == "linux-main:left"

    assert tx_channels[3].name == "ch-03"
    assert tx_channels[3].friendly_name == "linux-main:right"

    assert tx_channels[4].name == "ch-04"
    assert tx_channels[4].friendly_name == "mic-mix-low"


def test_build_tx_channels_without_friendly_names():
    device = make_device("test-device")

    records = _merged_tx_records(TX_RAW_4CH_48K, None)
    tx_channels = device._build_tx_from_records(records)

    assert len(tx_channels) == 4
    assert tx_channels[1].name == "ch-01"
    assert tx_channels[1].friendly_name is None


class TestParseBluetoothStatus:
    def test_connected_extracts_device_name(self, load_fixture):
        response = load_fixture("avio-bt-1_bluetooth_status_connected.bin")
        result = DanteDeviceParser.parse_bluetooth_status(response)
        assert result == "s00pcan-iphone-17"

    def test_disconnected_returns_none(self, load_fixture):
        response = load_fixture("avio-bt-1_bluetooth_status_disconnected.bin")
        result = DanteDeviceParser.parse_bluetooth_status(response)
        assert result is None

    def test_returns_none_for_none(self):
        assert DanteDeviceParser.parse_bluetooth_status(None) is None

    def test_returns_none_for_empty(self):
        assert DanteDeviceParser.parse_bluetooth_status(b"") is None
