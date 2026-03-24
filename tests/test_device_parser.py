from dataclasses import dataclass
from typing import Any, Dict, List
from unittest.mock import Mock

import pytest
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


class TestDanteDeviceParser:
    @pytest.fixture
    def parser(self):
        return DanteDeviceParser()

    @pytest.fixture
    def mock_device(self):
        device = Mock()
        device.name = "test-device"
        device.rx_count = 2
        device.tx_count = 2
        device.sample_rate = None
        return device

    def test_get_string_at_offset_with_valid_offset(self, parser):
        data = b"Hello\x00World\x00Test"
        assert parser._get_string_at_offset(data, 6) == "World"
        assert parser._get_string_at_offset(data, 12) == "Test"

    def test_get_string_at_offset_with_invalid_offset(self, parser):
        data = b"Hello"
        assert parser._get_string_at_offset(data, 10) is None

    def test_get_string_at_offset_with_zero_offset_returns_none(self, parser):
        data = b"Hello"
        assert parser._get_string_at_offset(data, 0) is None

    def test_get_string_at_offset_with_no_null_terminator(self, parser):
        data = b"_HelloWorld"
        assert parser._get_string_at_offset(data, 1) == "HelloWorld"

    @pytest.mark.asyncio
    async def test_get_rx_channels_with_real_data(
        self, parser, mock_device, load_fixture
    ):
        response_data = load_fixture(
            "20250517_200646_499097_avio-usb-2_get_receivers_response.bin"
        )

        mock_device.rx_count = 2
        mock_device.commands.command_receivers = Mock(return_value=("command", "service"))

        async def mock_dante_command(*args, **kwargs):
            return response_data

        rx_channels, subscriptions = await parser.get_rx_channels(
            mock_device, mock_dante_command
        )

        assert len(rx_channels) == 2
        assert len(subscriptions) == 2

        assert rx_channels[1].name == "mic-mix-1"
        assert rx_channels[1].number == 1
        assert rx_channels[1].channel_type == "rx"
        assert rx_channels[1].device == mock_device

        assert rx_channels[2].name == "mic-mix-2"
        assert rx_channels[2].number == 2

        assert subscriptions[0].rx_channel_name == "mic-mix-1"
        assert subscriptions[0].tx_channel_name == "mic-mix-high"
        assert subscriptions[0].tx_device_name == "lx-dante"
        assert subscriptions[0].rx_device_name == "test-device"

        assert subscriptions[1].rx_channel_name == "mic-mix-2"
        assert subscriptions[1].tx_channel_name == "mic-mix-high"
        assert subscriptions[1].tx_device_name == "lx-dante"

    @pytest.mark.asyncio
    async def test_get_rx_channels_with_no_response(self, parser, mock_device):
        async def mock_dante_command(*args, **kwargs):
            return None

        mock_device.commands.command_receivers = Mock(return_value=("command", "service"))

        rx_channels, subscriptions = await parser.get_rx_channels(
            mock_device, mock_dante_command
        )

        assert len(rx_channels) == 0
        assert len(subscriptions) == 0

    @pytest.mark.asyncio
    async def test_get_rx_channels_extracts_sample_rate(
        self, parser, mock_device, load_fixture
    ):
        response_data = load_fixture(
            "20250517_200646_289003_lx-dante_get_receivers_response.bin"
        )

        mock_device.rx_count = 16
        mock_device.commands.command_receivers = Mock(return_value=("command", "service"))
        mock_device.sample_rate = None

        async def mock_dante_command(*args, **kwargs):
            return response_data

        await parser.get_rx_channels(mock_device, mock_dante_command)

        assert mock_device.sample_rate is not None

    def test_parse_volume(self, parser, mock_device):
        mock_device.rx_count_raw = 2
        mock_device.tx_count_raw = 2

        tx_channel_1 = Mock()
        tx_channel_1.number = 1
        tx_channel_2 = Mock()
        tx_channel_2.number = 2

        rx_channel_1 = Mock()
        rx_channel_1.number = 1
        rx_channel_2 = Mock()
        rx_channel_2.number = 2

        tx_channels = {1: tx_channel_1, 2: tx_channel_2}
        rx_channels = {1: rx_channel_1, 2: rx_channel_2}

        volume_data = b"\x00\x00\x00\x00\x50\x60\x70\x80\x00"

        parser.parse_volume(
            volume_data,
            mock_device.rx_count_raw,
            mock_device.tx_count_raw,
            tx_channels,
            rx_channels,
        )

        assert tx_channel_1.volume == 0x50
        assert tx_channel_2.volume == 0x60
        assert rx_channel_1.volume == 0x70
        assert rx_channel_2.volume == 0x80


@dataclass
class RxParserTestCase:
    device_id: str
    fixture: str
    rx_count: int
    expected_channels: List[Dict[str, Any]]
    expected_subscriptions: List[Dict[str, Any]]


rx_parser_test_cases = [
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
@pytest.mark.asyncio
async def test_get_rx_channels_parser(load_fixture, test_case: RxParserTestCase):
    response_data = load_fixture(test_case.fixture)
    parser = DanteDeviceParser()

    device = Mock()
    device.name = test_case.device_id
    device.rx_count = test_case.rx_count
    device.sample_rate = None
    device.error = None
    device.commands.command_receivers = Mock(return_value=("command", "service"))

    async def mock_dante_command(*args, **kwargs):
        return response_data

    rx_channels, subscriptions = await parser.get_rx_channels(
        device, mock_dante_command
    )

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


@pytest.mark.asyncio
async def test_get_tx_channels_with_synthetic_data():
    parser = DanteDeviceParser()

    device = Mock()
    device.tx_count = 4
    device.sample_rate = None
    device.error = None
    device.commands.command_transmitters = Mock(return_value=("cmd", "svc"))

    async def mock_dante_command(*args, **kwargs):
        name = kwargs.get("logical_command_name", "")
        if "friendly" in name:
            return TX_FRIENDLY_4CH
        return TX_RAW_4CH_48K

    tx_channels = await parser.get_tx_channels(device, mock_dante_command)

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


@pytest.mark.asyncio
async def test_get_tx_channels_extracts_sample_rate():
    parser = DanteDeviceParser()

    device = Mock()
    device.tx_count = 4
    device.sample_rate = None
    device.error = None
    device.commands.command_transmitters = Mock(return_value=("cmd", "svc"))

    async def mock_dante_command(*args, **kwargs):
        name = kwargs.get("logical_command_name", "")
        if "friendly" in name:
            return TX_FRIENDLY_4CH
        return TX_RAW_4CH_48K

    await parser.get_tx_channels(device, mock_dante_command)

    assert device.sample_rate == 48000


@pytest.mark.asyncio
async def test_get_tx_channels_with_no_response():
    parser = DanteDeviceParser()

    device = Mock()
    device.tx_count = 4
    device.sample_rate = None
    device.error = None
    device.commands.command_transmitters = Mock(return_value=("cmd", "svc"))

    async def mock_dante_command(*args, **kwargs):
        return None

    tx_channels = await parser.get_tx_channels(device, mock_dante_command)

    assert len(tx_channels) == 0


@pytest.mark.asyncio
async def test_get_tx_channels_without_friendly_response():
    parser = DanteDeviceParser()

    device = Mock()
    device.tx_count = 4
    device.sample_rate = None
    device.error = None
    device.commands.command_transmitters = Mock(return_value=("cmd", "svc"))

    async def mock_dante_command(*args, **kwargs):
        name = kwargs.get("logical_command_name", "")
        if "friendly" in name:
            return None
        return TX_RAW_4CH_48K

    tx_channels = await parser.get_tx_channels(device, mock_dante_command)

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


