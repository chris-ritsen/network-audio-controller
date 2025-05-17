import codecs
import pathlib
import random
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device import DanteDevice
from tests.conftest import check_generated_command_payload

RECEIVERS_RESPONSE_HEADER_SIZE_HEXCHARS = 24
RECEIVERS_CHANNEL_BLOCK_SIZE_HEXCHARS = 40

RECEIVERS_CHANNEL_RELEVANT_DATA_SIZE_HEXCHARS = 32
MAX_CHANNELS_PER_RECEIVER_PAGE_PARSE = 16

FIELD_IDX_CH_NUM = 0
FIELD_IDX_TX_CH_NAME_OFFSET = 3
FIELD_IDX_TX_DEV_NAME_OFFSET = 4
FIELD_IDX_RX_CH_NAME_OFFSET = 5
FIELD_IDX_RX_CH_STATUS_CODE = 6
FIELD_IDX_SUBSCRIPTION_STATUS_CODE = 7
NUM_EXPECTED_FIELDS_PER_CHANNEL = 8

REQUEST_FIXTURE_GET_RECEIVERS = (
    "20250517_200646_499097_avio-usb-2_get_receivers_request.bin"
)
RESPONSE_FIXTURE_GET_RECEIVERS = (
    "20250517_200646_499097_avio-usb-2_get_receivers_response.bin"
)


@dataclass
class GetReceiversTestCase:
    device_id: str
    request_fixture: str
    response_fixture: str
    sequence_id: int
    expected_channels: List[Dict[str, Any]]
    num_to_parse: int


get_receivers_test_cases = [
    GetReceiversTestCase(
        device_id="avio-usb-2",
        request_fixture="20250517_200646_499097_avio-usb-2_get_receivers_request.bin",
        response_fixture="20250517_200646_499097_avio-usb-2_get_receivers_response.bin",
        sequence_id=0xB1C2,
        expected_channels=[
            {
                "number": 1,
                "rx_name": "mic-mix-1",
                "tx_ch_name": "mic-mix-high",
                "tx_dev_name": "lx-dante",
                "rx_status": 257,
                "sub_status": 9,
            },
            {
                "number": 2,
                "rx_name": "mic-mix-2",
                "tx_ch_name": "mic-mix-high",
                "tx_dev_name": "lx-dante",
                "rx_status": 257,
                "sub_status": 9,
            },
        ],
        num_to_parse=2,
    ),
    GetReceiversTestCase(
        device_id="avio-usb-1",
        request_fixture="20250517_200646_463580_avio-usb-1_get_receivers_request.bin",
        response_fixture="20250517_200646_463580_avio-usb-1_get_receivers_response.bin",
        sequence_id=0xFD4E,
        expected_channels=[
            {
                "number": 1,
                "rx_name": "mic-mix-1",
                "tx_ch_name": "mic-mix-high",
                "tx_dev_name": "lx-dante",
                "rx_status": 257,
                "sub_status": 9,
            },
            {
                "number": 2,
                "rx_name": "mic-mix-2",
                "tx_ch_name": "mic-mix-high",
                "tx_dev_name": "lx-dante",
                "rx_status": 257,
                "sub_status": 9,
            },
        ],
        num_to_parse=2,
    ),
    GetReceiversTestCase(
        device_id="avio-aes3-1",
        request_fixture="20250517_200646_429145_avio-aes3-1_get_receivers_request.bin",
        response_fixture="20250517_200646_429145_avio-aes3-1_get_receivers_response.bin",
        sequence_id=0xB099,
        expected_channels=[
            {
                "number": 1,
                "rx_name": "unused-1",
                "tx_ch_name": "linux-mic-mix:high",
                "tx_dev_name": "lx-dante",
                "rx_status": 0,
                "sub_status": 1,
            },
            {
                "number": 2,
                "rx_name": "unused-2",
                "tx_ch_name": "linux-mic-mix:high",
                "tx_dev_name": "lx-dante",
                "rx_status": 0,
                "sub_status": 1,
            },
        ],
        num_to_parse=2,
    ),
    GetReceiversTestCase(
        device_id="avio-usb-3",
        request_fixture="20250517_200646_408078_avio-usb-3_get_receivers_request.bin",
        response_fixture="20250517_200646_408078_avio-usb-3_get_receivers_response.bin",
        sequence_id=0x6309,
        expected_channels=[
            {
                "number": 1,
                "rx_name": "mic-mix-1",
                "tx_ch_name": "mic-mix-high",
                "tx_dev_name": "lx-dante",
                "rx_status": 257,
                "sub_status": 9,
            },
            {
                "number": 2,
                "rx_name": "mic-mix-2",
                "tx_ch_name": "mic-mix-high",
                "tx_dev_name": "lx-dante",
                "rx_status": 257,
                "sub_status": 9,
            },
        ],
        num_to_parse=2,
    ),
    GetReceiversTestCase(
        device_id="avio-bt-1",
        request_fixture="20250517_200646_385043_avio-bt-1_get_receivers_request.bin",
        response_fixture="20250517_200646_385043_avio-bt-1_get_receivers_response.bin",
        sequence_id=0xAC43,
        expected_channels=[
            {
                "number": 1,
                "rx_name": "mic-mix",
                "tx_ch_name": "shelford-channel",
                "tx_dev_name": "a32",
                "rx_status": 0,
                "sub_status": 1,
            }
        ],
        num_to_parse=1,
    ),
    GetReceiversTestCase(
        device_id="lx-dante",
        request_fixture="20250517_200646_289003_lx-dante_get_receivers_request.bin",
        response_fixture="20250517_200646_289003_lx-dante_get_receivers_response.bin",
        sequence_id=0xAD5D,
        expected_channels=[
            {
                "number": 1,
                "rx_name": "wireless-mic:1",
                "tx_ch_name": "01",
                "tx_dev_name": "ad4d",
                "rx_status": 0,
                "sub_status": 1,
            },
            {
                "number": 2,
                "rx_name": "wireless-mic:2",
                "tx_ch_name": "02",
                "tx_dev_name": "ad4d",
                "rx_status": 0,
                "sub_status": 1,
            },
            {
                "number": 3,
                "rx_name": "shelford-channel:minus-6db",
                "tx_ch_name": "shelford-channel-minus-6db",
                "tx_dev_name": "a32",
                "rx_status": 0,
                "sub_status": 1,
            },
            {
                "number": 4,
                "rx_name": "shelford-channel:0dB",
                "tx_ch_name": "shelford-channel",
                "tx_dev_name": "a32",
                "rx_status": 0,
                "sub_status": 1,
            },
            {
                "number": 5,
                "rx_name": "di-box:left",
                "tx_ch_name": "analog-out-19",
                "tx_dev_name": "a32",
                "rx_status": 0,
                "sub_status": 1,
            },
            {
                "number": 6,
                "rx_name": "di-box:right",
                "tx_ch_name": "analog-out-20",
                "tx_dev_name": "a32",
                "rx_status": 0,
                "sub_status": 1,
            },
            {
                "number": 7,
                "rx_name": "windows-gaming:left",
                "tx_ch_name": "windows-gaming:left",
                "tx_dev_name": "avio-usb-1",
                "rx_status": 257,
                "sub_status": 9,
            },
            {
                "number": 8,
                "rx_name": "windows-gaming:right",
                "tx_ch_name": "windows-gaming:right",
                "tx_dev_name": "avio-usb-1",
                "rx_status": 257,
                "sub_status": 9,
            },
            {
                "number": 9,
                "rx_name": "dinet:left",
                "tx_ch_name": "dinet:left",
                "tx_dev_name": "'\ufffd\x03\ufffd\ufffd]0",
                "rx_status": 0,
                "sub_status": 0,
            },
            {
                "number": 10,
                "rx_name": "dinet:right",
                "tx_ch_name": "dinet:right",
                "tx_dev_name": "'\ufffd\x03\ufffd\ufffd]0",
                "rx_status": 0,
                "sub_status": 0,
            },
            {
                "number": 11,
                "rx_name": "macbook-personal:left",
                "tx_ch_name": "macbook-personal:left",
                "tx_dev_name": "avio-usb-2",
                "rx_status": 257,
                "sub_status": 10,
            },
            {
                "number": 12,
                "rx_name": "macbook-personal:right",
                "tx_ch_name": "macbook-personal:right",
                "tx_dev_name": "avio-usb-2",
                "rx_status": 257,
                "sub_status": 10,
            },
            {
                "number": 13,
                "rx_name": "system:capture_13",
                "tx_ch_name": "system:capture_13",
                "tx_dev_name": "'\ufffd\x03\ufffd\ufffd]0",
                "rx_status": 0,
                "sub_status": 0,
            },
            {
                "number": 14,
                "rx_name": "system:capture_14",
                "tx_ch_name": "system:capture_14",
                "tx_dev_name": "'\ufffd\x03\ufffd\ufffd]0",
                "rx_status": 0,
                "sub_status": 0,
            },
            {
                "number": 15,
                "rx_name": "vrroom:left",
                "tx_ch_name": "vrroom:left",
                "tx_dev_name": "avio-aes3-1",
                "rx_status": 257,
                "sub_status": 9,
            },
            {
                "number": 16,
                "rx_name": "vrroom:right",
                "tx_ch_name": "vrroom:right",
                "tx_dev_name": "avio-aes3-1",
                "rx_status": 257,
                "sub_status": 9,
            },
        ],
        num_to_parse=16,
    ),
]


@pytest.mark.parametrize(
    "test_case",
    get_receivers_test_cases,
    ids=[tc.device_id for tc in get_receivers_test_cases],
)
def test_generate_get_receivers_command_payload(
    monkeypatch, load_fixture, test_case: GetReceiversTestCase
):
    """Test the generation of the 'get_receivers' (page 0) command payload."""
    device = DanteDevice()
    monkeypatch.setattr(random, "randint", lambda a, b: test_case.sequence_id)
    hex_command_str, service_type = device.command_receivers(page=0)

    check_generated_command_payload(
        generated_hex_payload=hex_command_str,
        actual_service_type=service_type,
        expected_service_type=SERVICE_ARC,
        expected_request_fixture=test_case.request_fixture,
        load_fixture_func=load_fixture,
        context_id=test_case.device_id,
    )


@pytest.mark.parametrize(
    "test_case",
    get_receivers_test_cases,
    ids=[tc.device_id for tc in get_receivers_test_cases],
)
def test_parse_get_receivers_response_payload(
    load_fixture, test_case: GetReceiversTestCase
):
    """Test parsing of the 'get_receivers' response payload."""
    raw_response_data = load_fixture(test_case.response_fixture)
    hex_rx_response = raw_response_data.hex()
    parsed_channels_info = []

    for i in range(
        0, min(test_case.num_to_parse, MAX_CHANNELS_PER_RECEIVER_PAGE_PARSE)
    ):
        block_start_hex = RECEIVERS_RESPONSE_HEADER_SIZE_HEXCHARS + (
            i * RECEIVERS_CHANNEL_BLOCK_SIZE_HEXCHARS
        )
        block_end_hex = block_start_hex + RECEIVERS_CHANNEL_RELEVANT_DATA_SIZE_HEXCHARS
        channel_data_hex = hex_rx_response[block_start_hex:block_end_hex]
        fields = [
            channel_data_hex[j : j + 4] for j in range(0, len(channel_data_hex), 4)
        ]

        if len(fields) < NUM_EXPECTED_FIELDS_PER_CHANNEL:
            pytest.fail(
                f"For {test_case.device_id}, Ch {i + 1} data block too short. Got {len(fields)} fields, expected {NUM_EXPECTED_FIELDS_PER_CHANNEL}. Block: {channel_data_hex}"
            )

        ch_num_hex = fields[FIELD_IDX_CH_NUM]
        tx_ch_name_offset_hex = fields[FIELD_IDX_TX_CH_NAME_OFFSET]
        tx_dev_name_offset_hex = fields[FIELD_IDX_TX_DEV_NAME_OFFSET]
        rx_ch_name_offset_hex = fields[FIELD_IDX_RX_CH_NAME_OFFSET]
        rx_ch_status_code_hex = fields[FIELD_IDX_RX_CH_STATUS_CODE]
        subscription_status_code_hex = fields[FIELD_IDX_SUBSCRIPTION_STATUS_CODE]

        channel_num = int(ch_num_hex, 16)
        rx_name = _get_label_for_test(hex_rx_response, rx_ch_name_offset_hex)
        tx_dev_name = _get_label_for_test(hex_rx_response, tx_dev_name_offset_hex)
        tx_ch_name = (
            _get_label_for_test(hex_rx_response, tx_ch_name_offset_hex)
            if tx_ch_name_offset_hex != "0000"
            else rx_name
        )
        rx_status = int(rx_ch_status_code_hex, 16)
        sub_status = int(subscription_status_code_hex, 16)

        parsed_channels_info.append(
            {
                "number": channel_num,
                "rx_name": rx_name,
                "tx_ch_name": tx_ch_name,
                "tx_dev_name": tx_dev_name,
                "rx_status": rx_status,
                "sub_status": sub_status,
            }
        )

    assert len(parsed_channels_info) == test_case.num_to_parse, (
        f"For {test_case.device_id}, incorrect number of channels parsed."
    )

    for i in range(test_case.num_to_parse):
        assert parsed_channels_info[i] == test_case.expected_channels[i], (
            f"For {test_case.device_id}, parsed channel data for channel {i + 1} does not match expected. Parsed: {parsed_channels_info[i]}, Expected: {test_case.expected_channels[i]}"
        )


def _get_label_for_test(full_hex_response_str, offset_as_hex_str):
    parsed_label = None
    try:
        start_index_in_hex_str = int(offset_as_hex_str, 16) * 2
        relevant_hex_substring = full_hex_response_str[start_index_in_hex_str:]
        label_bytes = bytes.fromhex(relevant_hex_substring).partition(b"\x00")[0]
        parsed_label = label_bytes.decode("utf-8", errors="replace")
    except ValueError:
        pass
    except Exception:
        pass
    return parsed_label
