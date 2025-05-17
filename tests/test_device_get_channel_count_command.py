import codecs
import random
from dataclasses import dataclass

import pytest

from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device import DanteDevice
from tests.conftest import check_generated_command_payload

CHANNEL_COUNT_RESPONSE_TX_START_OFFSET = 13
CHANNEL_COUNT_RESPONSE_TX_END_OFFSET = 14
CHANNEL_COUNT_RESPONSE_RX_START_OFFSET = 15
CHANNEL_COUNT_RESPONSE_RX_END_OFFSET = 16
MIN_CHANNEL_COUNT_RESPONSE_LENGTH = 16


@dataclass
class GetChannelCountTestCase:
    device_id: str
    request_fixture: str
    response_fixture: str
    sequence_id: int
    expected_tx: int
    expected_rx: int


get_channel_count_test_cases = [
    GetChannelCountTestCase(
        device_id="lx_dante",
        request_fixture="20250517_200646_226874_lx-dante_get_channel_count_request.bin",
        response_fixture="20250517_200646_226874_lx-dante_get_channel_count_response.bin",
        sequence_id=0xF215,
        expected_tx=128,
        expected_rx=128,
    ),
    GetChannelCountTestCase(
        device_id="avio-usb-2",
        request_fixture="20250517_200646_478965_avio-usb-2_get_channel_count_request.bin",
        response_fixture="20250517_200646_478965_avio-usb-2_get_channel_count_response.bin",
        sequence_id=0xD82F,
        expected_tx=2,
        expected_rx=2,
    ),
    GetChannelCountTestCase(
        device_id="avio-usb-1",
        request_fixture="20250517_200646_445946_avio-usb-1_get_channel_count_request.bin",
        response_fixture="20250517_200646_445946_avio-usb-1_get_channel_count_response.bin",
        sequence_id=0xBBA2,
        expected_tx=2,
        expected_rx=2,
    ),
    GetChannelCountTestCase(
        device_id="avio-aes3-1",
        request_fixture="20250517_200646_416392_avio-aes3-1_get_channel_count_request.bin",
        response_fixture="20250517_200646_416392_avio-aes3-1_get_channel_count_response.bin",
        sequence_id=0x5E02,
        expected_tx=2,
        expected_rx=2,
    ),
    GetChannelCountTestCase(
        device_id="avio-usb-3",
        request_fixture="20250517_200646_396138_avio-usb-3_get_channel_count_request.bin",
        response_fixture="20250517_200646_396138_avio-usb-3_get_channel_count_response.bin",
        sequence_id=0x4144,
        expected_tx=2,
        expected_rx=2,
    ),
    GetChannelCountTestCase(
        device_id="avio-bt-1",
        request_fixture="20250517_200646_363064_avio-bt-1_get_channel_count_request.bin",
        response_fixture="20250517_200646_363064_avio-bt-1_get_channel_count_response.bin",
        sequence_id=0x409B,
        expected_tx=2,
        expected_rx=1,
    ),
]


@pytest.mark.parametrize(
    "test_case",
    get_channel_count_test_cases,
    ids=[tc.device_id for tc in get_channel_count_test_cases],
)
def test_generate_get_channel_count_command_payload(
    monkeypatch, load_fixture, test_case: GetChannelCountTestCase
):
    """Test the generation of the 'get_channel_count' command payload."""
    device = DanteDevice()
    monkeypatch.setattr(random, "randint", lambda a, b: test_case.sequence_id)
    hex_command_str, service_type = device.command_channel_count()

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
    get_channel_count_test_cases,
    ids=[tc.device_id for tc in get_channel_count_test_cases],
)
def test_parse_get_channel_count_response_payload(
    load_fixture, test_case: GetChannelCountTestCase
):
    """Test the parsing of the 'get_channel_count' response payload."""
    raw_response_data = load_fixture(test_case.response_fixture)
    try:
        if len(raw_response_data) < MIN_CHANNEL_COUNT_RESPONSE_LENGTH:
            pytest.fail(
                f"For {test_case.device_id}, response from {test_case.response_fixture} too short (len {len(raw_response_data)}). Min {MIN_CHANNEL_COUNT_RESPONSE_LENGTH} bytes needed."
            )
        parsed_tx_count = int.from_bytes(
            raw_response_data[
                CHANNEL_COUNT_RESPONSE_TX_START_OFFSET:CHANNEL_COUNT_RESPONSE_TX_END_OFFSET
            ],
            "big",
        )
        parsed_rx_count = int.from_bytes(
            raw_response_data[
                CHANNEL_COUNT_RESPONSE_RX_START_OFFSET:CHANNEL_COUNT_RESPONSE_RX_END_OFFSET
            ],
            "big",
        )
    except Exception as e:
        pytest.fail(
            f"For {test_case.device_id}, error parsing {test_case.response_fixture}: {e}"
        )

    assert parsed_tx_count == test_case.expected_tx, (
        f"For {test_case.device_id}, parsed Tx count {parsed_tx_count} from {test_case.response_fixture} != expected {test_case.expected_tx}."
    )
    assert parsed_rx_count == test_case.expected_rx, (
        f"For {test_case.device_id}, parsed Rx count {parsed_rx_count} from {test_case.response_fixture} != expected {test_case.expected_rx}."
    )
