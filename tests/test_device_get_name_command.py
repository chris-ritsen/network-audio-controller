import codecs
import random
from dataclasses import dataclass

import pytest

from netaudio.dante.const import SERVICE_ARC
from netaudio.dante.device import DanteDevice
from tests.conftest import check_generated_command_payload

DEVICE_NAME_RESPONSE_PAYLOAD_START_OFFSET = 10
DEVICE_NAME_RESPONSE_PAYLOAD_END_OFFSET = -1
MIN_DEVICE_NAME_RESPONSE_LENGTH = 11


@dataclass
class GetNameTestCase:
    device_id: str
    request_fixture: str
    response_fixture: str
    sequence_id: int
    expected_name: str


get_device_name_test_cases = [
    GetNameTestCase(
        device_id="lx_dante_192_168_1_108",
        request_fixture="20250517_200646_215524_192_168_1_108_get_device_name_request.bin",
        response_fixture="20250517_200646_215524_192_168_1_108_get_device_name_response.bin",
        sequence_id=0xC546,
        expected_name="lx-dante",
    ),
    GetNameTestCase(
        device_id="device_192_168_1_36",
        request_fixture="20250517_200646_472412_192_168_1_36_get_device_name_request.bin",
        response_fixture="20250517_200646_472412_192_168_1_36_get_device_name_response.bin",
        sequence_id=0x0142,
        expected_name="avio-usb-2",
    ),
    GetNameTestCase(
        device_id="device_192_168_1_247",
        request_fixture="20250517_200646_439405_192_168_1_247_get_device_name_request.bin",
        response_fixture="20250517_200646_439405_192_168_1_247_get_device_name_response.bin",
        sequence_id=0x1150,
        expected_name="avio-usb-1",
    ),
    GetNameTestCase(
        device_id="device_192_168_1_18",
        request_fixture="20250517_200646_412248_192_168_1_18_get_device_name_request.bin",
        response_fixture="20250517_200646_412248_192_168_1_18_get_device_name_response.bin",
        sequence_id=0x9E7F,
        expected_name="avio-aes3-1",
    ),
    GetNameTestCase(
        device_id="device_192_168_1_94",
        request_fixture="20250517_200646_390658_192_168_1_94_get_device_name_request.bin",
        response_fixture="20250517_200646_390658_192_168_1_94_get_device_name_response.bin",
        sequence_id=0x668B,
        expected_name="avio-usb-3",
    ),
    GetNameTestCase(
        device_id="device_192_168_1_193",
        request_fixture="20250517_200646_356259_192_168_1_193_get_device_name_request.bin",
        response_fixture="20250517_200646_356259_192_168_1_193_get_device_name_response.bin",
        sequence_id=0xA172,
        expected_name="avio-bt-1",
    ),
]


@pytest.mark.parametrize(
    "test_case",
    get_device_name_test_cases,
    ids=[tc.device_id for tc in get_device_name_test_cases],
)
def test_generate_get_device_name_command_payload(
    monkeypatch, load_fixture, test_case: GetNameTestCase
):
    """Test the generation of the 'get_device_name' command payload with a fixed sequence ID."""
    device = DanteDevice()
    monkeypatch.setattr(random, "randint", lambda a, b: test_case.sequence_id)

    hex_command_str, service_type = device.command_device_name()

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
    get_device_name_test_cases,
    ids=[tc.device_id for tc in get_device_name_test_cases],
)
def test_parse_get_device_name_response_payload(
    load_fixture, test_case: GetNameTestCase
):
    """Test the parsing of the 'get_device_name' response payload."""
    raw_response_data = load_fixture(test_case.response_fixture)

    try:
        if len(raw_response_data) < MIN_DEVICE_NAME_RESPONSE_LENGTH:
            pytest.fail(
                f"For {test_case.device_id}, response data from {test_case.response_fixture} is too short (length {len(raw_response_data)}) for slicing [{DEVICE_NAME_RESPONSE_PAYLOAD_START_OFFSET}:{DEVICE_NAME_RESPONSE_PAYLOAD_END_OFFSET}]. Minimum {MIN_DEVICE_NAME_RESPONSE_LENGTH} bytes needed."
            )

        parsed_name = (
            raw_response_data[
                DEVICE_NAME_RESPONSE_PAYLOAD_START_OFFSET:DEVICE_NAME_RESPONSE_PAYLOAD_END_OFFSET
            ]
            .decode("utf-8", errors="replace")
            .strip()
        )
    except UnicodeDecodeError as e:
        pytest.fail(
            f"For {test_case.device_id}, failed to decode device name from {test_case.response_fixture} as UTF-8. Slice: {raw_response_data[DEVICE_NAME_RESPONSE_PAYLOAD_START_OFFSET:DEVICE_NAME_RESPONSE_PAYLOAD_END_OFFSET]}. Error: {e}"
        )
    except Exception as e:
        pytest.fail(
            f"For {test_case.device_id}, an unexpected error occurred during parsing of {test_case.response_fixture}: {e}"
        )

    assert parsed_name == test_case.expected_name, (
        f"For {test_case.device_id}, parsed device name '{parsed_name}' from {test_case.response_fixture} does not match expected '{test_case.expected_name}'."
    )
