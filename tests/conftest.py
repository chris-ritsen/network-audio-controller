import pathlib

import pytest

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def load_fixture():
    def _load_fixture(filename: str) -> bytes:
        fixture_path = FIXTURES_DIR / filename
        if not fixture_path.exists():
            pytest.fail(
                f"Fixture file not found: {fixture_path} (looked in {FIXTURES_DIR.resolve()})"
            )
        return fixture_path.read_bytes()

    return _load_fixture


def check_generated_command_payload(
    generated_hex_payload: bytes,
    actual_service_type: str,
    expected_service_type: str,
    expected_request_fixture: str,
    load_fixture_func,
    context_id: str,
):
    if not isinstance(generated_hex_payload, bytes):
        pytest.fail(
            f"For {context_id}, expected bytes payload but got {type(generated_hex_payload).__name__}"
        )
    generated_payload = generated_hex_payload

    expected_payload_binary = load_fixture_func(expected_request_fixture)

    assert generated_payload == expected_payload_binary, (
        f"For {context_id}, generated request payload does not match the fixture {expected_request_fixture}."
    )
    assert actual_service_type == expected_service_type, (
        f"For {context_id}, expected service type {expected_service_type}, got {actual_service_type}"
    )
