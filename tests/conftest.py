import codecs
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
    generated_hex_payload: str,
    actual_service_type: str,
    expected_service_type: str,
    expected_request_fixture: str,
    load_fixture_func,
    context_id: str,
):
    """
    Helper function to check a generated command payload against a fixture.
    Decodes the hex payload, compares with the loaded fixture, and checks service type.
    """
    try:
        generated_payload_binary = codecs.decode(generated_hex_payload, "hex")
    except Exception as e:
        pytest.fail(
            f"For {context_id}, failed to decode generated hex command string to binary: {e}"
        )

    expected_payload_binary = load_fixture_func(expected_request_fixture)

    assert generated_payload_binary == expected_payload_binary, (
        f"For {context_id}, generated request payload does not match the fixture {expected_request_fixture}."
    )
    assert actual_service_type == expected_service_type, (
        f"For {context_id}, expected service type {expected_service_type}, got {actual_service_type}"
    )
