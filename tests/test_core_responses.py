import json
from pathlib import Path

import pytest

from netaudio import core

if not core.available():
    pytest.skip("netaudio-core library not available", allow_module_level=True)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
GOLDEN = json.loads((FIXTURES_DIR / "core_responses_golden.json").read_text())


def _input_bytes(name, entry):
    if "hex" in entry:
        return bytes.fromhex(entry["hex"])
    return (FIXTURES_DIR / name).read_bytes()


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_parse_response_matches_golden(name):
    entry = GOLDEN[name]
    parsed = core.parse_response(entry["kind"], _input_bytes(name, entry))
    assert parsed == entry["parsed"]
