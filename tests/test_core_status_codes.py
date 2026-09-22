import re
from pathlib import Path

import pytest

from netaudio import core
from netaudio.core import binding

HEADER_PATH = Path(__file__).resolve().parents[1] / "packages" / "netaudio-core" / "include" / "netaudio_core.h"
HEADER_STATUS_PATTERN = re.compile(r"^\s*NETAUDIO_STATUS_([A-Z0-9_]+) = (\d+),?$", re.MULTILINE)


def test_native_error_does_not_repeat_its_description_as_detail():
    with pytest.raises(core.NetaudioCoreError) as raised:
        core.build_command({"command": "set_latency", "latency": -1})

    error = raised.value
    assert error.detail
    assert str(error).count(error.detail) == 1
    assert "set_latency" in str(error)


def header_status_codes():
    codes = {int(value): name.lower() for name, value in HEADER_STATUS_PATTERN.findall(HEADER_PATH.read_text())}
    assert codes, "no NETAUDIO_STATUS_ entries found in the generated header"
    return codes


def test_library_status_names_match_the_generated_header():
    library = core.require()
    for code, name in header_status_codes().items():
        assert library.netaudio_status_name(code).decode("ascii") == name
    assert library.netaudio_status_name(len(header_status_codes())).decode("ascii") == "unknown"


def test_errors_use_native_descriptions_and_categories(monkeypatch):
    library = core.require()
    monkeypatch.setattr(binding, "last_error_message", lambda: "readback unavailable")

    for code in header_status_codes():
        error = binding.NetaudioCoreError(code, "inventory")
        description = library.netaudio_status_description(code).decode("utf-8")
        category = library.netaudio_status_category(code).decode("ascii")
        assert str(error) == f"inventory: {description} (readback unavailable)"
        assert error.category == category
        assert "0x" not in description


def test_unknown_native_error_does_not_present_a_raw_code(monkeypatch):
    monkeypatch.setattr(binding, "last_error_message", lambda: "")
    error = binding.NetaudioCoreError(9999, "inventory")
    assert str(error) == "inventory: unknown native error"
    assert error.category == "api"
