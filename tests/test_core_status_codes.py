import re
from pathlib import Path

import pytest

from netaudio import core
from netaudio.core import binding

HEADER_PATH = Path(__file__).resolve().parents[1] / "packages" / "netaudio-core" / "include" / "netaudio_core.h"
HEADER_STATUS_PATTERN = re.compile(r"^\s*NETAUDIO_STATUS_([A-Z0-9_]+) = (\d+),?$", re.MULTILINE)


def header_status_codes():
    codes = {int(value): name.lower() for name, value in HEADER_STATUS_PATTERN.findall(HEADER_PATH.read_text())}
    assert codes, "no NETAUDIO_STATUS_ entries found in the generated header"
    return codes


def test_binding_status_table_mirrors_the_generated_header():
    header_codes = header_status_codes()
    assert header_codes[0] == "ok"
    assert set(binding._STATUS_NAMES) == set(header_codes) - {0}
    assert sorted(header_codes) == list(range(len(header_codes)))


@pytest.mark.skipif(not core.available(), reason="netaudio-core library not available")
def test_library_status_names_match_the_generated_header():
    library = core.require()
    library.netaudio_status_name.restype = __import__("ctypes").c_char_p
    for code, name in header_status_codes().items():
        assert library.netaudio_status_name(code).decode("ascii") == name
    assert library.netaudio_status_name(len(header_status_codes())).decode("ascii") == "unknown"
