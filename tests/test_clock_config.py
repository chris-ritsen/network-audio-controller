import pytest

from netaudio.dante.clock_config import (
    format_clock_source_code,
    format_clock_subdomain,
    parse_clock_source_selection,
    parse_clock_subdomain_selection,
)


def test_clock_source_labels_preserve_raw_integers():
    assert format_clock_source_code(None) == "unknown"
    assert format_clock_source_code(0) == "0 (0x0000)"
    assert format_clock_source_code(1) == "1 (0x0001)"
    assert format_clock_source_code(57044) == "57044 (0xDED4)"


def test_clock_subdomain_labels_unset_ascii_and_binary():
    assert format_clock_subdomain(None) == "unknown"
    assert format_clock_subdomain(bytes(16)) == "unset"
    assert format_clock_subdomain(b"_DFLT" + bytes(11)) == "_DFLT"
    assert format_clock_subdomain(bytes([0, 1, 2]) + bytes(13)) == "unset"
    assert format_clock_subdomain(bytes([0x74, 0x94, 0x11, 0x07, 0x01]) + bytes(11)) == "7494110701"


def test_parse_clock_source_accepts_decimal_and_hex():
    assert parse_clock_source_selection("0") == 0
    assert parse_clock_source_selection("1") == 1
    assert parse_clock_source_selection("0xDED4") == 57044


@pytest.mark.parametrize("value", ["", "65536"])
def test_parse_clock_source_rejects_empty_and_out_of_range(value):
    with pytest.raises(ValueError):
        parse_clock_source_selection(value)


def test_parse_clock_subdomain_accepts_unset_ascii_and_explicit_hex():
    assert parse_clock_subdomain_selection("unset") == bytes(16)
    assert parse_clock_subdomain_selection("_DFLT") == b"_DFLT" + bytes(11)
    assert parse_clock_subdomain_selection("hex:7494110701") == bytes([0x74, 0x94, 0x11, 0x07, 0x01]) + bytes(11)
