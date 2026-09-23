import pytest

from netaudio.dante.clock_config import (
    format_clock_source_code,
    format_clock_subdomain,
    parse_clock_source_selection,
    parse_clock_subdomain_selection,
)


def test_clock_source_labels_use_known_names_and_do_not_expose_unknown_codes():
    assert format_clock_source_code(None) == "unknown"
    assert format_clock_source_code(0) == "internal"
    assert format_clock_source_code(1) == "external/BNC"
    assert format_clock_source_code(2) == "AES"
    assert format_clock_source_code(57044) == "unknown"
    assert format_clock_source_code(True) == "unknown"


def test_clock_subdomain_labels_unset_ascii_and_binary():
    assert format_clock_subdomain(None) == "unknown"
    assert format_clock_subdomain(bytes(16)) == "unset"
    assert format_clock_subdomain(b"_DFLT" + bytes(11)) == "_DFLT"
    assert format_clock_subdomain(bytes([0, 1, 2]) + bytes(13)) == "unknown"
    assert format_clock_subdomain(bytes([0x74, 0x94, 0x11, 0x07, 0x01]) + bytes(11)) == "unknown"


def test_parse_clock_source_accepts_decimal_and_hex():
    assert parse_clock_source_selection("0") == 0
    assert parse_clock_source_selection("1") == 1
    assert parse_clock_source_selection("0x2") == 2


@pytest.mark.parametrize(
    "value", ["", "65536", "3", "0xDED4", "0x-1", pytest.param("1" + "0" * 400, id="oversized-number")]
)
def test_parse_clock_source_rejects_empty_and_out_of_range(value):
    with pytest.raises(ValueError):
        parse_clock_source_selection(value)


def test_native_clock_source_choices_require_advertisement_and_ignore_unknown_variants():
    from netaudio import core

    presentation = core.clock_sources({"current": 2, "supported": [2, 2, 99, True]})

    assert presentation["current"] == "AES"
    assert presentation["choices"] == [{"code": 0, "label": "internal"}, {"code": 2, "label": "AES"}]
    assert core.clock_sources({"current": 99, "supported": []})["current"] is None


@pytest.mark.parametrize("current", [None, True, -1, 65536, "1", {}, []])
def test_clock_source_malformed_observations_do_not_establish_support(current):
    from netaudio import core

    assert core.clock_sources({"current": current, "supported": [current]}) == {
        "current": None,
        "choices": [{"code": 0, "label": "internal"}],
    }


def test_parse_clock_subdomain_accepts_unset_ascii_and_explicit_hex():
    assert parse_clock_subdomain_selection("unset") == bytes(16)
    assert parse_clock_subdomain_selection("_DFLT") == b"_DFLT" + bytes(11)
    assert parse_clock_subdomain_selection("hex:7494110701") == bytes([0x74, 0x94, 0x11, 0x07, 0x01]) + bytes(11)


@pytest.mark.parametrize("value", ["a" * 16, "hex:610062", "hex:" + "61" * 16])
def test_clock_subdomain_input_rejects_names_the_device_cannot_encode(value):
    with pytest.raises(ValueError):
        parse_clock_subdomain_selection(value)


@pytest.mark.parametrize("value", [[True], [256], [-1], [97, 0, 98], "a" * 16, "\u2603"])
def test_presets_reject_invalid_subdomains_without_lossy_conversion(value):
    from netaudio.presets.schema import normalize_device_config

    with pytest.raises(ValueError, match="clock_subdomain"):
        normalize_device_config({"name": "Receiver", "clock_subdomain": value})


@pytest.mark.parametrize("value", ["house", b"house", bytearray(b"house"), [104, 111, 117, 115, 101]])
def test_preset_and_native_clock_inputs_share_normalization(value):
    from netaudio import core
    from netaudio.presets.schema import normalize_device_config

    expected = list(b"house" + bytes(11))
    assert core.normalize_clock_subdomain(value) == bytes(expected)
    assert normalize_device_config({"name": "Receiver", "clock_subdomain": value})["clock_subdomain"] == expected
