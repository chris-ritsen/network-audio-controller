import pytest

from netaudio.dante.sample_rate_pullup import (
    format_supported_sample_rate_pullup_values,
    parse_sample_rate_pullup_selection,
    sample_rate_pullup_label,
)


def test_sample_rate_pullup_labels_use_authentic_tuning_table():
    assert sample_rate_pullup_label(None) == "unknown"
    assert sample_rate_pullup_label(0) == "none"
    assert sample_rate_pullup_label(1) == "+4.1667%"
    assert sample_rate_pullup_label(2) == "+0.1%"
    assert sample_rate_pullup_label(3) == "-0.1%"
    assert sample_rate_pullup_label(4) == "-4.0%"
    assert sample_rate_pullup_label(7) == "raw 7"


def test_supported_sample_rate_pullup_values_are_named():
    assert format_supported_sample_rate_pullup_values(None) == "unknown"
    assert format_supported_sample_rate_pullup_values([]) == "none advertised"
    assert format_supported_sample_rate_pullup_values([0, 1, 2, 3, 4]) == "none, +4.1667%, +0.1%, -0.1%, -4.0%"


def test_parse_sample_rate_pullup_selection_accepts_names_and_raw_integers():
    assert parse_sample_rate_pullup_selection("none") == 0
    assert parse_sample_rate_pullup_selection("+4.1667%") == 1
    assert parse_sample_rate_pullup_selection("+0.1%") == 2
    assert parse_sample_rate_pullup_selection("-0.1%") == 3
    assert parse_sample_rate_pullup_selection("-4.0%") == 4
    assert parse_sample_rate_pullup_selection("0") == 0
    assert parse_sample_rate_pullup_selection("4") == 4


def test_parse_sample_rate_pullup_selection_rejects_empty_and_unknown_text():
    with pytest.raises(ValueError):
        parse_sample_rate_pullup_selection("")
    with pytest.raises(ValueError):
        parse_sample_rate_pullup_selection("plus four")
