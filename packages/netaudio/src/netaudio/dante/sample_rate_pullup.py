SAMPLE_RATE_PULLUP_RATIO_LABELS = {
    0: "none",
    1: "+4.1667%",
    2: "+0.1%",
    3: "-0.1%",
    4: "-4.0%",
}

_SAMPLE_RATE_PULLUP_SELECTION_ALIASES = {
    "none": 0,
    "+4.1667%": 1,
    "4.1667%": 1,
    "+0.1%": 2,
    "0.1%": 2,
    "-0.1%": 3,
    "-4.0%": 4,
    "-4%": 4,
}


def sample_rate_pullup_label(raw_value) -> str:
    if raw_value is None:
        return "unknown"
    if isinstance(raw_value, bool) or not isinstance(raw_value, int):
        return "unknown"
    return SAMPLE_RATE_PULLUP_RATIO_LABELS.get(raw_value, f"raw {raw_value}")


def format_supported_sample_rate_pullup_values(raw_values) -> str:
    if raw_values is None:
        return "unknown"
    if not raw_values:
        return "none advertised"
    return ", ".join(sample_rate_pullup_label(raw_value) for raw_value in raw_values)


def parse_sample_rate_pullup_selection(text: str) -> int:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("sample-rate pull-up value is required")
    stripped = text.strip()
    if stripped.isdigit():
        return int(stripped)
    alias = _SAMPLE_RATE_PULLUP_SELECTION_ALIASES.get(stripped.lower())
    if alias is None:
        alias = _SAMPLE_RATE_PULLUP_SELECTION_ALIASES.get(stripped)
    if alias is None:
        raise ValueError("sample-rate pull-up must be none, +4.1667%, +0.1%, -0.1%, -4.0%, or a raw integer")
    return alias
