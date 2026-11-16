from netaudio.core._audio_choices import SAMPLE_RATE_PULLUP_CHOICES

SAMPLE_RATE_PULLUP_RATIO_LABELS = {choice["value"]: choice["label"] for choice in SAMPLE_RATE_PULLUP_CHOICES}


def sample_rate_pullup_label(raw_value) -> str:
    if raw_value is None:
        return "unknown"
    if isinstance(raw_value, bool) or not isinstance(raw_value, int):
        return "unknown"
    return SAMPLE_RATE_PULLUP_RATIO_LABELS.get(raw_value, "unknown")


def format_supported_sample_rate_pullup_values(raw_values) -> str:
    if raw_values is None:
        return "unknown"
    if not raw_values:
        return "none advertised"
    return ", ".join(sample_rate_pullup_label(raw_value) for raw_value in raw_values)
