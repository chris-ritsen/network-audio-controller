def audio_capability_fields(status: dict, *, kind: str) -> dict:
    current_field, supported_field = {
        "encoding": ("encoding", "supported_encodings"),
        "sample_rate": ("sample_rate", "supported_sample_rates"),
        "sample_rate_pullup": ("sample_rate_pullup_raw_value", "supported_sample_rate_pullup_raw_values"),
    }[kind]
    fields = {
        current_field: status["current_value"],
        f"requested_{current_field}": status["requested_value"],
        f"{kind}_update_mode": status["update_mode"],
        supported_field: status["available_values"],
    }

    if kind == "sample_rate_pullup":
        fields["sample_rate_pullup_flags"] = status["flags"]
        fields["sample_rate_pullup_host_disabled"] = status.get("host_disabled")

    return fields
