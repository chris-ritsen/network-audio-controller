MICROSECONDS_PER_MILLISECOND = 1_000
NANOSECONDS_PER_MILLISECOND = 1_000_000
STANDARD_LATENCY_CHOICES_MILLISECONDS = (0.15, 0.25, 0.5, 1.0, 2.0, 5.0)

LATENCY_FIELD_NAMES = ("active", "configured", "default", "min", "max")


def nanoseconds_to_milliseconds(value):
    return int(value) / NANOSECONDS_PER_MILLISECOND


def milliseconds_to_microseconds(value):
    return int(round(float(value) * MICROSECONDS_PER_MILLISECOND))


def milliseconds_to_nanoseconds(value):
    return int(round(float(value) * NANOSECONDS_PER_MILLISECOND))


def optional_nanoseconds_to_milliseconds(value):
    if value is None:
        return None
    return nanoseconds_to_milliseconds(value)


def standard_latency_choices_for_range(minimum_latency_milliseconds, maximum_latency_milliseconds):
    if minimum_latency_milliseconds is None or maximum_latency_milliseconds is None:
        return None
    minimum_latency_milliseconds = float(minimum_latency_milliseconds)
    maximum_latency_milliseconds = float(maximum_latency_milliseconds)
    return [
        latency_milliseconds
        for latency_milliseconds in STANDARD_LATENCY_CHOICES_MILLISECONDS
        if minimum_latency_milliseconds <= latency_milliseconds <= maximum_latency_milliseconds
    ]


def latency_state_from_settings(settings):
    if not isinstance(settings, dict):
        return {}

    state = {}
    for field_name in LATENCY_FIELD_NAMES:
        nanoseconds_key = f"{field_name}_latency_ns"
        if nanoseconds_key not in settings:
            continue
        nanoseconds = settings[nanoseconds_key]
        state[nanoseconds_key] = int(nanoseconds) if nanoseconds is not None else None
        state[f"{field_name}_latency_ms"] = optional_nanoseconds_to_milliseconds(nanoseconds)

    minimum = state.get("min_latency_ms")
    maximum = state.get("max_latency_ms")
    choices = standard_latency_choices_for_range(minimum, maximum)
    if choices is None:
        return state

    state["latency_options_ms"] = choices
    state["latency_options_ns"] = [milliseconds_to_nanoseconds(choice) for choice in choices]
    state["latency_options_source"] = "controller_fixed_set_filtered_by_reported_range"
    for field_name in ("active", "configured"):
        value = state.get(f"{field_name}_latency_ms")
        if value is None:
            continue
        state[f"{field_name}_latency_is_standard_choice"] = value in choices
        state[f"{field_name}_latency_within_reported_range"] = minimum <= value <= maximum
    return state


def latency_controls_from_settings(settings):
    controls = {}
    active_latency_present = "active_latency_ns" in settings
    configured_latency_present = "configured_latency_ns" in settings
    active_latency_nanoseconds = settings.get("active_latency_ns")
    configured_latency_nanoseconds = settings.get("configured_latency_ns")

    if active_latency_present:
        controls["active_latency"] = optional_nanoseconds_to_milliseconds(active_latency_nanoseconds)

    if configured_latency_present:
        controls["configured_latency"] = optional_nanoseconds_to_milliseconds(configured_latency_nanoseconds)

    effective_latency_nanoseconds = active_latency_nanoseconds
    if effective_latency_nanoseconds is None:
        effective_latency_nanoseconds = configured_latency_nanoseconds
    if active_latency_present or configured_latency_present:
        controls["latency"] = optional_nanoseconds_to_milliseconds(effective_latency_nanoseconds)

    for field_name in ("default", "min", "max"):
        nanoseconds_key = f"{field_name}_latency_ns"
        if nanoseconds_key in settings:
            controls[f"{field_name}_latency"] = optional_nanoseconds_to_milliseconds(settings[nanoseconds_key])

    return controls


def unavailable_latency_controls():
    return {
        "active_latency": None,
        "configured_latency": None,
        "default_latency": None,
        "latency": None,
        "max_latency": None,
        "min_latency": None,
    }
