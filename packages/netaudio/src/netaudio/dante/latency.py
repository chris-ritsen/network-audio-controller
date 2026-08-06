NANOSECONDS_PER_MILLISECOND = 1_000_000
MICROSECONDS_PER_MILLISECOND = 1_000
STANDARD_LATENCY_CHOICES_MILLISECONDS = (0.15, 0.25, 0.5, 1.0, 2.0, 5.0)


def nanoseconds_to_milliseconds(value):
    return int(value) / NANOSECONDS_PER_MILLISECOND


def milliseconds_to_microseconds(value):
    return int(round(float(value) * MICROSECONDS_PER_MILLISECOND))


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


def latency_controls_from_settings(settings):
    controls = {}
    active_latency_present = "active_latency_ns" in settings
    configured_latency_present = "configured_latency_ns" in settings
    compatibility_latency_present = "latency_ns" in settings
    active_latency_nanoseconds = settings.get("active_latency_ns")
    configured_latency_nanoseconds = settings.get("configured_latency_ns")
    compatibility_latency_nanoseconds = settings.get("latency_ns")

    if active_latency_present:
        controls["active_latency"] = (
            nanoseconds_to_milliseconds(active_latency_nanoseconds) if active_latency_nanoseconds is not None else None
        )

    if configured_latency_present:
        controls["configured_latency"] = (
            nanoseconds_to_milliseconds(configured_latency_nanoseconds)
            if configured_latency_nanoseconds is not None
            else None
        )
    elif not active_latency_present and compatibility_latency_present:
        controls["configured_latency"] = (
            nanoseconds_to_milliseconds(compatibility_latency_nanoseconds)
            if compatibility_latency_nanoseconds is not None
            else None
        )

    effective_latency_nanoseconds = active_latency_nanoseconds
    if effective_latency_nanoseconds is None:
        effective_latency_nanoseconds = configured_latency_nanoseconds
    if effective_latency_nanoseconds is None:
        effective_latency_nanoseconds = compatibility_latency_nanoseconds
    if active_latency_present or configured_latency_present or compatibility_latency_present:
        controls["latency"] = (
            nanoseconds_to_milliseconds(effective_latency_nanoseconds)
            if effective_latency_nanoseconds is not None
            else None
        )

    for nanoseconds_field_name, milliseconds_field_name in (
        ("default_latency_ns", "default_latency"),
        ("min_latency_ns", "min_latency"),
        ("max_latency_ns", "max_latency"),
    ):
        if nanoseconds_field_name in settings:
            nanoseconds_value = settings[nanoseconds_field_name]
            controls[milliseconds_field_name] = (
                nanoseconds_to_milliseconds(nanoseconds_value) if nanoseconds_value is not None else None
            )

    return controls


def unavailable_latency_controls():
    return {
        "latency": None,
        "active_latency": None,
        "configured_latency": None,
        "default_latency": None,
        "min_latency": None,
        "max_latency": None,
    }
