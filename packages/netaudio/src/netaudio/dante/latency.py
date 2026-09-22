from netaudio import core

MICROSECONDS_PER_MILLISECOND = 1_000
NANOSECONDS_PER_MILLISECOND = 1_000_000


def nanoseconds_to_milliseconds(value):
    return int(value) / NANOSECONDS_PER_MILLISECOND


def milliseconds_to_microseconds(value):
    return int(round(float(value) * MICROSECONDS_PER_MILLISECOND))


def milliseconds_to_nanoseconds(value):
    return core.latency_control(value)["requested_latency_ns"]


def latency_choices(
    minimum_latency_milliseconds,
    maximum_latency_milliseconds,
    active_latency_milliseconds=None,
    configured_latency_milliseconds=None,
):
    settings = {
        f"{field}_latency_ns": None if value is None else milliseconds_to_nanoseconds(value)
        for field, value in (
            ("min", minimum_latency_milliseconds),
            ("max", maximum_latency_milliseconds),
            ("active", active_latency_milliseconds),
            ("configured", configured_latency_milliseconds),
        )
    }

    return core.latency_configuration(settings)["state"].get("latency_options_ms")


def latency_state_from_settings(settings):
    if not isinstance(settings, dict):
        return {}

    return core.latency_configuration(settings)["state"]


def latency_controls_from_settings(settings):
    return core.latency_configuration(settings)["controls"]


def unavailable_latency_controls():
    return {
        "active_latency": None,
        "configured_latency": None,
        "default_latency": None,
        "latency": None,
        "max_latency": None,
        "min_latency": None,
    }
