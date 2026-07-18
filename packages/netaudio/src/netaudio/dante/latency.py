NANOSECONDS_PER_MILLISECOND = 1_000_000
MICROSECONDS_PER_MILLISECOND = 1_000


def nanoseconds_to_milliseconds(value):
    return int(value) / NANOSECONDS_PER_MILLISECOND


def milliseconds_to_microseconds(value):
    return int(round(float(value) * MICROSECONDS_PER_MILLISECOND))
