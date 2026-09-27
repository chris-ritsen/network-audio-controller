def browser_values(value):
    """Keep SDP identities lossless across JSON consumers with binary64 numbers."""
    if isinstance(value, dict):
        result = value
        for key, item in value.items():
            converted = (
                str(item)
                if key in {"session_id", "session_version"} and isinstance(item, int) and not isinstance(item, bool)
                else browser_values(item)
            )
            if converted is not item:
                if result is value:
                    result = dict(value)
                result[key] = converted
        return result

    if isinstance(value, (list, tuple)):
        result = value
        for index, item in enumerate(value):
            converted = browser_values(item)
            if converted is not item:
                if result is value:
                    result = list(value)
                result[index] = converted
        return result

    return value
