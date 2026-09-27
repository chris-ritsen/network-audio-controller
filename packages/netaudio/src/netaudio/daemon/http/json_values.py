def browser_values(value):
    """Keep SDP identities lossless across JSON consumers with binary64 numbers."""
    if isinstance(value, dict):
        return {
            key: str(item)
            if key in {"session_id", "session_version"} and isinstance(item, int) and not isinstance(item, bool)
            else browser_values(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [browser_values(item) for item in value]

    return value
