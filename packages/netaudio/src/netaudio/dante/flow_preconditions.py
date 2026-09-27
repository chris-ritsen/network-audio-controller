from __future__ import annotations

from netaudio import core


async def refresh_flow_state(device, *, rtp: bool) -> str | None:
    """Refresh mutable authoring facts under the caller's topology mutation lock."""
    if getattr(device, "requires_managed_control", False):
        return "flow writes have no established managed transport"

    application = getattr(device, "application", None)
    populate = getattr(device, "populate_from_core", None)
    lock_probe = getattr(application, "probe_lock_status", None)
    if populate is None or lock_probe is None:
        return "fresh channel and lock-state readback is unavailable"

    try:
        if not await populate(include_channels=True, request_timeout_milliseconds=1000, request_attempts=1):
            return "fresh channel and capability readback is unavailable"

        lock = await lock_probe(device, timeout=2.0)
        device.is_locked = lock.is_locked if lock is not None else None
        if device.is_locked is not False:
            return "receiver or transmitter lock state is locked or unknown"

        if rtp:
            probe = getattr(application, "probe_aes67_state", None)
            if probe is None:
                return "fresh AES67 enablement readback is unavailable"

            status = await probe(device, timeout=2.0)
            device.aes67_current = status[0] if status is not None else None
            if device.aes67_current is not True:
                return "AES67 is disabled or current enablement is unknown"

        for attribute, probe_name in (
            ("sample_rate", "probe_sample_rate_status"),
            ("encoding", "probe_encoding_status"),
        ):
            probe = getattr(application, probe_name, None)
            if probe is None:
                return "fresh audio-format readback is unavailable"

            status = await probe(device, timeout=2.0)
            readback = core.flow_format_readback(status if isinstance(status, dict) else None, 0)
            value = readback["current_value"]
            if readback["state"] == "unavailable":
                setattr(device, attribute, None)
                return "fresh audio-format readback is unavailable"

            setattr(device, attribute, value)
    except (OSError, RuntimeError, TimeoutError, core.NetaudioCoreError) as error:
        return f"fresh flow preconditions unavailable: {error}"

    return None
