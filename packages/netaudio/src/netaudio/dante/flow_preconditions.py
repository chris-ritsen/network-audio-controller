from __future__ import annotations

import logging

from netaudio import core

logger = logging.getLogger("netaudio")


async def refresh_flow_state(device, *, rtp: bool) -> str | None:
    """Refresh mutable authoring facts under the caller's topology mutation lock."""
    if getattr(device, "requires_managed_control", False):
        return "flow writes have no established managed transport"

    application = getattr(device, "application", None)
    populate = getattr(device, "populate_from_core", None)
    if populate is None:
        return "fresh channel readback is unavailable"

    try:
        if not await populate(include_channels=True, request_timeout_milliseconds=1000, request_attempts=1):
            return "fresh channel and capability readback is unavailable"
    except (OSError, RuntimeError, TimeoutError, core.NetaudioCoreError) as error:
        return f"fresh channel readback is unavailable: {error}"

    lock_probe = getattr(application, "probe_lock_status", None)
    if lock_probe is not None:
        try:
            lock = await lock_probe(device, timeout=2.0)
        except (OSError, RuntimeError, TimeoutError, core.NetaudioCoreError) as error:
            logger.warning("flow change proceeds without a lock reading: %s", error)
        else:
            device.is_locked = lock.is_locked if lock is not None else None
    if getattr(device, "is_locked", None) is True:
        return "device is locked"

    if rtp:
        probe = getattr(application, "probe_aes67_state", None)
        if probe is not None:
            try:
                status = await probe(device, timeout=2.0)
            except (OSError, RuntimeError, TimeoutError, core.NetaudioCoreError) as error:
                logger.warning("flow change proceeds without an AES67 reading: %s", error)
            else:
                device.aes67_current = status[0] if status is not None else None
        if getattr(device, "aes67_current", None) is False:
            return "AES67 is turned off"

    for attribute, probe_name in (
        ("sample_rate", "probe_sample_rate_status"),
        ("encoding", "probe_encoding_status"),
    ):
        probe = getattr(application, probe_name, None)
        if probe is None:
            continue

        try:
            status = await probe(device, timeout=2.0)
        except (OSError, RuntimeError, TimeoutError, core.NetaudioCoreError) as error:
            logger.warning("flow change proceeds without a %s reading: %s", attribute, error)
            continue

        readback = core.flow_format_readback(status if isinstance(status, dict) else None, 0)
        if readback["state"] != "unavailable":
            setattr(device, attribute, readback["current_value"])

    return None
