"""Analog reference levels, resolved from a fresh codec response."""

from __future__ import annotations

import asyncio
from netaudio.dante.gain import gain_adapter_from_codec_status


def permission(device, *, write):
    if device.requires_managed_control:
        return "Managed analog-control transport is unavailable."
    if not device.ipv4 or getattr(device, "online", None) is False:
        return "Device is unavailable."
    if device.generic_codec_control_supported is not True:
        return "Codec-control support is unavailable."
    if write and device.is_locked is not False:
        return "Device is locked or its lock state is unknown."
    return None


async def plan_analog(application, device, channel, level, direction=None, *, timeout=1.0):
    plan = {
        "category": "analog_level",
        "requested": {"channel": channel, "level": level, "direction": direction},
        "action": "unavailable",
        "reason": permission(device, write=False),
    }
    if plan["reason"]:
        return plan
    if (
        isinstance(channel, bool)
        or not isinstance(channel, int)
        or channel not in (1, 2)
        or isinstance(level, bool)
        or not isinstance(level, int)
        or level not in (1, 2, 3, 4, 5)
    ):
        return {
            **plan,
            "action": "unsupported",
            "reason": "Select a reported channel (1 or 2) and a level from 1 through 5.",
        }
    try:
        status = await application.probe_codec_status(device, timeout=timeout)
    except (RuntimeError, TimeoutError):
        device.codec_observed_at = None
        return {**plan, "reason": "Fresh analog status is unavailable."}
    application._apply_codec_status(device, status)
    adapter = gain_adapter_from_codec_status(device, status)
    if adapter is None:
        return {
            **plan,
            "action": "ambiguous",
            "reason": "Codec status does not identify one unambiguous analog control.",
        }
    if direction is not None and direction != adapter["device_type"]:
        return {**plan, "action": "unsupported", "reason": "Requested direction differs from device status."}
    if channel > len(adapter["channel_levels"]):
        return {**plan, "action": "unsupported", "reason": "Channel is not reported by the device."}
    plan.update(before=adapter["channel_levels"][channel - 1], direction=adapter["device_type"])
    if plan["before"] == level:
        return {**plan, "action": "unchanged", "reason": None}
    reason = permission(device, write=True)
    return {**plan, "action": "unavailable" if reason else "change", "reason": reason}


async def apply_analog(application, device, channel, level, direction=None, timeout=2.0):
    async with application._capability_probe_lock("analog_write", application._control_key(device)):
        plan = await plan_analog(application, device, channel, level, direction, timeout=min(timeout, 1.0))
        result = {
            "plan": plan,
            "reason": plan["reason"],
            "request_sent": False,
            "request_acknowledged": None,
            "transport_reply": False,
            "correlated_status": False,
            "effective_state_confirmed": plan["action"] == "unchanged",
            "persistence": "unknown",
            "media_readiness": "unknown",
        }
        if plan["action"] != "change":
            return result
        reason = permission(device, write=True)
        if reason:
            return {**result, "reason": reason}
        await application.send_set_gain_level(device, channel, level, plan["direction"])
        result["request_sent"] = True
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            reason = permission(device, write=False)
            if reason:
                return {**result, "reason": reason}
            try:
                status = await application.probe_codec_status(
                    device, timeout=min(0.5, deadline - asyncio.get_running_loop().time())
                )
                application._apply_codec_status(device, status)
                adapter = gain_adapter_from_codec_status(device, status)
                if (
                    adapter
                    and adapter["device_type"] == plan["direction"]
                    and channel <= len(adapter["channel_levels"])
                    and adapter["channel_levels"][channel - 1] == level
                ):
                    return {**result, "effective_state_confirmed": True}
            except (TimeoutError, RuntimeError):
                device.codec_observed_at = None
            await asyncio.sleep(0.02)
        return result
