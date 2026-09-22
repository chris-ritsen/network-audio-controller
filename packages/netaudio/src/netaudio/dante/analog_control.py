"""Analog reference levels, resolved from a fresh codec response."""

from __future__ import annotations

import asyncio
from copy import deepcopy

from netaudio import core


def permission(device, *, write):
    return core.analog_access(
        {
            "managed": bool(device.requires_managed_control),
            "address_available": bool(device.ipv4),
            "online": getattr(device, "online", None),
            "supported": device.generic_codec_control_supported,
            "locked": device.is_locked,
            "write": write,
        }
    )


async def plan_analog(application, device, channel, level, direction=None, *, timeout=1.0):
    plan = {
        "category": "analog_level",
        "requested": {"channel": channel, "level": level, "direction": direction},
        "action": "unavailable",
        "reason": permission(device, write=False),
    }

    if plan["reason"]:
        return plan

    validation = core.analog_level_control(None, channel, level, direction)

    if validation["action"] == "unsupported":
        return {**plan, **validation}

    try:
        status = await application.probe_codec_status(device, timeout=timeout)
    except (RuntimeError, TimeoutError):
        device.codec_observed_at = None

        return {**plan, "reason": "Fresh analog status is unavailable."}

    application._apply_codec_status(device, status)
    plan.update(core.analog_level_control(status.get("gain_adapter"), channel, level, direction))
    plan["status"] = deepcopy(status.get("gain_adapter"))

    if plan["action"] != "change":
        return plan

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
            "status": plan.get("status"),
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
                readback = core.analog_level_control(status.get("gain_adapter"), channel, level, plan["direction"])
                result["status"] = deepcopy(status.get("gain_adapter"))

                if readback["action"] == "unchanged":
                    return {**result, "effective_state_confirmed": True}
            except (TimeoutError, RuntimeError):
                device.codec_observed_at = None

            await asyncio.sleep(0.02)

        return result
