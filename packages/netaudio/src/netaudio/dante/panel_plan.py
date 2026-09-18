"""Plans are built from fresh observations and contain only the selected operation."""

from __future__ import annotations

from netaudio import core
from netaudio.dante.panel_state import fresh, panel_family, panel_permission

CONFIGURABLE = frozenset(
    {
        "bluetooth_identification",
        "bluetooth_discovery",
        "bluetooth_pairing",
        "video_format",
        "codec_format",
        "serial",
        "bandwidth",
        "hdcp",
    }
)


def _value(device, category):
    observation = (getattr(device, "device_controls", {}) or {}).get("observations", {}).get(category)
    if not fresh(device, observation):
        raise LookupError(f"Fresh {category.replace('_', ' ')} status is unavailable.")
    return observation["value"]


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _single(value):
    return _integer(value) and value > 0 and value & (value - 1) == 0


def plan_panel(device, category, requested, *, confirm_clear=False):
    plan = {
        "category": category,
        "requested": requested,
        "action": "unavailable",
        "reason": None,
        "requests": [],
        "before": None,
    }
    if category not in CONFIGURABLE:
        plan.update(action="unsupported", reason="This setting has no supported writer.")
        return plan
    family = panel_family(device)
    if (category.startswith("bluetooth_") and family != "bluetooth") or (
        not category.startswith("bluetooth_") and family != "dante_av"
    ):
        plan.update(action="unsupported", reason="This setting does not belong to the device's panel.")
        return plan
    try:
        current = _value(device, category)
        plan["before"] = current
        request, expected, extra = _request(device, category, requested, current, confirm_clear)
        plan["expected"] = expected
        if category != "bluetooth_pairing" and matches(current, expected):
            plan["action"] = "unchanged"
            return plan
        reason = panel_permission(device, write=True)
        if reason:
            plan["reason"] = reason
            return plan
        requests = [request, *extra]
        for message in requests:
            core.build_command(
                {
                    "command": "panel_control",
                    "request": message,
                    "requester": 0,
                    "sequence": 1,
                    "message_id": 1,
                    "host_mac": "000000000000",
                }
            )
        plan.update(action="change", requests=requests)
    except LookupError as exc:
        plan["reason"] = str(exc)
    except (ValueError, TypeError, KeyError, core.NetaudioCoreError) as exc:
        plan.update(action="unsupported", reason=str(exc))
    return plan


def _request(device, category, value, current, confirm_clear):
    extra = []
    if category == "bluetooth_identification":
        if current.get("name_source") not in (1, 2):
            raise ValueError("The current Bluetooth naming mode is unsupported.")
        if not isinstance(value, dict) or set(value) != {"name_source", "custom_name"}:
            raise ValueError("Specify name_source and custom_name.")
        if value["name_source"] not in (1, 2) or isinstance(value["name_source"], bool):
            raise ValueError("Name source must be 1 (device name) or 2 (custom name).")
        name = value["custom_name"]
        if not isinstance(name, str) or len(name) > 32:
            raise ValueError("Bluetooth names may contain at most 32 characters.")
        name.encode("utf-8")
        return {"operation": category, **value}, value, extra
    if category == "bluetooth_discovery":
        if current not in (1, 2):
            raise ValueError("The device has not reported a supported discoverability state.")
        if not isinstance(value, bool):
            raise ValueError("Discoverability must be true or false.")
        return {"operation": category, "discoverable": value}, 1 if value else 2, extra
    if category == "bluetooth_pairing":
        if value != "clear" or confirm_clear is not True:
            raise ValueError("Clearing remembered devices requires explicit confirmation.")
        return {"operation": "bluetooth_clear_pairing", "confirmed": True}, 0, extra
    if not isinstance(value, dict):
        raise ValueError("Settings must be an object.")
    if category == "video_format":
        if set(value) != {"format", "selection"}:
            raise ValueError("Specify format and selection.")
        fmt, mode = value["format"], value["selection"]
        if not isinstance(fmt, dict) or not isinstance(mode, dict):
            raise ValueError("Format and selection must be objects.")
        if current["direction"] != 0 or getattr(device, "video_transmission_supported", None) is not True:
            raise ValueError("Video format changes are supported only on the advertised reference transmitter.")
        if set(mode) != {"manual_resolution", "manual_bit_depth", "manual_color_space"} or any(
            not isinstance(v, bool) for v in mode.values()
        ):
            raise ValueError("Selection flags must be booleans.")
        if set(fmt) != {"resolution", "bit_depth", "color_space"} or any(
            not _integer(v) or v < 0 for v in fmt.values()
        ):
            raise ValueError("Format values must be unsigned integers.")
        candidates = [
            f for f in current["supported"] if not mode["manual_resolution"] or f["resolution"] == fmt["resolution"]
        ]
        if not candidates:
            raise ValueError("Resolution is not advertised by the device.")
        for name in ("bit_depth", "color_space"):
            if mode[f"manual_{name}"]:
                if not _single(fmt[name]):
                    raise ValueError("Manual depth and color must each select one supported bit.")
                candidates = [f for f in candidates if f[name] & fmt[name]]
                if not candidates:
                    raise ValueError("Depth/color combination is not advertised for this resolution.")
        changed_depth_color = any(
            fmt[name] != (current.get("configured") or {}).get(name)
            or mode[f"manual_{name}"] != (current.get("selection") or {}).get(f"manual_{name}")
            for name in ("bit_depth", "color_space")
        )
        if changed_depth_color or mode["manual_resolution"]:
            if _value(device, "visca")["capability"] != 1:
                raise ValueError("Fresh scoped video-format capability is required.")
        if mode["manual_resolution"]:
            extra.append({"operation": "video_visca_format", **value})
        return {"operation": category, **value}, {"configured": fmt, "selection": mode}, extra
    if category == "codec_format":
        if not any(
            f["codec_type"] == value.get("codec_type")
            and f["profile"] == value.get("profile")
            and _single(value.get("level"))
            and f["level"] & value["level"]
            for f in current["supported"]
        ):
            raise ValueError("Codec combination is not advertised by the device.")
        return {"operation": category, "format": value}, {"current": value}, extra
    if category == "serial":
        if (
            current["baud_rate"] not in (1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200, 230400)
            or current["data_bits"] not in (7, 8)
            or current["parity"] not in (0, 1, 2)
            or current["stop_bits"] not in (1, 2)
        ):
            raise ValueError("The device has not reported a supported serial format.")
        if current["hardware_flow_control"] or current["software_flow_control"]:
            raise ValueError("Existing flow-control settings are unsupported; refusing to overwrite them.")
        return {"operation": category, "settings": value}, value, extra
    if category == "bandwidth":
        if (
            set(value) != {"target", "enabled"}
            or not isinstance(value["enabled"], bool)
            or not _integer(value["target"])
        ):
            raise ValueError("Specify integer target in Mbit/s and boolean enabled.")
        if (
            _value(device, "video_format")["direction"] != 0
            or getattr(device, "video_transmission_supported", None) is not True
        ):
            raise ValueError("Bandwidth is writable only on a transmitter.")
        if current["maximum"] == 0 or current["minimum"] == 0:
            raise ValueError("Bandwidth control is unavailable.")
        if value["enabled"] and not current["minimum"] <= value["target"] <= min(current["maximum"], 700):
            raise ValueError("Target is outside the device's supported Mbit/s range.")
        return {"operation": category, **value}, {"target": value["target"], "enabled": int(value["enabled"])}, extra
    if category == "hdcp":
        if set(value) != {"mode"} or not _integer(value["mode"]) or value["mode"] not in current["supported_modes"]:
            raise ValueError("HDCP mode is not advertised by the device.")
        return {"operation": category, **value}, {"configured_mode": value["mode"]}, extra
    raise ValueError("Unsupported setting.")


def matches(value, expected):
    return (
        all(value.get(k) == v for k, v in expected.items())
        if isinstance(expected, dict) and isinstance(value, dict)
        else value == expected
    )
