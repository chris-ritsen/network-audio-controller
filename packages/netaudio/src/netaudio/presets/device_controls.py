from copy import deepcopy
from netaudio.dante.panel_state import fresh


def capture_device_controls(device):
    state = getattr(device, "device_controls", {}) or {}
    settings = {}
    for category, observation in state.get("observations", {}).items():
        if not fresh(device, observation):
            continue
        value = deepcopy(observation["value"])
        if category in {"bluetooth_identification", "serial"}:
            settings[category] = value
        elif category == "bluetooth_discovery" and value in (1, 2):
            settings[category] = value == 1
        elif category == "video_format" and value.get("configured") is not None and value.get("selection") is not None:
            settings[category] = {"format": value["configured"], "selection": value["selection"]}
        elif category == "codec_format" and value.get("current") is not None:
            settings[category] = value["current"]
        elif category == "bandwidth" and value.get("enabled") in (0, 1):
            settings[category] = {
                "target": value["target"] if value["enabled"] else 0,
                "enabled": value["enabled"] == 1,
            }
        elif category == "hdcp":
            settings[category] = {"mode": value["configured_mode"]}
    return {
        "settings": settings,
        "identity": deepcopy(state.get("identity", {})),
        "observed_extensions": deepcopy(state),
    }


def validate_device_controls(value):
    if not isinstance(value, dict) or not isinstance(value.get("settings", {}), dict):
        raise ValueError("device_controls must contain a settings object")
    if any("pair" in category.lower() for category in value.get("settings", {})):
        raise ValueError("Pairing-list clearing cannot be saved or applied in a preset.")
    return deepcopy(value)
