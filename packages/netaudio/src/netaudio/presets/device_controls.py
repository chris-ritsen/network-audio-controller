from copy import deepcopy
from netaudio import core
from netaudio.dante.panel_state import fresh


def capture_device_controls(device):
    state = getattr(device, "device_controls", {}) or {}
    fresh_values = {
        category: observation["value"]
        for category, observation in state.get("observations", {}).items()
        if fresh(device, observation)
    }

    return {
        "settings": core.capture_panel_configuration(fresh_values),
        "identity": deepcopy(state.get("identity", {})),
        "observed_extensions": deepcopy(state),
    }


def validate_device_controls(value):
    if not isinstance(value, dict) or not isinstance(value.get("settings", {}), dict):
        raise ValueError("device_controls must contain a settings object")
    if any("pair" in category.lower() for category in value.get("settings", {})):
        raise ValueError("Pairing-list clearing cannot be saved or applied in a preset.")
    return deepcopy(value)
