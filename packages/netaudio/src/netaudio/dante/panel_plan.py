from netaudio import core
from netaudio.dante.panel_state import fresh, panel_facts


def plan_panel(device, category, requested, *, confirm_clear=False):
    observations = (getattr(device, "device_controls", None) or {}).get("observations", {})
    return core.plan_panel(
        {
            "profile": panel_facts(device),
            "category": category,
            "requested": requested,
            "fresh_values": {
                name: observation["value"] for name, observation in observations.items() if fresh(device, observation)
            },
            "confirm_clear": confirm_clear,
        }
    )


def matches(value, expected):
    return core.panel_readback_matches({"observed": value, "expected": expected})
