from __future__ import annotations

from datetime import datetime, timezone

CHANNEL_CAPABILITY_CAPACITIES = (
    "maximum_transmit_flow_channel_slots",
    "maximum_receive_flow_channel_slots",
    "maximum_transmit_flows",
    "maximum_receive_flows",
)
CHANNEL_CAPABILITY_FIELDS = (*CHANNEL_CAPABILITY_CAPACITIES, "channel_capability_observed_at")


def channel_capability_fields(counts) -> dict:
    return {
        **{name: counts.get(name) for name in CHANNEL_CAPABILITY_CAPACITIES},
        "channel_capability_observed_at": datetime.now(timezone.utc).isoformat(),
    }


def apply_channel_capability(device, counts) -> None:
    for name, value in channel_capability_fields(counts).items():
        setattr(device, name, value)
