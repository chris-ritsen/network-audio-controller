from __future__ import annotations

from netaudio.core import subscription_status


def default_status_entry(code: int, receiver_status_code: int | None = None) -> dict[str, object]:
    entry = subscription_status(code, receiver_status_code)
    entry["labels"] = (entry["label"],)
    return entry


MANAGED_STATUS_PRESENTATION = {
    "BUNDLE_FORMAT": (
        "Multicast flow format mismatch",
        "The multicast flow format isn't compatible with the receiver.",
    ),
    "CHANNEL_FORMAT": ("Channel format mismatch", "The source and destination channel formats differ."),
    "CHANNEL_LATENCY": ("Incorrect channel latencies", "The source requires more latency than the receiver supports."),
    "CLOCK_DOMAIN": ("Clock domain mismatch", "The transmitter and receiver aren't in the same clock domain."),
    "DYNAMIC": ("Subscribed (unicast)", None),
    "DYNAMIC_PROTOCOL": ("Dynamic protocol", None),
    "IDLE": ("Idle", None),
    "IN_PROGRESS": ("Establishing flow", "Setting up the flow with the transmitter."),
    "INVALID_CHANNEL": ("Invalid channel", "The subscription can't complete because the channel is invalid."),
    "INVALID_MSG": ("Request rejected by transmitter", "The transmitter couldn't interpret the receiver's request."),
    "MANUAL": ("Manually configured", None),
    "NONE": ("Not subscribed", None),
    "NO_CONNECTION": ("No connection", "Couldn't reach the transmitter."),
    "NO_RX": ("No more flows (Rx)", "The receiver can't take on any more flows."),
    "NO_TX": ("No more flows (Tx)", "The transmitter can't supply any more flows."),
    "QOS_FAIL_RX": ("Rx bandwidth exceeded", "The receiver can't reliably take on more inbound flows."),
    "QOS_FAIL_TX": ("Tx bandwidth exceeded", "The transmitter can't reliably take on more outbound flows."),
    "RESOLVED": ("Resolved", "The source was found; the flow is being set up."),
    "RESOLVED_NONE": ("Source not present", "The source channel isn't present on the network."),
    "RESOLVE_FAIL": ("Resolve failed", "An error occurred while looking up the source channel."),
    "RX_FAIL": ("Receiver setup failed", "An error occurred on the receiver."),
    "RX_LINK_DOWN": ("Receiver link down", "The subscription can't complete while the receiver's link is down."),
    "RX_NOT_READY": ("External receiver not ready", None),
    "STATIC": ("Subscribed (multicast)", None),
    "SUBSCRIBE_SELF": ("Subscribed (self)", "Subscribed to a channel on this same device."),
    "SUBSCRIBE_SELF_POLICY": (
        "Self-subscription not allowed",
        "This device doesn't permit local subscriptions between these channels.",
    ),
    "TX_CHANNEL_ENCRYPTED": (
        "Encryption unsupported (Rx)",
        "The receiver doesn't support the required signal encryption.",
    ),
    "TX_FAIL": ("Transmitter setup failed", "An error occurred on the transmitter."),
    "TX_FANOUT_LIMIT_REACHED": ("No more unicast flows (Tx)", "The transmitter can't supply any more unicast flows."),
    "TX_LINK_DOWN": ("Transmitter link down", "The subscription can't complete while the transmitter's link is down."),
    "TX_NOT_READY": ("External transmitter not ready", None),
    "TX_REJECTED_ADDR": ("Address rejected by transmitter", "The transmitter can't reach the receiver's address."),
    "TX_SCHEDULER_FAILURE": (
        "Tx scheduler failure",
        "Often caused by a receiver asking for under 1 ms unicast latency from a transmitter on a 100 Mbps link.",
    ),
    "UNRESOLVED": ("Unresolved", "The transmitting device isn't currently on the network."),
    "UNSUPPORTED": ("Unsupported feature", "The subscription needs a feature this device doesn't support."),
}

_MANAGED_MESSAGE_PREFIXES = ("error:", "warning:", "info:")


def humanize_managed_status(status) -> str:
    if not isinstance(status, str) or not status.strip():
        return "Status unavailable"
    return status.strip().replace("_", " ").capitalize()


def clean_managed_message(status_message) -> str | None:
    if not isinstance(status_message, str):
        return None
    message = status_message.strip()
    lowered = message.casefold()
    for prefix in _MANAGED_MESSAGE_PREFIXES:
        if lowered.startswith(prefix):
            message = message[len(prefix) :].strip()
            break
    return message or None


def managed_status_presentation(status, status_message, summary) -> tuple[str, str | None]:
    message = clean_managed_message(status_message)
    identifier = status.strip().upper() if isinstance(status, str) and status.strip() else None
    if identifier in MANAGED_STATUS_PRESENTATION:
        label, detail = MANAGED_STATUS_PRESENTATION[identifier]
        return label, detail or message
    normalized_summary = summary.strip().casefold() if isinstance(summary, str) and summary.strip() else None
    if identifier:
        return humanize_managed_status(identifier), message
    if normalized_summary == "connected":
        return "Connected", message
    if normalized_summary:
        return humanize_managed_status(summary), message
    return "Status unavailable", message
