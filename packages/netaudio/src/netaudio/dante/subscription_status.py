from __future__ import annotations

from enum import Enum, IntEnum

UNKNOWN_RANGE_START = 0x0028
UNKNOWN_RANGE_END = 0x0040


class SubscriptionSeverity(str, Enum):
    NONE = "none"
    OK = "ok"
    INFO = "info"
    PROGRESS = "progress"
    WARNING = "warning"
    ERROR = "error"


class SubscriptionState(str, Enum):
    NONE = "none"
    UNRESOLVED = "unresolved"
    RESOLVED = "resolved"
    IDLE = "idle"
    IN_PROGRESS = "in_progress"
    PENDING = "pending"
    CONNECTED = "connected"
    ERROR = "error"
    UNKNOWN = "unknown"


class SubscriptionStatus(IntEnum):
    NOT_SUBSCRIBED = 0x00
    UNRESOLVED = 0x01
    RESOLVED = 0x02
    RESOLVE_FAILED = 0x03
    SUBSCRIBED_SELF = 0x04
    SOURCE_NOT_PRESENT = 0x05
    SUBSCRIPTION_FAILED = 0x06
    UNLABELED = 0x07
    ESTABLISHING_FLOW = 0x08
    SUBSCRIBED_UNICAST = 0x09
    SUBSCRIBED_MULTICAST = 0x0A
    MANUALLY_CONFIGURED = 0x0E
    NO_CONNECTION = 0x0F
    CHANNEL_FORMAT_MISMATCH = 0x10
    MULTICAST_FORMAT_MISMATCH = 0x11
    NO_MORE_FLOWS_RX = 0x12
    RX_SETUP_FAILED = 0x13
    NO_MORE_FLOWS_TX = 0x14
    TX_SETUP_FAILED = 0x15
    RX_BANDWIDTH_EXCEEDED = 0x16
    TX_BANDWIDTH_EXCEEDED = 0x17
    TX_REJECTED_ADDRESS = 0x18
    TX_REJECTED_MESSAGE = 0x19
    CHANNEL_LATENCY_TOO_LOW = 0x1A
    CLOCK_DOMAIN_MISMATCH = 0x1B
    UNSUPPORTED_FEATURE = 0x1C
    RX_LINK_DOWN = 0x1D
    TX_LINK_DOWN = 0x1E
    DYNAMIC_PROTOCOL = 0x1F
    INVALID_CHANNEL = 0x20
    TX_SCHEDULER_FAILURE = 0x21
    SELF_SUBSCRIPTION_NOT_ALLOWED = 0x22
    EXTERNAL_TX_NOT_READY = 0x23
    EXTERNAL_RX_NOT_READY = 0x24
    NO_MORE_UNICAST_FLOWS_TX = 0x25
    RX_ENCRYPTION_UNSUPPORTED = 0x26
    UNEXPECTED_TX_RESPONSE = 0x27


CONNECTED_STATUSES = frozenset(
    {
        SubscriptionStatus.SUBSCRIBED_SELF,
        SubscriptionStatus.SUBSCRIBED_UNICAST,
        SubscriptionStatus.SUBSCRIBED_MULTICAST,
        SubscriptionStatus.MANUALLY_CONFIGURED,
    }
)


def _entry(
    status: SubscriptionStatus,
    state: SubscriptionState,
    severity: SubscriptionSeverity,
    label: str,
    detail: str | None,
) -> dict[str, object]:
    return {
        "status": status.name,
        "state": state.value,
        "severity": severity.value,
        "label": label,
        "detail": detail,
    }


_SUBSCRIPTION_FAILED = (
    SubscriptionState.ERROR,
    SubscriptionSeverity.ERROR,
    "Subscription failed",
    "The subscription failed for an unspecified reason.",
)

_DEFINITIONS: dict[int, dict[str, object]] = {
    SubscriptionStatus.NOT_SUBSCRIBED: _entry(
        SubscriptionStatus.NOT_SUBSCRIBED,
        SubscriptionState.NONE,
        SubscriptionSeverity.NONE,
        "Not subscribed",
        None,
    ),
    SubscriptionStatus.UNRESOLVED: _entry(
        SubscriptionStatus.UNRESOLVED,
        SubscriptionState.UNRESOLVED,
        SubscriptionSeverity.WARNING,
        "Unresolved",
        "The transmitting device isn't currently on the network.",
    ),
    SubscriptionStatus.RESOLVED: _entry(
        SubscriptionStatus.RESOLVED,
        SubscriptionState.RESOLVED,
        SubscriptionSeverity.PROGRESS,
        "Resolved",
        "The source was found; the flow is being set up.",
    ),
    SubscriptionStatus.RESOLVE_FAILED: _entry(
        SubscriptionStatus.RESOLVE_FAILED,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Resolve failed",
        "An error occurred while looking up the source channel.",
    ),
    SubscriptionStatus.SUBSCRIBED_SELF: _entry(
        SubscriptionStatus.SUBSCRIBED_SELF,
        SubscriptionState.CONNECTED,
        SubscriptionSeverity.OK,
        "Subscribed (self)",
        "Subscribed to a channel on this same device.",
    ),
    SubscriptionStatus.SOURCE_NOT_PRESENT: _entry(
        SubscriptionStatus.SOURCE_NOT_PRESENT,
        SubscriptionState.UNRESOLVED,
        SubscriptionSeverity.ERROR,
        "Source not present",
        "The source channel isn't present on the network.",
    ),
    SubscriptionStatus.SUBSCRIPTION_FAILED: _entry(SubscriptionStatus.SUBSCRIPTION_FAILED, *_SUBSCRIPTION_FAILED),
    SubscriptionStatus.UNLABELED: _entry(
        SubscriptionStatus.UNLABELED,
        SubscriptionState.NONE,
        SubscriptionSeverity.NONE,
        "",
        None,
    ),
    SubscriptionStatus.ESTABLISHING_FLOW: _entry(
        SubscriptionStatus.ESTABLISHING_FLOW,
        SubscriptionState.IN_PROGRESS,
        SubscriptionSeverity.PROGRESS,
        "Establishing flow",
        "Setting up the flow with the transmitter.",
    ),
    SubscriptionStatus.SUBSCRIBED_UNICAST: _entry(
        SubscriptionStatus.SUBSCRIBED_UNICAST,
        SubscriptionState.CONNECTED,
        SubscriptionSeverity.OK,
        "Subscribed (unicast)",
        None,
    ),
    SubscriptionStatus.SUBSCRIBED_MULTICAST: _entry(
        SubscriptionStatus.SUBSCRIBED_MULTICAST,
        SubscriptionState.CONNECTED,
        SubscriptionSeverity.OK,
        "Subscribed (multicast)",
        None,
    ),
    SubscriptionStatus.MANUALLY_CONFIGURED: _entry(
        SubscriptionStatus.MANUALLY_CONFIGURED,
        SubscriptionState.CONNECTED,
        SubscriptionSeverity.OK,
        "Manually configured",
        None,
    ),
    SubscriptionStatus.NO_CONNECTION: _entry(
        SubscriptionStatus.NO_CONNECTION,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "No connection",
        "Couldn't reach the transmitter.",
    ),
    SubscriptionStatus.CHANNEL_FORMAT_MISMATCH: _entry(
        SubscriptionStatus.CHANNEL_FORMAT_MISMATCH,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Channel format mismatch",
        "The source and destination channel formats differ.",
    ),
    SubscriptionStatus.MULTICAST_FORMAT_MISMATCH: _entry(
        SubscriptionStatus.MULTICAST_FORMAT_MISMATCH,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Multicast flow format mismatch",
        "The multicast flow format isn't compatible with the receiver.",
    ),
    SubscriptionStatus.NO_MORE_FLOWS_RX: _entry(
        SubscriptionStatus.NO_MORE_FLOWS_RX,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "No more flows (Rx)",
        "The receiver can't take on any more flows.",
    ),
    SubscriptionStatus.RX_SETUP_FAILED: _entry(
        SubscriptionStatus.RX_SETUP_FAILED,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Receiver setup failed",
        "An error occurred on the receiver.",
    ),
    SubscriptionStatus.NO_MORE_FLOWS_TX: _entry(
        SubscriptionStatus.NO_MORE_FLOWS_TX,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "No more flows (Tx)",
        "The transmitter can't supply any more flows.",
    ),
    SubscriptionStatus.TX_SETUP_FAILED: _entry(
        SubscriptionStatus.TX_SETUP_FAILED,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Transmitter setup failed",
        "An error occurred on the transmitter.",
    ),
    SubscriptionStatus.RX_BANDWIDTH_EXCEEDED: _entry(
        SubscriptionStatus.RX_BANDWIDTH_EXCEEDED,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Rx bandwidth exceeded",
        "The receiver can't reliably take on more inbound flows.",
    ),
    SubscriptionStatus.TX_BANDWIDTH_EXCEEDED: _entry(
        SubscriptionStatus.TX_BANDWIDTH_EXCEEDED,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Tx bandwidth exceeded",
        "The transmitter can't reliably take on more outbound flows.",
    ),
    SubscriptionStatus.TX_REJECTED_ADDRESS: _entry(
        SubscriptionStatus.TX_REJECTED_ADDRESS,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Address rejected by transmitter",
        "The transmitter can't reach the receiver's address.",
    ),
    SubscriptionStatus.TX_REJECTED_MESSAGE: _entry(
        SubscriptionStatus.TX_REJECTED_MESSAGE,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Request rejected by transmitter",
        "The transmitter couldn't interpret the receiver's request.",
    ),
    SubscriptionStatus.CHANNEL_LATENCY_TOO_LOW: _entry(
        SubscriptionStatus.CHANNEL_LATENCY_TOO_LOW,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Channel latency too low",
        "The source needs more latency than the receiver allows.",
    ),
    SubscriptionStatus.CLOCK_DOMAIN_MISMATCH: _entry(
        SubscriptionStatus.CLOCK_DOMAIN_MISMATCH,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Clock domain mismatch",
        "The transmitter and receiver aren't in the same clock domain.",
    ),
    SubscriptionStatus.UNSUPPORTED_FEATURE: _entry(
        SubscriptionStatus.UNSUPPORTED_FEATURE,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Unsupported feature",
        "The subscription needs a feature this device doesn't support.",
    ),
    SubscriptionStatus.RX_LINK_DOWN: _entry(
        SubscriptionStatus.RX_LINK_DOWN,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Receiver link down",
        "The subscription can't complete while the receiver's link is down.",
    ),
    SubscriptionStatus.TX_LINK_DOWN: _entry(
        SubscriptionStatus.TX_LINK_DOWN,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Transmitter link down",
        "The subscription can't complete while the transmitter's link is down.",
    ),
    SubscriptionStatus.DYNAMIC_PROTOCOL: _entry(
        SubscriptionStatus.DYNAMIC_PROTOCOL,
        SubscriptionState.UNKNOWN,
        SubscriptionSeverity.WARNING,
        "Dynamic protocol",
        None,
    ),
    SubscriptionStatus.INVALID_CHANNEL: _entry(
        SubscriptionStatus.INVALID_CHANNEL,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Invalid channel",
        "The subscription can't complete because the channel is invalid.",
    ),
    SubscriptionStatus.TX_SCHEDULER_FAILURE: _entry(
        SubscriptionStatus.TX_SCHEDULER_FAILURE,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Tx scheduler failure",
        "Often caused by a receiver asking for under 1 ms unicast latency from a transmitter on a 100 Mbps link.",
    ),
    SubscriptionStatus.SELF_SUBSCRIPTION_NOT_ALLOWED: _entry(
        SubscriptionStatus.SELF_SUBSCRIPTION_NOT_ALLOWED,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Self-subscription not allowed",
        "This device doesn't permit local subscriptions between these channels.",
    ),
    SubscriptionStatus.EXTERNAL_TX_NOT_READY: _entry(
        SubscriptionStatus.EXTERNAL_TX_NOT_READY,
        SubscriptionState.PENDING,
        SubscriptionSeverity.WARNING,
        "External transmitter not ready",
        None,
    ),
    SubscriptionStatus.EXTERNAL_RX_NOT_READY: _entry(
        SubscriptionStatus.EXTERNAL_RX_NOT_READY,
        SubscriptionState.PENDING,
        SubscriptionSeverity.WARNING,
        "External receiver not ready",
        None,
    ),
    SubscriptionStatus.NO_MORE_UNICAST_FLOWS_TX: _entry(
        SubscriptionStatus.NO_MORE_UNICAST_FLOWS_TX,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "No more unicast flows (Tx)",
        "The transmitter can't supply any more unicast flows.",
    ),
    SubscriptionStatus.RX_ENCRYPTION_UNSUPPORTED: _entry(
        SubscriptionStatus.RX_ENCRYPTION_UNSUPPORTED,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Encryption unsupported (Rx)",
        "The receiver doesn't support the required signal encryption.",
    ),
    SubscriptionStatus.UNEXPECTED_TX_RESPONSE: _entry(
        SubscriptionStatus.UNEXPECTED_TX_RESPONSE,
        SubscriptionState.ERROR,
        SubscriptionSeverity.ERROR,
        "Unexpected transmitter response",
        None,
    ),
}

_UNKNOWN_ENTRY = {
    "status": None,
    "state": SubscriptionState.UNKNOWN.value,
    "severity": SubscriptionSeverity.ERROR.value,
    "label": _SUBSCRIPTION_FAILED[2],
    "detail": _SUBSCRIPTION_FAILED[3],
}

for _reserved in (0x0B, 0x0C, 0x0D):
    _DEFINITIONS[_reserved] = dict(_UNKNOWN_ENTRY)

for _code in range(UNKNOWN_RANGE_START, UNKNOWN_RANGE_END):
    _DEFINITIONS.setdefault(_code, dict(_UNKNOWN_ENTRY))

SUBSCRIPTION_STATUS_DEFINITIONS: dict[int, dict[str, object]] = _DEFINITIONS


def default_status_entry(code: int) -> dict[str, object]:
    if code in SUBSCRIPTION_STATUS_DEFINITIONS:
        return dict(SUBSCRIPTION_STATUS_DEFINITIONS[code])

    low_byte = code & 0x00FF
    if code & 0xFF00 and low_byte in SUBSCRIPTION_STATUS_DEFINITIONS:
        return dict(SUBSCRIPTION_STATUS_DEFINITIONS[low_byte])

    return dict(_UNKNOWN_ENTRY)
