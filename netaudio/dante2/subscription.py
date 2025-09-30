from __future__ import annotations
import enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .channel import DanteRxChannel, DanteTxChannel


class DanteSubscription:

    def __init__(
        self,
        rx_channel: DanteRxChannel,
        tx_channel: DanteTxChannel | None,
        status: DanteSubscriptionStatus,
    ):
        self._rx_channel: DanteRxChannel = rx_channel
        self._tx_channel: DanteTxChannel | None = tx_channel
        self._status: DanteSubscriptionStatus = status

    def __str__(self) -> str:
        if self._tx_channel:
            tx_text = f" <- {self._tx_channel}"
        else:
            tx_text = ""
        status_text = ", ".join(self.status_text)
        return f"{self._rx_channel}{tx_text} [{status_text}]"

    def json(self):
        return {
            "rx_channel": str(self._rx_channel),
            "tx_channel": str(self.tx_channel),
            "status_code": self.status.value,
            "status_text": self.status_text,
        }

    @property
    def rx_channel(self) -> DanteRxChannel:
        return self._rx_channel

    @property
    def status(self) -> DanteSubscriptionStatus:
        return self._status

    @property
    def status_text(self) -> str:
        if self._status in DANTE_SUBSCRIPTION_STATUS_LABELS:
            return DANTE_SUBSCRIPTION_STATUS_LABELS[self._status]
        return ''

    @property
    def tx_channel(self) -> DanteTxChannel:
        return self._tx_channel


class DanteSubscriptionStatus(enum.Enum):
    BUNDLE_FORMAT = 17
    CHANNEL_FORMAT = 16
    CHANNEL_LATENCY = 26
    CLOCK_DOMAIN = 27
    DYNAMIC = 9
    DYNAMIC_PROTOCOL = 31
    FLAG_NO_ADVERT = 256
    FLAG_NO_DBCP = 512
    HDCP_NEGOTIATION_ERROR = 112
    IDLE = 7
    INVALID_CHANNEL = 32
    INVALID_MSG = 25
    IN_PROGRESS = 8
    MANUAL = 14
    NONE = 0
    NO_CONNECTION = 15
    NO_DATA = 65536
    NO_RX = 18
    NO_TX = 20
    QOS_FAIL_RX = 22
    QOS_FAIL_TX = 23
    RESOLVED = 2
    RESOLVED_NONE = 5
    RESOLVE_FAIL = 3
    RX_FAIL = 19
    RX_LINK_DOWN = 29
    RX_NOT_READY = 36
    RX_UNSUPPORTED_SUB_MODE = 69
    STATIC = 10
    SUBSCRIBE_SELF = 4
    SUBSCRIBE_SELF_POLICY = 34
    SYSTEM_FAIL = 255
    TEMPLATE_FULL = 68
    TEMPLATE_MISMATCH_CONFIG = 67
    TEMPLATE_MISMATCH_DEVICE = 64
    TEMPLATE_MISMATCH_FORMAT = 65
    TEMPLATE_MISSING_CHANNEL = 66
    TX_ACCESS_CONTROL_DENIED = 96
    TX_ACCESS_CONTROL_PENDING = 97
    TX_CHANNEL_ENCRYPTED = 38
    TX_FAIL = 21
    TX_FANOUT_LIMIT_REACHED = 37
    TX_LINK_DOWN = 30
    TX_NOT_READY = 35
    TX_REJECTED_ADDR = 24
    TX_RESPONSE_UNEXPECTED = 39
    TX_SCHEDULER_FAILURE = 33
    TX_UNSUPPORTED_SUB_MODE = 70
    UNRESOLVED = 1
    UNSUPPORTED = 28

    @classmethod
    def derive(cls, value):
        if value not in cls:
            return None
        return next(
            filter(
                lambda member: member.value == value,
                cls.__members__.values()
            )
        )


DANTE_SUBSCRIPTION_STATUS_LABELS = {
    DanteSubscriptionStatus.BUNDLE_FORMAT: (
        "Incorrect flow format",
        "Incorrect multicast flow format",
        "flow format incompatible with receiver",
    ),
    DanteSubscriptionStatus.CHANNEL_FORMAT: (
        "Incorrect channel format",
        "source and destination channels do not match",
    ),
    DanteSubscriptionStatus.CHANNEL_LATENCY: (
        "No suitable channel latency",
        "Incorrect channel latencies",
        "source demands more latency than the receiver has available",
    ),
    DanteSubscriptionStatus.CLOCK_DOMAIN: (
        "Mismatched clock domains",
        "The transmitter and receiver are not part of the same clock domain",
    ),
    DanteSubscriptionStatus.DYNAMIC: (
        "Connected (unicast)",
    ),
    DanteSubscriptionStatus.DYNAMIC_PROTOCOL: (
        "Dynamic Protocol",
    ),
    DanteSubscriptionStatus.FLAG_NO_ADVERT: (
        "No audio data.",
    ),
    DanteSubscriptionStatus.IDLE: (
        "Subscription idle",
        "Flow creation idle",
        "Insufficient information to create flow",
    ),
    DanteSubscriptionStatus.IN_PROGRESS: (
        "Subscription in progress",
        "Flow creation in progress",
        "communicating with transmitter to create flow",
    ),
    DanteSubscriptionStatus.INVALID_CHANNEL: (
        "Invalid Channel",
        "the subscription cannot be completed as channel is invalid",
    ),
    DanteSubscriptionStatus.INVALID_MSG: (
        "Subscription message rejected by transmitter",
        "Transmitter rejected message",
        "transmitter can't understand receiver's request",
    ),
    DanteSubscriptionStatus.MANUAL: (
        "Manually Configured",
    ),
    DanteSubscriptionStatus.NO_CONNECTION: (
        "No connection",
        "could not communicate with transmitter",
    ),
    DanteSubscriptionStatus.NO_RX: (
        "No Receive flows",
        "No more flows (RX)",
        "receiver cannot support any more flows",
        "Is receiver subscribed to too many different devices?",
    ),
    DanteSubscriptionStatus.NO_TX: (
        "No Transmit flows",
        "No more flows (TX)",
        "transmitter cannot support any more flows",
        "Reduce fan out by unsubscribing receivers or switching to multicast.",
    ),
    DanteSubscriptionStatus.NONE: (
        "none",
        "No subscription for this channel",
    ),
    DanteSubscriptionStatus.QOS_FAIL_RX: (
        "Receive bandwidth exceeded",
        "receiver can't reliably support any more inbound flows",
        "Reduce number of subscriptions or look for excessive multicast.",
    ),
    DanteSubscriptionStatus.QOS_FAIL_TX: (
        "Transmit bandwidth exceeded",
        "transmitter can't reliably support any more outbound flows",
        "Reduce fan out by unsubscribing receivers or switching to multicast.",
    ),
    DanteSubscriptionStatus.RESOLVE_FAIL: (
        "Can't resolve subscription",
        "Resolve failed",
        "received an unexpected error when trying to resolve this channel",
    ),
    DanteSubscriptionStatus.RESOLVED: (
        "Subscription resolved",
        "Resolved",
        "channel found; preparing to create flow",
    ),
    DanteSubscriptionStatus.RX_FAIL: (
        "Receive failure",
        "Receiver setup failed",
        "unexpected error on receiver",
    ),
    DanteSubscriptionStatus.RX_LINK_DOWN: (
        "RX link down",
        "RX link down",
        "The subscription cannot be completed as RX link is down",
    ),
    DanteSubscriptionStatus.STATIC: (
        "Connected (multicast)",
    ),
    DanteSubscriptionStatus.SUBSCRIBE_SELF: (
        "Subscribed to own signal",
        "Connected (self)",
    ),
    DanteSubscriptionStatus.SUBSCRIBE_SELF_POLICY: (
        "Subscription to own signal disallowed by device",
        "Policy failure for subscription to self",
        "The device does not support local subscriptions for the given transmit and receive channels.",
    ),
    DanteSubscriptionStatus.SYSTEM_FAIL: (
        "System failure",
        "Incorrect multicast flow format",
        "flow format incompatible with receiver",
    ),
    DanteSubscriptionStatus.TEMPLATE_MISMATCH_CONFIG: (
        "The receive channel's resolved information conflicts with the multicast templates resolved information",
        "Template mismatch (config)",
    ),
    DanteSubscriptionStatus.TEMPLATE_MISMATCH_DEVICE: (
        "The receive channel's subscription does not match the templates TX device",
        "Template mismatch (device)",
    ),
    DanteSubscriptionStatus.TEMPLATE_MISMATCH_FORMAT: (
        "The receive channel's available audio formats do not match the template's audio format",
        "Template mismatch (format)",
    ),
    DanteSubscriptionStatus.TEMPLATE_MISSING_CHANNEL: (
        "The receive channel's subscription is not a part of the given multicast template",
        "Template missing channel",
    ),
    DanteSubscriptionStatus.TEMPLATE_FULL: (
        "The receive channel's template is already full",
        "Template full",
    ),
    DanteSubscriptionStatus.TX_FAIL: (
        "Transmit failure",
        "Transmitter setup failed",
        "unexpected error on transmitter",
    ),
    DanteSubscriptionStatus.TX_LINK_DOWN: (
        "TX link down",
        "The subscription cannot be completed as TX link is down",
    ),
    DanteSubscriptionStatus.TX_REJECTED_ADDR: (
        "Subscription address rejected by transmitter",
        "Transmitter rejected address",
        "transmitter can't talk to receiver's address",
        "Check for address change on transmitter or receiver.",
    ),
    DanteSubscriptionStatus.TX_SCHEDULER_FAILURE: (
        "TX Scheduler failure",
        "This is most often caused by a receiver with  < 1ms unicast latency subscribing to a transmitter on a 100MB connection",
    ),
    DanteSubscriptionStatus.UNRESOLVED: (
        "Subscription unresolved",
        "Unresolved",
        "cannot find this channel on the network",
    ),
    DanteSubscriptionStatus.UNSUPPORTED: (
        "Unsupported feature",
        "The subscription cannot be completed as it requires features that are not supported on this device",
    ),
}
