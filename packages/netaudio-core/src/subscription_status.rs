use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionTransport {
    Unicast,
    Multicast,
}

fn presentation(identifier: &str) -> Option<(&'static str, Option<&'static str>)> {
    Some(match identifier {
        "BUNDLE_FORMAT" => ("Multicast flow format mismatch", Some("The multicast flow format isn't compatible with the receiver.")),
        "CHANNEL_FORMAT" => ("Channel format mismatch", Some("The source and destination channel formats differ.")),
        "CHANNEL_LATENCY" => ("Incorrect channel latencies", Some("The source requires more latency than the receiver supports.")),
        "CLOCK_DOMAIN" => ("Clock domain mismatch", Some("The transmitter and receiver aren't in the same clock domain.")),
        "DYNAMIC" => ("Subscribed (unicast)", None),
        "DYNAMIC_PROTOCOL" => ("Dynamic protocol", None),
        "IDLE" => ("Idle", None),
        "IN_PROGRESS" => ("Establishing flow", Some("Setting up the flow with the transmitter.")),
        "INVALID_CHANNEL" => ("Invalid channel", Some("The subscription can't complete because the channel is invalid.")),
        "INVALID_MSG" => ("Request rejected by transmitter", Some("The transmitter couldn't interpret the receiver's request.")),
        "MANUAL" => ("Manually configured", None),
        "NONE" => ("Not subscribed", None),
        "NO_CONNECTION" => ("No connection", Some("Couldn't reach the transmitter.")),
        "NO_RX" => ("No more flows (Rx)", Some("The receiver can't take on any more flows.")),
        "NO_TX" => ("No more flows (Tx)", Some("The transmitter can't supply any more flows.")),
        "QOS_FAIL_RX" => ("Rx bandwidth exceeded", Some("The receiver can't reliably take on more inbound flows.")),
        "QOS_FAIL_TX" => ("Tx bandwidth exceeded", Some("The transmitter can't reliably take on more outbound flows.")),
        "RESOLVED" => ("Resolved", Some("The source was found; the flow is being set up.")),
        "RESOLVED_NONE" => ("Source not present", Some("The source channel isn't present on the network.")),
        "RESOLVE_FAIL" => ("Resolve failed", Some("An error occurred while looking up the source channel.")),
        "RX_FAIL" => ("Receiver setup failed", Some("An error occurred on the receiver.")),
        "RX_LINK_DOWN" => ("Receiver link down", Some("The subscription can't complete while the receiver's link is down.")),
        "RX_NOT_READY" => ("External receiver not ready", None),
        "STATIC" => ("Subscribed (multicast)", None),
        "SUBSCRIBE_SELF" => ("Subscribed (self)", Some("Subscribed to a channel on this same device.")),
        "SUBSCRIBE_SELF_POLICY" => ("Self-subscription not allowed", Some("This device doesn't permit local subscriptions between these channels.")),
        "TX_CHANNEL_ENCRYPTED" => ("Encryption unsupported (Rx)", Some("The receiver doesn't support the required signal encryption.")),
        "TX_FAIL" => ("Transmitter setup failed", Some("An error occurred on the transmitter.")),
        "TX_FANOUT_LIMIT_REACHED" => ("No more unicast flows (Tx)", Some("The transmitter can't supply any more unicast flows.")),
        "TX_LINK_DOWN" => ("Transmitter link down", Some("The subscription can't complete while the transmitter's link is down.")),
        "TX_NOT_READY" => ("External transmitter not ready", None),
        "TX_REJECTED_ADDR" => ("Address rejected by transmitter", Some("The transmitter can't reach the receiver's address.")),
        "TX_SCHEDULER_FAILURE" => ("Tx scheduler failure", Some("Often caused by a receiver asking for under 1 ms unicast latency from a transmitter on a 100 Mbps link.")),
        "UNRESOLVED" => ("Unresolved", Some("The transmitting device isn't currently on the network.")),
        "UNSUPPORTED" => ("Unsupported feature", Some("The subscription needs a feature this device doesn't support.")),
        _ => return None,
    })
}

const SUBSCRIBE_SELF: u16 = 0x0004;

pub fn is_self_connection(source_device: Option<&str>, status_code: u16) -> bool {
    source_device == Some(".") || status_code == SUBSCRIBE_SELF
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SubscriptionStatus {
    pub transport: Option<SubscriptionTransport>,
    pub code: u16,
    pub receiver_status_code: Option<u16>,
    pub status: Option<&'static str>,
    pub state: &'static str,
    pub severity: &'static str,
    pub settled: bool,
    pub label: &'static str,
    pub detail: Option<&'static str>,
    pub observed_summary: Option<&'static str>,
    pub interpretation: &'static str,
}

pub fn decode(code: u16, receiver_status_code: Option<u16>) -> SubscriptionStatus {
    let definition = match code {
        0x0001 => match receiver_status_code {
            Some(0x0101) => ("DYNAMIC", "connected", "ok", "DYNAMIC", Some("Active subscription to an automatically configured source flow"), "CONNECTED"),
            Some(0x0000) => ("UNRESOLVED", "unresolved", "error", "UNRESOLVED", Some("Error: Channel Name not yet found on network"), "ERROR"),
            _ => return unknown(code, receiver_status_code, "receiver_context_required"),
        },
        0x0000 => ("NONE", "none", "none", "NONE", Some("No subscription for this channel"), "NONE"),
        0x0002 => ("RESOLVED", "resolved", "progress", "RESOLVED", Some("Channel Name has been found, but not yet processed"), "IN_PROGRESS"),
        0x0003 => ("RESOLVE_FAIL", "error", "error", "RESOLVE_FAIL", Some("Error: an error occurred while trying to resolve the channel name"), "ERROR"),
        SUBSCRIBE_SELF => ("SUBSCRIBE_SELF", "connected", "ok", "SUBSCRIBE_SELF", Some("Channel is successfully subscribed to own TX channels (local loopback mode)"), "CONNECTED"),
        0x0005 => ("RESOLVED_NONE", "error", "error", "RESOLVED_NONE", Some("Error: Channel Name explicitly does not exist on this network"), "ERROR"),
        0x0007 => ("IDLE", "idle", "none", "IDLE", Some("A flow has been configured but does not have sufficient information to establish an audio connection"), "NONE"),
        0x0008 => ("IN_PROGRESS", "in_progress", "progress", "IN_PROGRESS", Some("Channel Name has been found and processed; setting up flow now"), "IN_PROGRESS"),
        0x0009 => ("DYNAMIC", "connected", "ok", "DYNAMIC", Some("Active subscription to an automatically configured source flow"), "CONNECTED"),
        0x000a => ("STATIC", "connected", "ok", "STATIC", Some("Active subscription to a manually configured source flow"), "CONNECTED"),
        0x000e => ("MANUAL", "connected", "ok", "MANUAL", Some("Manual flow configuration bypassing the standard subscription process"), "CONNECTED"),
        0x000f => ("NO_CONNECTION", "error", "error", "NO_CONNECTION", Some("Error: The name was found but the connection process failed (the receiver could not communicate with the transmitter)"), "ERROR"),
        0x0010 => ("CHANNEL_FORMAT", "error", "error", "CHANNEL_FORMAT", Some("Error: Channel formats do not match"), "ERROR"),
        0x0011 => ("BUNDLE_FORMAT", "error", "error", "BUNDLE_FORMAT", Some("Error: Flow formats do not match"), "ERROR"),
        0x0012 => ("NO_RX", "error", "error", "NO_RX", Some("Error: Receiver is out of resources (e.g. flows)"), "ERROR"),
        0x0013 => ("RX_FAIL", "error", "error", "RX_FAIL", Some("Error: Receiver couldn't set up the flow"), "ERROR"),
        0x0014 => ("NO_TX", "error", "error", "NO_TX", Some("Error: Transmitter is out of resources (e.g. flows)"), "ERROR"),
        0x0015 => ("TX_FAIL", "error", "error", "TX_FAIL", Some("Error: Transmitter couldn't set up the flow"), "ERROR"),
        0x0016 => ("QOS_FAIL_RX", "error", "error", "QOS_FAIL_RX", Some("Error: Receiver got a QoS failure (too much data) when setting up the flow"), "ERROR"),
        0x0017 => ("QOS_FAIL_TX", "error", "error", "QOS_FAIL_TX", Some("Error: Transmitter got a QoS failure (too much data) when setting up the flow"), "ERROR"),
        0x0018 => ("TX_REJECTED_ADDR", "error", "error", "TX_REJECTED_ADDR", Some("Error: Tx rejected the address given by rx (usually indicates an ARP failure)"), "ERROR"),
        0x0019 => ("INVALID_MSG", "error", "error", "INVALID_MSG", Some("Error: Transmitter rejected the bundle request as invalid"), "ERROR"),
        0x001a => ("CHANNEL_LATENCY", "error", "error", "CHANNEL_LATENCY", Some("Error: Tx channel latency higher than maximum supported Rx latency"), "ERROR"),
        0x001b => ("CLOCK_DOMAIN", "error", "error", "CLOCK_DOMAIN", Some("Error: Tx and Rx and in different clock subdomains"), "ERROR"),
        0x001c => ("UNSUPPORTED", "error", "error", "UNSUPPORTED", Some("Error: Attempt to use an unsupported feature"), "ERROR"),
        0x001d => ("RX_LINK_DOWN", "error", "error", "RX_LINK_DOWN", Some("Error: All Rx links are down"), "ERROR"),
        0x001e => ("TX_LINK_DOWN", "error", "error", "TX_LINK_DOWN", Some("Error: All Tx links are down"), "ERROR"),
        0x001f => ("DYNAMIC_PROTOCOL", "error", "error", "DYNAMIC_PROTOCOL", Some("Error: can't find suitable protocol for dynamic connection"), "ERROR"),
        0x0020 => ("INVALID_CHANNEL", "error", "error", "INVALID_CHANNEL", Some("Error: Channel does not exist (eg no such local channel)"), "ERROR"),
        0x0021 => ("TX_SCHEDULER_FAILURE", "error", "error", "TX_SCHEDULER_FAILURE", Some("Error: Tx Scheduler failure"), "ERROR"),
        0x0022 => ("SUBSCRIBE_SELF_POLICY", "error", "error", "SUBSCRIBE_SELF_POLICY", Some("Error: The given subscription to self was disallowed by the device"), "ERROR"),
        0x0023 => ("TX_NOT_READY", "pending", "warning", "TX_NOT_READY", Some("Warning: There is an external issue with Tx Channel"), "WARNING"),
        0x0024 => ("RX_NOT_READY", "pending", "warning", "RX_NOT_READY", Some("Warning: There is an external issue with Rx Channel"), "WARNING"),
        0x0025 => ("TX_FANOUT_LIMIT_REACHED", "error", "error", "TX_FANOUT_LIMIT_REACHED", Some("Error: Tx device cannot support additional unicast flows"), "ERROR"),
        0x0026 => ("TX_CHANNEL_ENCRYPTED", "error", "error", "TX_CHANNEL_ENCRYPTED", Some("Error: Rx device does not support the signal encryption"), "ERROR"),
        0x0027 => ("TX_RESPONSE_UNEXPECTED", "error", "error", "TX_RESPONSE_UNEXPECTED", Some("Error: Unexpected response from TX device"), "ERROR"),
        0x0040 => ("TEMPLATE_MISMATCH_DEVICE", "error", "error", "TEMPLATE_MISMATCH_DEVICE", Some("Error: Template-based subscription failed: template and subscription device names don't match"), "ERROR"),
        0x0041 => ("TEMPLATE_MISMATCH_FORMAT", "error", "error", "TEMPLATE_MISMATCH_FORMAT", Some("Error: Template-based subscription failed: flow and channel formats don't match"), "ERROR"),
        0x0042 => ("TEMPLATE_MISSING_CHANNEL", "error", "error", "TEMPLATE_MISSING_CHANNEL", Some("Error: Template-based subscription failed: the channel is not part of the given multicast flow"), "ERROR"),
        0x0043 => ("TEMPLATE_MISMATCH_CONFIG", "error", "error", "TEMPLATE_MISMATCH_CONFIG", Some("Error: Template-based subscription failed: something else about the template configuration made it impossible to complete the subscription using the given flow"), "ERROR"),
        0x0044 => ("TEMPLATE_FULL", "error", "error", "TEMPLATE_FULL", Some("Error: Template-based subscription failed: the unicast template is full"), "ERROR"),
        0x0045 => ("RX_UNSUPPORTED_SUB_MODE", "error", "error", "RX_UNSUPPORTED_SUB_MODE", Some("Error: Rx device does not have a supported subscription mode (unicast/multicast) available"), "ERROR"),
        0x0046 => ("TX_UNSUPPORTED_SUB_MODE", "error", "error", "TX_UNSUPPORTED_SUB_MODE", Some("Error: Tx device does not have a supported subscription mode (unicast/multicast) available"), "ERROR"),
        0x0060 => ("TX_ACCESS_CONTROL_DENIED", "error", "error", "TX_ACCESS_CONTROL_DENIED", Some("Error: Tx access control denied the request"), "ERROR"),
        0x0061 => ("TX_ACCESS_CONTROL_PENDING", "error", "error", "TX_ACCESS_CONTROL_PENDING", Some("Tx access control request is in progress"), "ERROR"),
        0x0070 => ("HDCP_NEGOTIATION_FAILED", "error", "error", "HDCP_NEGOTIATION_FAILED", Some("Error: HDCP key negotiation failed"), "ERROR"),
        0x0071 => ("RX_ENCRYPTION_UNSUPPORTED", "error", "error", "RX_ENCRYPTION_UNSUPPORTED", Some("Error: TX requires encryption but RX does not support any of the requested encryption schemes"), "ERROR"),
        0x0072 => ("RX_TRANSPORT_UNSUPPORTED", "error", "error", "RX_TRANSPORT_UNSUPPORTED", Some("Error: TX requires a transport protocol but RX does not support it"), "ERROR"),
        0x00ff => ("SYSTEM_FAIL", "error", "error", "SYSTEM_FAIL", Some("Error: Unexpected system failure"), "ERROR"),
        _ => return unknown(code, receiver_status_code, "unknown"),
    };
    let observed =
        receiver_status_code == Some(0x0101) || (code == 1 && receiver_status_code == Some(0));
    let (label, detail) = presentation(definition.0).unwrap_or((definition.3, None));

    SubscriptionStatus {
        code,
        receiver_status_code,
        status: Some(definition.0),
        transport: match definition.0 {
            "DYNAMIC" => Some(SubscriptionTransport::Unicast),
            "STATIC" => Some(SubscriptionTransport::Multicast),
            _ => None,
        },
        state: definition.1,
        severity: definition.2,
        settled: matches!(definition.1, "connected" | "error" | "unresolved"),
        label,
        detail: detail.or(definition.4),
        observed_summary: observed.then_some(definition.5),
        interpretation: if observed {
            "observed"
        } else {
            "receiver_context_unverified"
        },
    }
}

fn unknown(
    code: u16,
    receiver_status_code: Option<u16>,
    interpretation: &'static str,
) -> SubscriptionStatus {
    SubscriptionStatus {
        code,
        receiver_status_code,
        status: None,
        transport: None,
        state: "unknown",
        severity: "warning",
        settled: false,
        label: "Unknown subscription status",
        detail: Some("The available observations do not establish a classification for this value and receiver context."),
        observed_summary: None,
        interpretation,
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SubscriptionClassification {
    pub transport: Option<SubscriptionTransport>,
    pub state: &'static str,
    pub severity: &'static str,
    pub settled: bool,
    pub label: Option<String>,
    pub detail: Option<String>,
}

pub fn classification_for_identifier(identifier: &str) -> SubscriptionClassification {
    let entry = (0..=255)
        .map(|code| decode(code, Some(0x0101)))
        .chain(std::iter::once(decode(1, Some(0))))
        .find(|entry| entry.status == Some(identifier))
        .unwrap_or_else(|| unknown(0, None, "unknown"));
    let display = presentation(identifier);

    SubscriptionClassification {
        transport: entry.transport,
        state: entry.state,
        severity: entry.severity,
        settled: entry.settled,
        label: display.map(|(label, _)| label.to_owned()),
        detail: display.and_then(|(_, detail)| detail).map(str::to_owned),
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ManagedStatusRequest {
    status: Option<serde_json::Value>,
    status_message: Option<serde_json::Value>,
    summary: Option<serde_json::Value>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagedSubscriptionStatus {
    #[serde(flatten)]
    classification: SubscriptionClassification,
    status: Option<String>,
}

fn text(value: &Option<serde_json::Value>) -> Option<&str> {
    value
        .as_ref()
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

pub fn managed_status(request: ManagedStatusRequest) -> ManagedSubscriptionStatus {
    let identifier = text(&request.status).map(str::to_uppercase);
    let summary = text(&request.summary).map(str::to_uppercase);
    let mut classification =
        classification_for_identifier(identifier.as_deref().unwrap_or_default());
    let message = text(&request.status_message).and_then(|message| {
        let lower = message.to_ascii_lowercase();
        let prefix = ["error:", "warning:", "info:"]
            .into_iter()
            .find(|prefix| lower.starts_with(prefix));
        let cleaned = prefix
            .map_or(message, |prefix| &message[prefix.len()..])
            .trim();
        (!cleaned.is_empty()).then(|| cleaned.to_owned())
    });

    if classification.label.is_none() {
        classification.label = Some(identifier.as_deref().or(summary.as_deref()).map_or_else(
            || "Status unavailable".to_owned(),
            |value| {
                let human = value.replace('_', " ").to_lowercase();
                let mut chars = human.chars();
                chars
                    .next()
                    .map(|first| first.to_uppercase().to_string() + chars.as_str())
                    .unwrap_or_default()
            },
        ));
    }

    classification.detail = classification.detail.or(message);
    // An optimistic aggregate summary cannot erase a known per-channel failure.
    classification.severity = match summary.as_deref() {
        Some("ERROR") => "error",
        Some("WARNING") if classification.severity != "error" => "warning",
        _ => classification.severity,
    };

    ManagedSubscriptionStatus {
        classification,
        status: identifier,
    }
}

#[cfg(test)]
mod tests;
