use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionStatus {
    pub code: u16,
    pub receiver_status_code: Option<u16>,
    pub status: Option<&'static str>,
    pub state: &'static str,
    pub severity: &'static str,
    pub label: &'static str,
    pub detail: Option<&'static str>,
    pub observed_summary: Option<&'static str>,
    pub interpretation: &'static str,
}

pub fn decode(code: u16, receiver_status_code: Option<u16>) -> SubscriptionStatus {
    let definition = match code {
        0x0001 => match receiver_status_code {
            Some(0x0101) => ("DYNAMIC", "connected", "ok", "Subscribed (automatic flow)", Some("The subscription uses an automatically configured source flow."), "CONNECTED"),
            Some(0x0000) => ("UNRESOLVED", "unresolved", "error", "Unresolved", Some("The source channel has not been located."), "ERROR"),
            _ => return unknown(code, receiver_status_code, "receiver_context_required"),
        },
        0x0000 => ("NONE", "none", "none", "Not subscribed", None, "NONE"),
        0x0002 => ("RESOLVED", "resolved", "progress", "Resolved", Some("The source was located and is awaiting processing."), "IN_PROGRESS"),
        0x0003 => ("RESOLVE_FAIL", "error", "error", "Resolve failed", Some("The source channel lookup encountered an error."), "ERROR"),
        0x0004 => ("SUBSCRIBE_SELF", "connected", "ok", "Subscribed (self)", Some("Subscribed to a channel on this same device."), "CONNECTED"),
        0x0005 => ("RESOLVED_NONE", "error", "error", "Source not present", Some("The lookup confirmed that the source channel is absent."), "ERROR"),
        0x0007 => ("IDLE", "idle", "none", "Idle", Some("The configured flow needs more information before a connection can be established."), "NONE"),
        0x0008 => ("IN_PROGRESS", "in_progress", "progress", "Establishing flow", Some("Setting up the flow with the transmitter."), "IN_PROGRESS"),
        0x0009 => ("DYNAMIC", "connected", "ok", "Subscribed (automatic flow)", Some("The subscription uses an automatically configured source flow."), "CONNECTED"),
        0x000a => ("STATIC", "connected", "ok", "Subscribed (configured flow)", Some("The subscription uses a source flow configured manually."), "CONNECTED"),
        0x000e => ("MANUAL", "connected", "ok", "Manually configured", Some("The flow was set up directly, outside the usual subscription procedure."), "CONNECTED"),
        0x000f => ("NO_CONNECTION", "error", "error", "No connection", Some("The receiver could not establish communication with the source."), "ERROR"),
        0x0010 => ("CHANNEL_FORMAT", "error", "error", "Channel format mismatch", Some("The source and destination channel formats differ."), "ERROR"),
        0x0011 => ("BUNDLE_FORMAT", "error", "error", "Flow format mismatch", Some("The source flow format is incompatible with the receiver."), "ERROR"),
        0x0012 => ("NO_RX", "error", "error", "No more receiver resources", Some("The receiver cannot allocate resources for another flow."), "ERROR"),
        0x0013 => ("RX_FAIL", "error", "error", "Receiver setup failed", Some("Flow creation failed on the receiver."), "ERROR"),
        0x0014 => ("NO_TX", "error", "error", "No more transmitter resources", Some("The transmitter cannot allocate resources for another flow."), "ERROR"),
        0x0015 => ("TX_FAIL", "error", "error", "Transmitter setup failed", Some("Flow creation failed on the transmitter."), "ERROR"),
        0x0016 => ("QOS_FAIL_RX", "error", "error", "Receiver bandwidth exceeded", Some("The incoming flow exceeds the receiver bandwidth allowance."), "ERROR"),
        0x0017 => ("QOS_FAIL_TX", "error", "error", "Transmitter bandwidth exceeded", Some("The outgoing flow exceeds the transmitter bandwidth allowance."), "ERROR"),
        0x0018 => ("TX_REJECTED_ADDR", "error", "error", "Receiver address rejected", Some("The transmitter rejected the destination address."), "ERROR"),
        0x0019 => ("INVALID_MSG", "error", "error", "Flow request rejected", Some("The transmitter rejected an invalid flow request."), "ERROR"),
        0x001a => ("CHANNEL_LATENCY", "error", "error", "Unsupported channel latency", Some("The source latency exceeds what the receiver supports."), "ERROR"),
        0x001b => ("CLOCK_DOMAIN", "error", "error", "Clock domain mismatch", Some("The source and destination use different clock subdomains."), "ERROR"),
        0x001c => ("UNSUPPORTED", "error", "error", "Unsupported feature", Some("The subscription needs a feature the device does not support."), "ERROR"),
        0x001d => ("RX_LINK_DOWN", "error", "error", "Receiver links down", Some("No receiver network link is available."), "ERROR"),
        0x001e => ("TX_LINK_DOWN", "error", "error", "Transmitter links down", Some("No transmitter network link is available."), "ERROR"),
        0x001f => ("DYNAMIC_PROTOCOL", "error", "error", "No suitable dynamic protocol", Some("No suitable protocol was found for the automatic connection."), "ERROR"),
        0x0020 => ("INVALID_CHANNEL", "error", "error", "Invalid channel", Some("The requested channel does not exist."), "ERROR"),
        0x0021 => ("TX_SCHEDULER_FAILURE", "error", "error", "Transmitter scheduling failed", Some("The transmitter could not schedule the flow."), "ERROR"),
        0x0022 => ("SUBSCRIBE_SELF_POLICY", "error", "error", "Self-subscription not allowed", Some("This device does not permit the requested local subscription."), "ERROR"),
        0x0023 => ("TX_NOT_READY", "pending", "warning", "External transmitter issue", Some("An external condition affects the source channel."), "WARNING"),
        0x0024 => ("RX_NOT_READY", "pending", "warning", "External receiver issue", Some("An external condition affects the destination channel."), "WARNING"),
        0x0025 => ("TX_FANOUT_LIMIT_REACHED", "error", "error", "No more unicast flows", Some("The transmitter has reached its unicast flow capacity."), "ERROR"),
        0x0026 => ("TX_CHANNEL_ENCRYPTED", "error", "error", "Encrypted source channel", Some("The receiver cannot use the source signal encryption."), "ERROR"),
        0x0027 => ("TX_RESPONSE_UNEXPECTED", "error", "error", "Unexpected transmitter response", Some("The source returned an unexpected reply."), "ERROR"),
        0x0040 => ("TEMPLATE_MISMATCH_DEVICE", "error", "error", "Template device mismatch", Some("The subscription source name differs from the template device name."), "ERROR"),
        0x0041 => ("TEMPLATE_MISMATCH_FORMAT", "error", "error", "Template format mismatch", Some("The channel format is incompatible with the template flow."), "ERROR"),
        0x0042 => ("TEMPLATE_MISSING_CHANNEL", "error", "error", "Channel absent from template flow", Some("The selected multicast flow does not contain the requested channel."), "ERROR"),
        0x0043 => ("TEMPLATE_MISMATCH_CONFIG", "error", "error", "Template configuration mismatch", Some("The template configuration prevents use of the selected flow."), "ERROR"),
        0x0044 => ("TEMPLATE_FULL", "error", "error", "Template capacity reached", Some("The unicast template has no remaining capacity."), "ERROR"),
        0x0045 => ("RX_UNSUPPORTED_SUB_MODE", "error", "error", "Receiver subscription mode unavailable", Some("The receiver has no supported unicast or multicast mode available for this subscription."), "ERROR"),
        0x0046 => ("TX_UNSUPPORTED_SUB_MODE", "error", "error", "Transmitter subscription mode unavailable", Some("The transmitter has no supported unicast or multicast mode available for this subscription."), "ERROR"),
        0x0060 => ("TX_ACCESS_CONTROL_DENIED", "error", "error", "Transmitter access denied", Some("The source access policy rejected this subscription."), "ERROR"),
        0x0061 => ("TX_ACCESS_CONTROL_PENDING", "error", "error", "Awaiting transmitter access decision", Some("The source has not completed its access decision; DDM reports an error summary."), "ERROR"),
        0x0070 => ("HDCP_NEGOTIATION_FAILED", "error", "error", "HDCP negotiation failed", Some("The devices could not agree on HDCP keys."), "ERROR"),
        0x0071 => ("RX_ENCRYPTION_UNSUPPORTED", "error", "error", "Receiver encryption schemes unsupported", Some("The receiver supports none of the encryption schemes required by the source."), "ERROR"),
        0x0072 => ("RX_TRANSPORT_UNSUPPORTED", "error", "error", "Receiver transport unsupported", Some("The receiver cannot use the transport required by the source."), "ERROR"),
        0x00ff => ("SYSTEM_FAIL", "error", "error", "System failure", Some("An unexpected system error prevented the subscription."), "ERROR"),
        _ => return unknown(code, receiver_status_code, "unknown"),
    };
    let observed =
        receiver_status_code == Some(0x0101) || (code == 1 && receiver_status_code == Some(0));
    SubscriptionStatus {
        code,
        receiver_status_code,
        status: Some(definition.0),
        state: definition.1,
        severity: definition.2,
        label: definition.3,
        detail: definition.4,
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
        state: "unknown",
        severity: "warning",
        label: "Unknown subscription status",
        detail: Some("The available observations do not establish a classification for this value and receiver context."),
        observed_summary: None,
        interpretation,
    }
}

pub fn state_for_identifier(identifier: &str) -> &'static str {
    (0..=255)
        .map(|code| decode(code, Some(0x0101)))
        .chain(std::iter::once(decode(1, Some(0))))
        .find(|entry| entry.status == Some(identifier))
        .map_or("unknown", |entry| entry.state)
}

#[cfg(test)]
mod tests;
