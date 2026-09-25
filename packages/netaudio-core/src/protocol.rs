#[repr(u16)]
pub enum NetaudioProtocol {
    DefaultArc = 0x27FF,
    Arc2729 = 0x2729,
    Arc2801 = 0x2801,
    Arc2809 = 0x2809,
    Arc280C = 0x280C,
    Arc280F = 0x280F,
    Cmc = 0x1200,
    Settings = 0xFFFF,
}

#[derive(serde::Serialize)]
pub struct ProtocolMetadata {
    pub protocol_id: u16,
    pub family: &'static str,
    pub modern_arc: bool,
}

pub fn protocol_catalog() -> Vec<ProtocolMetadata> {
    use NetaudioProtocol::*;

    [
        Arc2729, DefaultArc, Arc2801, Arc2809, Arc280C, Arc280F, Cmc, Settings,
    ]
    .into_iter()
    .map(|protocol| {
        let family = match protocol {
            Cmc => "CMC",
            Settings => "SETTINGS",
            DefaultArc | Arc2729 | Arc2801 | Arc2809 | Arc280C | Arc280F => "ARC",
        };
        let protocol_id = protocol as u16;

        ProtocolMetadata {
            protocol_id,
            family,
            modern_arc: is_modern_arc_protocol(protocol_id),
        }
    })
    .collect()
}

#[repr(u16)]
pub enum NetaudioPort {
    Arc = 4440,
    ArcSecondary = 4455,
    Settings = 8700,
    Info = 8702,
    Heartbeat = 8708,
    Control = 8800,
    ControllerMetering = 8751,
    MulticastMetering = 8752,
}

pub const PROTOCOL_ID: u16 = NetaudioProtocol::DefaultArc as u16;
pub const PROTOCOL_ARC_2809: u16 = NetaudioProtocol::Arc2809 as u16;
pub const PROTOCOL_ARC_280C: u16 = NetaudioProtocol::Arc280C as u16;
pub const PROTOCOL_ARC_280F: u16 = NetaudioProtocol::Arc280F as u16;
pub const OPCODE_CHANNEL_COUNT: u16 = 0x1000;
pub const OPCODE_DEVICE_NAME_SET: u16 = 0x1001;
pub const OPCODE_TX_CHANNEL_INFO: u16 = 0x2000;
pub const OPCODE_TX_CHANNEL_NAMES: u16 = 0x2010;
pub const OPCODE_RX_CHANNELS: u16 = 0x3000;
pub const SERVICE_ARC: &str = "_netaudio-arc._udp.local.";
pub const SERVICE_CHAN: &str = "_netaudio-chan._udp.local.";
pub const SERVICE_CMC: &str = "_netaudio-cmc._udp.local.";
pub const SERVICE_DBC: &str = "_netaudio-dbc._udp.local.";
pub const SERVICE_VIDEO: &str = "_dantevideo._udp.local.";
pub const MULTICAST_GROUP_HEARTBEAT: &str = "224.0.0.233";
pub const MULTICAST_GROUP_CONTROL_MONITORING: &str = "224.0.0.231";
pub const DANTE_NAME_MAX_LENGTH: usize = 31;
pub const RESPONSE_HEADER_SIZE: usize = 10;
#[repr(u16)]
pub enum NetaudioResultCode {
    Request = 0,
    Success = 1,
    Error = 0x0022,
    FrontendUnavailable = 0x0030,
    MorePages = 0x8112,
}

pub const RESULT_CODE_SUCCESS: u16 = NetaudioResultCode::Success as u16;
pub const RESULT_CODE_MORE_PAGES: u16 = NetaudioResultCode::MorePages as u16;
pub const RESULT_CODE_FRONTEND_UNAVAILABLE: u16 = NetaudioResultCode::FrontendUnavailable as u16;

pub struct ArcResultStatus {
    pub accepted: bool,
    pub name: Option<&'static str>,
    pub label: Option<&'static str>,
}

pub fn arc_result_status(code: u16) -> ArcResultStatus {
    let (accepted, name, label) = match code {
        RESULT_CODE_SUCCESS => (true, Some("RESULT_CODE_SUCCESS"), Some("success")),
        RESULT_CODE_MORE_PAGES => (
            true,
            Some("RESULT_CODE_SUCCESS_EXTENDED"),
            Some("success (paginated)"),
        ),
        value if value == NetaudioResultCode::Request as u16 => (false, None, Some("request")),
        value if value == NetaudioResultCode::Error as u16 => {
            (false, Some("RESULT_CODE_ERROR"), Some("error"))
        }
        _ => (false, None, None),
    };

    ArcResultStatus {
        accepted,
        name,
        label,
    }
}
pub const COMMON_ARC_PROTOCOL_IDS: [u16; 3] = [
    PROTOCOL_ID,
    NetaudioProtocol::Arc2729 as u16,
    PROTOCOL_ARC_2809,
];
pub const DEVICE_SETTINGS_ARC_PROTOCOL_IDS: [u16; 4] = [
    PROTOCOL_ID,
    NetaudioProtocol::Arc2729 as u16,
    NetaudioProtocol::Arc2801 as u16,
    PROTOCOL_ARC_2809,
];
pub const MODERN_ARC_PROTOCOL_IDS: [u16; 3] =
    [PROTOCOL_ARC_2809, PROTOCOL_ARC_280C, PROTOCOL_ARC_280F];

pub fn next_message_id(previous: u16) -> u16 {
    previous.wrapping_add(1).max(1)
}

pub fn allocate_message_id() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};

    static MESSAGE_ID: AtomicU16 = AtomicU16::new(0);
    let previous = MESSAGE_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
            Some(next_message_id(previous))
        })
        .expect("message ID allocation always advances");

    next_message_id(previous)
}

pub fn is_supported_arc_protocol(protocol_id: u16) -> bool {
    DEVICE_SETTINGS_ARC_PROTOCOL_IDS.contains(&protocol_id) || is_modern_arc_protocol(protocol_id)
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ArcProtocol {
    pub protocol_id: u16,
    pub modern_channel_inventory: bool,
    pub subscription_page: bool,
    pub subscription_batch_limit: usize,
    pub channel_name_probe_protocol_id: u16,
    pub flow_query_protocol_ids: Vec<u16>,
}

#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowInventoryProtocolFacts {
    pub observed: Option<u16>,
    pub version: Option<String>,
    pub managed: bool,
}

pub fn flow_inventory_protocol(
    facts: FlowInventoryProtocolFacts,
) -> Result<Option<u16>, &'static str> {
    if let Some(observed) = facts.observed {
        return arc_protocol_for_identifier(observed, facts.managed)
            .map(|protocol| Some(protocol.protocol_id));
    }

    arc_protocol(facts.version.as_deref(), facts.managed)
        .map(|protocol| protocol.map(|protocol| protocol.protocol_id))
}

pub fn arc_protocol(
    version: Option<&str>,
    managed: bool,
) -> Result<Option<ArcProtocol>, &'static str> {
    let protocol_id = if managed {
        // Managed control's observed ARC transport uses this explicit revision.
        PROTOCOL_ARC_2809
    } else {
        let Some(version) = version else {
            return Ok(None);
        };

        let mut parts = version.split('.');
        let mut component = || {
            let part = parts.next()?;
            if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }

            part.parse::<u16>().ok()
        };
        let components = (component(), component(), component());
        let (Some(major), Some(minor), Some(patch)) = components else {
            return Err("unsupported ARC protocol version");
        };

        if parts.next().is_some() || major > 15 || minor > 15 || patch > 255 {
            return Err("unsupported ARC protocol version");
        }

        (major << 12) | (minor << 8) | patch
    };

    arc_protocol_for_identifier(protocol_id, managed).map(Some)
}

pub fn arc_protocol_for_identifier(
    protocol_id: u16,
    managed: bool,
) -> Result<ArcProtocol, &'static str> {
    if !is_supported_arc_protocol(protocol_id) {
        return Err("unsupported ARC protocol version");
    }

    Ok(ArcProtocol {
        protocol_id,
        modern_channel_inventory: is_modern_arc_protocol(protocol_id),
        subscription_page: protocol_id == PROTOCOL_ARC_280F,
        subscription_batch_limit: if managed || protocol_id == PROTOCOL_ARC_280F {
            crate::commands::SUBSCRIPTION_PAGE_CAPACITY
        } else {
            crate::commands::LEGACY_SUBSCRIPTION_BATCH_CAPACITY
        },
        channel_name_probe_protocol_id: if is_modern_arc_protocol(protocol_id) {
            protocol_id
        } else {
            PROTOCOL_ARC_2809
        },
        flow_query_protocol_ids: if is_modern_arc_protocol(protocol_id) {
            vec![protocol_id]
        } else {
            vec![0x2729, 0x2801, PROTOCOL_ARC_2809, PROTOCOL_ARC_280F]
        },
    })
}

const PROTOCOL_SETTINGS: u16 = 0xFFFF;

pub fn transmit_flow_inventory_protocols(
    advertised: u16,
    observed: u16,
) -> Result<Vec<u16>, &'static str> {
    use crate::commands::{PROTOCOL_DANTE_FLOW, PROTOCOL_DANTE_FLOW_2801};

    if !is_supported_arc_protocol(advertised)
        || (!matches!(observed, PROTOCOL_DANTE_FLOW | PROTOCOL_DANTE_FLOW_2801)
            && !is_modern_arc_protocol(observed))
    {
        return Err("unsupported transmitter inventory protocol");
    }

    if is_modern_arc_protocol(advertised) {
        return Ok(vec![advertised]);
    }

    if is_modern_arc_protocol(observed) {
        return Ok(vec![observed]);
    }

    Ok(vec![PROTOCOL_ARC_2809, observed])
}
const CONMON_MINIMUM_SIZE: usize = 28;
const CONMON_MAGIC_OFFSET: usize = 16;
const CONMON_OPCODE_OFFSET: usize = 26;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseEnvelope<'a> {
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub body: &'a [u8],
}

pub fn response_envelope(response: &[u8]) -> Option<ResponseEnvelope<'_>> {
    if response.len() < RESPONSE_HEADER_SIZE
        || usize::from(crate::bytes::read_u16(response, 2)?) != response.len()
    {
        return None;
    }
    Some(ResponseEnvelope {
        protocol_id: crate::bytes::read_u16(response, 0)?,
        transaction_id: crate::bytes::read_u16(response, 4)?,
        opcode: crate::bytes::read_u16(response, 6)?,
        result_code: crate::bytes::read_u16(response, 8)?,
        body: &response[RESPONSE_HEADER_SIZE..],
    })
}

pub fn validate_response_envelope<'a>(
    response: &'a [u8],
    expected_protocol_opcodes: &[(u16, u16)],
    accepted_results: &[u16],
) -> Option<ResponseEnvelope<'a>> {
    let envelope = response_envelope(response)?;
    if !expected_protocol_opcodes.contains(&(envelope.protocol_id, envelope.opcode))
        || !accepted_results.contains(&envelope.result_code)
    {
        return None;
    }
    Some(envelope)
}

pub fn common_arc_protocol_opcodes(opcode: u16) -> [(u16, u16); 3] {
    COMMON_ARC_PROTOCOL_IDS.map(|protocol_id| (protocol_id, opcode))
}

pub fn device_settings_arc_protocol_opcodes(opcode: u16) -> [(u16, u16); 4] {
    DEVICE_SETTINGS_ARC_PROTOCOL_IDS.map(|protocol_id| (protocol_id, opcode))
}

pub fn modern_arc_protocol_opcodes(opcode: u16) -> [(u16, u16); 3] {
    MODERN_ARC_PROTOCOL_IDS.map(|protocol_id| (protocol_id, opcode))
}

pub fn is_modern_arc_protocol(protocol_id: u16) -> bool {
    MODERN_ARC_PROTOCOL_IDS.contains(&protocol_id)
}

pub fn is_common_arc_protocol(protocol_id: u16) -> bool {
    COMMON_ARC_PROTOCOL_IDS.contains(&protocol_id)
}

pub fn conmon_opcode(data: &[u8]) -> Option<u16> {
    if data.len() < CONMON_MINIMUM_SIZE
        || crate::bytes::read_u16(data, 0)? != PROTOCOL_SETTINGS
        || usize::from(crate::bytes::read_u16(data, 2)?) != data.len()
        || crate::bytes::read_u16(data, 6)? != 0
        || data.get(CONMON_MAGIC_OFFSET..CONMON_MAGIC_OFFSET + 8)? != b"Audinate"
    {
        return None;
    }
    crate::bytes::read_u16(data, CONMON_OPCODE_OFFSET)
}

#[derive(serde::Serialize)]
pub struct DiagnosticPacketHeader {
    pub family: &'static str,
    pub protocol_name: &'static str,
    pub result_name: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_accepted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_label: Option<&'static str>,
    pub protocol_id: u16,
    pub length: u16,
    pub transaction_id: Option<u16>,
    pub opcode: u16,
    pub opcode_name: Option<ArcOperation>,
    pub result_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_decoder: Option<ResponseDecoder>,
}

#[derive(serde::Serialize)]
pub struct ResponseDecoder {
    kind: &'static str,
    paged: bool,
}

fn response_decoder(protocol: u16, opcode: u16, result: Option<u16>) -> Option<ResponseDecoder> {
    if protocol == PROTOCOL_SETTINGS {
        let kind = crate::responses::conmon_response_kind(opcode)?;
        return Some(ResponseDecoder { kind, paged: false });
    }

    let result = result?;
    if result == 0 {
        return None;
    }

    use ArcOperation::*;
    let operation = arc_operation(protocol, opcode)?;
    let page = match operation {
        ReceiverChannels => Some("rx"),
        TransmitterChannels => Some("tx_info"),
        TransmitterChannelNames => Some("tx_friendly"),
        _ => None,
    };
    if let Some(kind) = page {
        return Some(ResponseDecoder { kind, paged: true });
    }

    if !matches!(result, RESULT_CODE_SUCCESS | RESULT_CODE_MORE_PAGES) {
        return None;
    }

    let kind = match operation {
        ChannelCount => "channel_count",
        DeviceInfo => "device_info",
        DeviceName => "device_name",
        DeviceSettings => "device_settings",
        PropertyDirectory => "property_directory",
        TransmitterFlows => "tx_flows",
        TransmitterChannelStatus => "modern_arc_transmitter_channel_status_page",
        TransmitterFlowStatus => "transmitter_flow_status_page",
        ReceiverFlows => "receiver_flow_page",
        ReceiverChannelStatus => "modern_arc_receiver_channel_status_page",
        ReceiverFlowStatus => "modern_arc_receiver_flow_status_page",
        _ => return None,
    };
    Some(ResponseDecoder { kind, paged: false })
}

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArcOperation {
    CmcRegistration,
    ChannelCount,
    SetDeviceName,
    DeviceName,
    DeviceInfo,
    DeviceSettings,
    SetLatency,
    PropertyDirectory,
    StoreCurrentConfiguration,
    TransmitterChannels,
    TransmitterChannelNames,
    ReceiverChannels,
    SetChannelName,
    AddSubscriptions,
    RemoveSubscriptions,
    TransmitterFlows,
    TransmitterFlowLabels,
    ReceiverFlows,
    ReceiverPortRanges,
    TransmitterChannelStatus,
    TransmitterFlowStatus,
    ReceiverChannelStatus,
    Subscriptions,
    ReceiverFlowStatus,
}

fn arc_operation(protocol: u16, opcode: u16) -> Option<ArcOperation> {
    use crate::commands;
    use ArcOperation::*;

    if !is_supported_arc_protocol(protocol) {
        return None;
    }

    if is_modern_arc_protocol(protocol) {
        let name = match opcode {
            commands::OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809 => {
                Some(TransmitterChannelStatus)
            }
            commands::OPCODE_QUERY_TX_FLOWS_2809 => Some(TransmitterFlowStatus),
            commands::OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809 => Some(ReceiverChannelStatus),
            commands::OPCODE_MODERN_ARC_SUBSCRIPTION => Some(Subscriptions),
            commands::OPCODE_QUERY_RECEIVER_FLOW_STATUS_2809 => Some(ReceiverFlowStatus),
            commands::OPCODE_SET_RECEIVER_CHANNEL_NAME_2809 => Some(SetChannelName),
            _ => None,
        };

        if name.is_some() {
            return name;
        }
    }

    match opcode {
        OPCODE_CHANNEL_COUNT => Some(ChannelCount),
        OPCODE_DEVICE_NAME_SET => Some(SetDeviceName),
        commands::OPCODE_DEVICE_NAME => Some(DeviceName),
        commands::OPCODE_DEVICE_INFO => Some(DeviceInfo),
        commands::OPCODE_DEVICE_SETTINGS => Some(DeviceSettings),
        commands::OPCODE_DEVICE_SETTINGS_SET => Some(SetLatency),
        commands::OPCODE_PROPERTY_DIRECTORY => Some(PropertyDirectory),
        commands::OPCODE_STORE_CURRENT_CONFIGURATION => Some(StoreCurrentConfiguration),
        OPCODE_TX_CHANNEL_INFO => Some(TransmitterChannels),
        OPCODE_TX_CHANNEL_NAMES => Some(TransmitterChannelNames),
        OPCODE_RX_CHANNELS => Some(ReceiverChannels),
        commands::OPCODE_TX_CHANNEL_NAME_SET | commands::OPCODE_RX_CHANNEL_NAME_SET => {
            Some(SetChannelName)
        }
        commands::OPCODE_SUBSCRIPTION_ADD => Some(AddSubscriptions),
        commands::OPCODE_SUBSCRIPTION_REMOVE => Some(RemoveSubscriptions),
        commands::OPCODE_QUERY_TX_FLOWS => Some(TransmitterFlows),
        commands::OPCODE_QUERY_TX_FLOW_LABELS => Some(TransmitterFlowLabels),
        commands::OPCODE_QUERY_RECEIVER_FLOWS => Some(ReceiverFlows),
        commands::OPCODE_QUERY_RECEIVER_PORT_RANGES => Some(ReceiverPortRanges),
        _ => None,
    }
}

#[derive(serde::Serialize)]
pub struct ControlRequestHeader {
    pub(crate) protocol_id: u16,
    pub(crate) transaction_id: u16,
    pub(crate) opcode: u16,
    operation: ArcOperation,
}

/// Validate the complete request envelope; operation-specific decoders validate its body.
pub fn control_request_header(data: &[u8]) -> Option<ControlRequestHeader> {
    use crate::bytes::read_u16;

    if usize::from(read_u16(data, 2)?) != data.len() || read_u16(data, 8)? != 0 {
        return None;
    }

    let protocol_id = read_u16(data, 0)?;
    let opcode = read_u16(data, 6)?;
    let operation = if protocol_id == crate::commands::PROTOCOL_CMC
        && opcode == crate::commands::OPCODE_CMC_REGISTER
    {
        ArcOperation::CmcRegistration
    } else {
        arc_operation(protocol_id, opcode)?
    };

    Some(ControlRequestHeader {
        protocol_id,
        transaction_id: read_u16(data, 4)?,
        opcode,
        operation,
    })
}

/// Extract diagnostic header fields, not a command acknowledgement or body.
/// ARC captures can be partial; command-specific parsers validate full framing.
pub fn diagnostic_packet_header(data: &[u8]) -> Option<DiagnosticPacketHeader> {
    use crate::bytes::read_u16;

    let protocol_id = read_u16(data, 0)?;
    let length = read_u16(data, 2)?;
    let (family, transaction_id, opcode, result_code) = match protocol_id {
        PROTOCOL_SETTINGS => ("settings", None, conmon_opcode(data)?, None),
        0x0008 => (
            "ddp_lock",
            read_u16(data, 16),
            read_u16(data, 10)?,
            read_u16(data, 6),
        ),
        PROTOCOL_ID | 0x2729 | 0x2801 | PROTOCOL_ARC_2809 | PROTOCOL_ARC_280F | 0x1200 => (
            "arc",
            read_u16(data, 4),
            read_u16(data, 6)?,
            read_u16(data, 8),
        ),
        _ => return None,
    };

    let result = result_code
        .filter(|_| family == "arc")
        .map(arc_result_status);

    Some(DiagnosticPacketHeader {
        family,
        protocol_name: match protocol_id {
            PROTOCOL_SETTINGS => "PROTOCOL_SETTINGS",
            PROTOCOL_ID => "PROTOCOL_ARC",
            PROTOCOL_ARC_2809 => "PROTOCOL_ARC_SETTINGS",
            PROTOCOL_ARC_280F => "PROTOCOL_ARC_280F",
            crate::commands::PROTOCOL_CMC => "PROTOCOL_CMC",
            crate::commands::PROTOCOL_DANTE_FLOW => "PROTOCOL_ARC_2729",
            crate::commands::PROTOCOL_DANTE_FLOW_2801 => "PROTOCOL_ARC_2801",
            0x0008 => "DDP_LOCK",
            _ => return None,
        },
        result_name: result.as_ref().and_then(|status| status.name),
        result_accepted: result.as_ref().map(|status| status.accepted),
        result_label: result.as_ref().and_then(|status| status.label),
        protocol_id,
        length,
        transaction_id,
        opcode,
        opcode_name: arc_operation(protocol_id, opcode),
        result_code,
        response_decoder: response_decoder(protocol_id, opcode, result_code),
    })
}

pub fn validate_conmon_envelope(data: &[u8], expected_opcode: u16) -> Option<()> {
    (conmon_opcode(data)? == expected_opcode).then_some(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetaudioError {
    InvalidChannel,
    InvalidDestination,
    InvalidEncoding,
    InvalidFlowIdentity,
    InvalidFlowProtocol,
    InvalidFlowSlot,
    InvalidGainLevel,
    InvalidNetworkConfiguration(&'static str),
    InvalidLatency,
    InvalidPage,
    InvalidReceiverMapping,
    InvalidSampleRate,
    InvalidSequence,
    InvalidSubscriptionChannel,
    NameInvalidChars,
    NameInvalidHyphen,
    NameTooLong,
    PacketTooLarge,
    SubscriptionCount,
    UnsupportedProtocolOperation,
}

impl std::fmt::Display for NetaudioError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            NetaudioError::InvalidChannel => {
                "channel number must be at least 1 and fit the protocol"
            }
            NetaudioError::InvalidDestination => {
                "external RTP destination is invalid or unsupported"
            }
            NetaudioError::InvalidEncoding => "encoding value must be nonzero",
            NetaudioError::InvalidFlowIdentity => {
                "external flow source and session identity must be nonzero"
            }
            NetaudioError::InvalidFlowProtocol => "unsupported flow protocol for this operation",
            NetaudioError::InvalidFlowSlot => "flow slot must be from 1 through 32",
            NetaudioError::InvalidGainLevel => "gain level must be an integer from 1 through 5",
            NetaudioError::InvalidNetworkConfiguration(reason) => reason,
            NetaudioError::InvalidLatency => {
                "latency must be finite, nonnegative, and fit on the wire"
            }
            NetaudioError::InvalidPage => "page exceeds the protocol channel range",
            NetaudioError::InvalidReceiverMapping => {
                "receiver IDs must be valid, unique, and paired with in-range flow slots"
            }
            NetaudioError::InvalidSampleRate => "sample rate must be nonzero",
            NetaudioError::InvalidSequence => {
                "message_id must be nonzero; omit it to let a client assign one"
            }
            NetaudioError::InvalidSubscriptionChannel => {
                "subscription receiver channel must fit in one byte"
            }
            NetaudioError::NameInvalidChars => "name contains unsupported characters",
            NetaudioError::NameInvalidHyphen => "name cannot begin or end with a hyphen",
            NetaudioError::NameTooLong => "name exceeds 31 characters",
            NetaudioError::PacketTooLarge => "command packet exceeds the protocol length limit",
            NetaudioError::SubscriptionCount => "subscription count must be 1-16",
            NetaudioError::UnsupportedProtocolOperation => {
                "the selected protocol does not support this operation"
            }
        })
    }
}

fn validate_dante_name_with_character_policy(
    name: &str,
    allow_colon: bool,
    allow_underscore: bool,
) -> Result<(), NetaudioError> {
    if name.chars().count() > DANTE_NAME_MAX_LENGTH {
        return Err(NetaudioError::NameTooLong);
    }

    let valid_pattern = !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || character == '-'
                || (allow_colon && character == ':')
                || (allow_underscore && character == '_')
        })
        && !name.starts_with('-')
        && !name.ends_with('-')
        && (!allow_colon || (!name.starts_with(':') && !name.ends_with(':')));

    if valid_pattern {
        return Ok(());
    }

    if name.starts_with('-') || name.ends_with('-') || name.starts_with(':') || name.ends_with(':')
    {
        return Err(NetaudioError::NameInvalidHyphen);
    }

    Err(NetaudioError::NameInvalidChars)
}

pub fn validate_dante_name(name: &str) -> Result<(), NetaudioError> {
    validate_dante_name_with_character_policy(name, false, false)
}

pub fn validate_dante_channel_name(name: &str) -> Result<(), NetaudioError> {
    validate_dante_name_with_character_policy(name, true, true)
}

pub fn validate_dante_channel_reference(name: &str) -> Result<(), NetaudioError> {
    if name.chars().count() > DANTE_NAME_MAX_LENGTH {
        return Err(NetaudioError::NameTooLong);
    }
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_graphic() || character == ' ')
    {
        return Err(NetaudioError::NameInvalidChars);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArcHeader {
    pub message_id: u16,
    pub opcode: u16,
    pub protocol_id: u16,
}

impl ArcHeader {
    pub fn packet(&self, body: &[u8]) -> Result<Vec<u8>, NetaudioError> {
        frame_packet(self.protocol_id, &[self.message_id, self.opcode], body)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConmonHeader {
    pub message_id: u16,
    pub protocol_id: u16,
}

impl ConmonHeader {
    pub fn packet(&self, body: &[u8]) -> Result<Vec<u8>, NetaudioError> {
        frame_packet(self.protocol_id, &[self.message_id], body)
    }
}

fn frame_packet(
    protocol_id: u16,
    header_words: &[u16],
    body: &[u8],
) -> Result<Vec<u8>, NetaudioError> {
    let header_length = 4usize
        .checked_add(
            header_words
                .len()
                .checked_mul(2)
                .ok_or(NetaudioError::PacketTooLarge)?,
        )
        .ok_or(NetaudioError::PacketTooLarge)?;
    let length = header_length
        .checked_add(body.len())
        .ok_or(NetaudioError::PacketTooLarge)?;
    let encoded_length = u16::try_from(length).map_err(|_| NetaudioError::PacketTooLarge)?;
    let mut packet = Vec::with_capacity(length);
    packet.extend_from_slice(&protocol_id.to_be_bytes());
    packet.extend_from_slice(&encoded_length.to_be_bytes());
    for word in header_words {
        packet.extend_from_slice(&word.to_be_bytes());
    }
    packet.extend_from_slice(body);
    Ok(packet)
}

pub fn build_control_packet(
    opcode: u16,
    payload: &[u8],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_control_packet_for_protocol(PROTOCOL_ID, opcode, payload, message_id)
}

pub fn build_control_packet_for_protocol(
    protocol_id: u16,
    opcode: u16,
    payload: &[u8],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    ArcHeader {
        message_id,
        opcode,
        protocol_id,
    }
    .packet(payload)
}

pub fn build_set_device_name(name: &str, message_id: u16) -> Result<Vec<u8>, NetaudioError> {
    validate_dante_name(name)?;

    let name_bytes = name.as_bytes();
    let mut payload = Vec::with_capacity(2 + name_bytes.len() + 1);
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(name_bytes);
    payload.push(0);

    build_control_packet_for_protocol(
        PROTOCOL_ARC_2809,
        OPCODE_DEVICE_NAME_SET,
        &payload,
        message_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_device_name_known_bytes() {
        let packet = build_set_device_name("AVIO", 0).unwrap();
        let expected = [
            0x28, 0x09, 0x00, 0x0F, 0x00, 0x00, 0x10, 0x01, 0x00, 0x00, 0x41, 0x56, 0x49, 0x4F,
            0x00,
        ];
        assert_eq!(packet, expected);
    }

    #[test]
    fn set_device_name_matches_controller_request() {
        let packet = build_set_device_name("avio-bt-11", 0x261B).unwrap();
        let expected = [
            0x28, 0x09, 0x00, 0x15, 0x26, 0x1B, 0x10, 0x01, 0x00, 0x00, 0x61, 0x76, 0x69, 0x6F,
            0x2D, 0x62, 0x74, 0x2D, 0x31, 0x31, 0x00,
        ];
        assert_eq!(packet, expected);
    }

    #[test]
    fn control_packet_rejects_declared_length_overflow() {
        assert_eq!(
            build_control_packet(0x1000, &vec![0; 65_528], 0),
            Err(NetaudioError::PacketTooLarge)
        );

        let maximum = build_control_packet(0x1000, &vec![0; 65_527], 0).unwrap();
        assert_eq!(maximum.len(), u16::MAX as usize);
        assert_eq!(&maximum[2..4], &u16::MAX.to_be_bytes());
    }

    #[test]
    fn validate_accepts_valid_names() {
        for name in ["a", "A1", "Studio-AVIO", "x-1-y", "Z", &"a".repeat(31)] {
            assert_eq!(validate_dante_name(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn validate_channel_name_accepts_controller_labels() {
        for name in [
            "windows-gaming:left",
            "main-mix:right",
            "shelford-channel:0dB",
            "system:capture_13",
        ] {
            assert_eq!(validate_dante_channel_name(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn validate_channel_reference_accepts_firmware_reported_labels() {
        for name in ["Output 01", "Main Mix Left", "windows-gaming:left"] {
            assert_eq!(validate_dante_channel_reference(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn validate_channel_reference_rejects_non_wire_characters() {
        for name in ["", "tx\0a", "tx\na", "über"] {
            assert_eq!(
                validate_dante_channel_reference(name),
                Err(NetaudioError::NameInvalidChars),
                "{name:?}"
            );
        }
    }

    #[test]
    fn validate_rejects_long_names() {
        assert_eq!(
            validate_dante_name(&"a".repeat(32)),
            Err(NetaudioError::NameTooLong)
        );
    }

    #[test]
    fn validate_rejects_hyphen_at_edges() {
        assert_eq!(
            validate_dante_name("-foo"),
            Err(NetaudioError::NameInvalidHyphen)
        );
        assert_eq!(
            validate_dante_name("foo-"),
            Err(NetaudioError::NameInvalidHyphen)
        );
    }

    #[test]
    fn validate_rejects_invalid_characters() {
        for name in ["", "foo_bar", "foo bar", "über", "foo.bar"] {
            assert_eq!(
                validate_dante_name(name),
                Err(NetaudioError::NameInvalidChars),
                "{name}"
            );
        }
    }

    #[test]
    fn validate_device_name_rejects_colon_labels() {
        assert_eq!(
            validate_dante_name("windows-gaming:left"),
            Err(NetaudioError::NameInvalidChars)
        );
    }
}
