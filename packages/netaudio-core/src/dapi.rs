use crate::bytes::hexadecimal;
use serde::{Deserialize, Serialize};

const REQUEST_MARKER: [u8; 4] = [0xB9, 0x1A, 0x37, 0x26];
pub const FRAME_HEADER_BYTES: usize = 12;
const RESPONSE_MARKER: [u8; 4] = [0xB9, 0x1A, 0x37, 0x25];
const NORMAL_MESSAGE: u32 = 2;
const LAST_INITIALIZATION_WRAPPER_ID: u16 = 5;

/// Zero denotes a new session; initialization reserves wrappers through five.
pub fn next_wrapper_id(previous: u16) -> u16 {
    if previous == 0 {
        LAST_INITIALIZATION_WRAPPER_ID + 1
    } else {
        previous.wrapping_add(1).max(1)
    }
}

const SESSION_DESCRIPTION: u32 = 3;
const SESSION_OPEN: u32 = 5;
const AUTHENTICATION: u32 = 1;
const OBSERVED_CONTROLLER_TOKEN_BYTES: usize = 43;
const OBSERVED_API_KEY_BYTES: usize = 36;
const MAX_FRAME_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEVICE_SERVICE_MESSAGE: u16 = 0x200B;
const DEVICE_LOOKUP_COMPONENT: u16 = 0x000D;
const DEVICE_TARGET_SELECTOR_DISTANCE_FROM_SERVICE_NAME: usize = 26;
const ARC_MESSAGE: u16 = 0x2004;
const SETTINGS_REQUEST: u16 = 0x2002;
const SETTINGS_PUBLICATION: u16 = 0x2003;
const RECORD_OFFSET: usize = 36;
const FAMILY_OFFSET: usize = 38;
const FAMILY_HEADER_OFFSET: usize = 40;
const WRAPPER_ID_OFFSET: usize = 44;
const INNER_LENGTH_OFFSET: usize = 48;
const ARC_PACKET_OFFSET: usize = 56;
const SETTINGS_PACKET_OFFSET: usize = 60;
const IDENTIFY_RESPONSE_OPCODE: u16 = 0x0062;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame<'a> {
    pub message_type: u32,
    pub payload: &'a [u8],
    pub server_to_client: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionDescription {
    pub domain_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeviceAnnouncement {
    pub device_id: String,
    pub target_selector: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ServiceAnnouncement {
    pub message_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentifyConfirmation {
    pub device_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ArcResponse {
    pub wrapper_id: u16,
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub packet_hex: String,
    pub alignment_bytes_hex: String,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ArcCorrelationRequest {
    pub request_packet: Vec<u8>,
    pub wrapper_id: u16,
    pub response_frame: Vec<u8>,
}

pub fn correlate_arc_response(
    request: ArcCorrelationRequest,
) -> Result<Option<ArcResponse>, &'static str> {
    let expected = parse_arc_request_header(&request.request_packet)
        .ok_or("invalid or unsupported ARC request packet")?;

    if request.wrapper_id == 0 {
        return Err("managed ARC wrapper identifier must be nonzero");
    }

    let Some(response) = parse_arc_response(&request.response_frame) else {
        return Ok(None);
    };

    if response.wrapper_id != request.wrapper_id {
        return Ok(None);
    }

    if Some(response.transaction_id) != expected.transaction_id
        || response.opcode != expected.opcode
        || response.protocol_id != expected.protocol_id
    {
        return Err("DDM returned a mismatched managed ARC response");
    }

    Ok(Some(response))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SettingsAcknowledgement {
    pub wrapper_id: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", schemars(extend("required" = ["device_id", "wrapper_id", "response_opcode", "acknowledged", "packet_hex"])))]
pub struct SettingsExchange {
    device_id: String,
    wrapper_id: u16,
    #[serde(deserialize_with = "Option::deserialize")]
    response_opcode: Option<u16>,
    acknowledged: bool,
    #[serde(deserialize_with = "Option::deserialize")]
    pub packet_hex: Option<String>,
}

impl SettingsExchange {
    pub fn new(
        device_id: &str,
        wrapper_id: u16,
        response_opcode: Option<u16>,
    ) -> Result<Self, &'static str> {
        let device_id = crate::device_identity::managed_device_id(device_id);

        if wrapper_id == 0 || device_id.is_none() {
            return Err(
                "managed settings requires a nonzero wrapper and a 16-digit device identity",
            );
        }

        Ok(Self {
            device_id: device_id.expect("device identity was validated"),
            wrapper_id,
            response_opcode,
            acknowledged: false,
            packet_hex: None,
        })
    }

    pub fn complete(&self) -> bool {
        self.acknowledged && (self.response_opcode.is_none() || self.packet_hex.is_some())
    }

    pub fn accept(&mut self, frame: &[u8]) {
        if self.complete() {
            return;
        }

        if parse_settings_acknowledgement(frame)
            .is_some_and(|ack| ack.wrapper_id == self.wrapper_id)
        {
            self.acknowledged = true;
        }

        if let Some(publication) = parse_settings_publication(frame) {
            if publication.device_id == self.device_id
                && Some(publication.opcode) == self.response_opcode
            {
                self.packet_hex = Some(publication.packet_hex);
            }
        }
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SettingsExchangeRequest {
    pub device_id: String,
    pub wrapper_id: u16,
    pub response_opcode: Option<u16>,
    pub frame: Vec<u8>,
    pub state: Option<SettingsExchange>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SettingsExchangeResult {
    pub state: SettingsExchange,
    pub complete: bool,
}

pub fn advance_settings_exchange(
    request: SettingsExchangeRequest,
) -> Result<SettingsExchangeResult, &'static str> {
    let initial = SettingsExchange::new(
        &request.device_id,
        request.wrapper_id,
        request.response_opcode,
    )?;
    let mut state = request.state.unwrap_or_else(|| initial.clone());

    if state.device_id != initial.device_id
        || state.wrapper_id != initial.wrapper_id
        || state.response_opcode != initial.response_opcode
    {
        return Err("managed settings evidence belongs to a different exchange");
    }

    state.accept(&request.frame);
    Ok(SettingsExchangeResult {
        complete: state.complete(),
        state,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettingsRequest {
    pub target_selector: u16,
    pub wrapper_id: u16,
    pub opcode: u16,
    pub packet_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettingsPublication {
    pub wrapper_id: u16,
    pub target_name: String,
    pub device_id: String,
    pub message_id: u16,
    pub opcode: u16,
    pub packet_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SignalPresencePublication {
    pub device_id: String,
    pub records: Vec<crate::signal_presence::SignalPresenceRecord>,
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn aligned_to_four(length: usize) -> Option<usize> {
    length.checked_add(3).map(|value| value & !3)
}

fn device_request_header(target_selector: u16, routing_channel: u16) -> Vec<u8> {
    let mut payload = Vec::with_capacity(24);
    payload.extend_from_slice(&[
        0x00, 0x18, 0x00, 0x01, 0x10, 0x00, 0x02, 0x08, 0x08, 0x40, 0x01, 0x00,
    ]);
    payload.extend_from_slice(&target_selector.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x00, 0x08, 0x41, 0x00, 0x00, 0x00, 0x00]);
    payload.extend_from_slice(&routing_channel.to_be_bytes());
    payload
}

fn valid_arc_packet(packet: &[u8]) -> Option<crate::protocol::ResponseEnvelope<'_>> {
    let envelope = crate::protocol::response_envelope(packet)?;
    (envelope.protocol_id == crate::protocol::PROTOCOL_ARC_2809).then_some(envelope)
}

fn settings_opcode(packet: &[u8]) -> Option<u16> {
    if packet.len() < 28
        || read_u16(packet, 0)? != 0xFFFF
        || usize::from(read_u16(packet, 2)?) != packet.len()
        || packet.get(16..24)? != b"Audinate"
        || packet.get(24).copied()? != 0x07
    {
        return None;
    }
    read_u16(packet, 26)
}

fn normal_record(bytes: &[u8], family: u16, family_header: [u8; 4]) -> Option<Frame<'_>> {
    let frame = parse_frame(bytes)?;
    if frame.message_type != NORMAL_MESSAGE
        || bytes.len() < WRAPPER_ID_OFFSET + 4
        || usize::from(read_u16(bytes, RECORD_OFFSET)?) != bytes.len() - RECORD_OFFSET
        || read_u16(bytes, FAMILY_OFFSET)? != family
        || bytes.get(FAMILY_HEADER_OFFSET..FAMILY_HEADER_OFFSET + 4)? != family_header
    {
        return None;
    }
    Some(frame)
}

fn append_frame(message_type: u32, payload: &[u8]) -> Option<Vec<u8>> {
    if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
        return None;
    }
    let payload_length = u32::try_from(payload.len()).ok()?;
    let mut frame = Vec::with_capacity(12 + payload.len());
    frame.extend_from_slice(&REQUEST_MARKER);
    frame.extend_from_slice(&message_type.to_be_bytes());
    frame.extend_from_slice(&payload_length.to_be_bytes());
    frame.extend_from_slice(payload);
    Some(frame)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FrameHeader {
    pub message_type: u32,
    pub payload_length: usize,
    pub server_to_client: bool,
}

pub fn parse_frame_header(bytes: &[u8]) -> Option<FrameHeader> {
    if bytes.len() != 12 {
        return None;
    }
    let server_to_client = match bytes.get(..4)? {
        marker if marker == RESPONSE_MARKER => true,
        marker if marker == REQUEST_MARKER => false,
        _ => return None,
    };
    let payload_length = usize::try_from(read_u32(bytes, 8)?).ok()?;
    if payload_length > MAX_FRAME_PAYLOAD_BYTES {
        return None;
    }
    Some(FrameHeader {
        message_type: read_u32(bytes, 4)?,
        payload_length,
        server_to_client,
    })
}

pub fn parse_frame(bytes: &[u8]) -> Option<Frame<'_>> {
    let header = parse_frame_header(bytes.get(..12)?)?;
    if bytes.len() != 12 + header.payload_length {
        return None;
    }
    Some(Frame {
        message_type: header.message_type,
        payload: &bytes[12..],
        server_to_client: header.server_to_client,
    })
}

pub fn build_session_open() -> Vec<u8> {
    append_frame(SESSION_OPEN, &0u32.to_be_bytes()).expect("four-byte payload fits")
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ManagedCredential {
    ApiKey(String),
    ControllerToken(String),
}

impl ManagedCredential {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::ApiKey(value) => {
                let bytes = value.as_bytes();
                if bytes.len() != OBSERVED_API_KEY_BYTES
                    || !bytes.iter().enumerate().all(|(index, byte)| {
                        if matches!(index, 8 | 13 | 18 | 23) {
                            *byte == b'-'
                        } else {
                            byte.is_ascii_hexdigit()
                        }
                    })
                {
                    return Err("DDM API key must use the observed UUID format");
                }
            }
            Self::ControllerToken(value) => {
                if value.len() != OBSERVED_CONTROLLER_TOKEN_BYTES || !value.is_ascii() {
                    return Err("DDM returned an unsupported Controller authentication token");
                }
            }
        }

        Ok(())
    }
}

pub fn build_authentication(credential: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(credential).ok()?.to_owned();
    let value = match credential.len() {
        OBSERVED_API_KEY_BYTES => ManagedCredential::ApiKey(text),
        OBSERVED_CONTROLLER_TOKEN_BYTES => ManagedCredential::ControllerToken(text),
        _ => return None,
    };
    value.validate().ok()?;
    append_frame(AUTHENTICATION, credential)
}

pub fn parse_session_description(bytes: &[u8]) -> Option<SessionDescription> {
    let frame = parse_frame(bytes)?;
    if !frame.server_to_client
        || frame.message_type != SESSION_DESCRIPTION
        || bytes.len() < 82
        || read_u16(bytes, 12)? != 0x0018
        || usize::from(read_u16(bytes, 36)?) != bytes.len() - 36
        || read_u16(bytes, 42)? != 0x000C
        || read_u16(bytes, 44)? != 0x0003
        || read_u16(bytes, 46)? != 0x0014
        || read_u16(bytes, 64)? != 0x07FF
    {
        return None;
    }
    Some(SessionDescription {
        domain_id: hexadecimal(&bytes[66..82]),
    })
}

fn build_domain_subscription(domain_id: &[u8], subscription_id: u16) -> Option<Vec<u8>> {
    if domain_id.len() != 16 || !(2..=5).contains(&subscription_id) {
        return None;
    }
    let mut payload = Vec::with_capacity(68);
    payload.extend_from_slice(&[
        0x00, 0x18, 0x00, 0x01, 0x10, 0x00, 0x02, 0x08, 0x08, 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0x00,
        0x00, 0x08, 0x41, 0x00, 0x00, 0x00, 0x00,
    ]);
    payload.extend_from_slice(&subscription_id.to_be_bytes());
    payload.extend_from_slice(&[
        0x00, 0x2C, 0x20, 0x09, 0x00, 0x04, 0x00, 0x20, 0x00, 0x09, 0x00, 0x00,
    ]);
    payload.extend_from_slice(domain_id);
    payload.extend_from_slice(&lookup_identifier(subscription_id).to_be_bytes());
    payload.extend_from_slice(&[0; 12]);
    append_frame(NORMAL_MESSAGE, &payload)
}

fn build_device_inventory_subscription(domain_id: &[u8]) -> Option<Vec<u8>> {
    if domain_id.len() != 16 {
        return None;
    }
    let mut payload = Vec::with_capacity(68);
    payload.extend_from_slice(&[
        0x00, 0x18, 0x00, 0x01, 0x10, 0x00, 0x02, 0x08, 0x08, 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0x00,
        0x00, 0x08, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x2C, 0x20, 0x09, 0x00, 0x04,
        0x00, 0x20, 0x00, 0x09, 0x00, 0x00,
    ]);
    payload.extend_from_slice(domain_id);
    payload.extend_from_slice(&lookup_identifier(DEVICE_LOOKUP_COMPONENT).to_be_bytes());
    payload.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
    ]);
    append_frame(NORMAL_MESSAGE, &payload)
}

fn lookup_identifier(component: u16) -> u32 {
    u32::from(component) << 16 | 4
}

pub fn domain_lookups() -> impl Iterator<Item = (u16, u32)> {
    (2..=LAST_INITIALIZATION_WRAPPER_ID)
        .chain([DEVICE_LOOKUP_COMPONENT])
        .map(|component| (component, lookup_identifier(component)))
}

pub fn build_domain_initialization(
    domain_id: &[u8],
    first_message_id: u16,
    notification_port: u16,
    local_ipv4: [u8; 4],
) -> Option<Vec<u8>> {
    let inventory =
        build_inventory_initialization(domain_id, first_message_id, notification_port, local_ipv4)?;
    let mut frames = Vec::new();

    for subscription in 2..=LAST_INITIALIZATION_WRAPPER_ID {
        frames.extend(build_domain_subscription(domain_id, subscription)?);
    }

    frames.extend(inventory);
    frames.extend(build_device_inventory_subscription(domain_id)?);
    Some(frames)
}

fn build_inventory_initialization(
    domain_id: &[u8],
    first_message_id: u16,
    notification_port: u16,
    local_ipv4: [u8; 4],
) -> Option<Vec<u8>> {
    if domain_id.len() != 16 || first_message_id == 0 || notification_port == 0 {
        return None;
    }

    let message_id = |offset: u16| {
        (((u32::from(first_message_id) - 1 + u32::from(offset)) % u32::from(u16::MAX)) + 1) as u16
    };

    let mut first_payload = Vec::with_capacity(52);
    first_payload.extend_from_slice(&[
        0x00, 0x18, 0x00, 0x01, 0x10, 0x00, 0x02, 0x08, 0x08, 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0x00,
        0x00, 0x08, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x1C, 0x20, 0x12, 0x00, 0x04,
        0x00, 0x10,
    ]);
    first_payload.extend_from_slice(&first_message_id.to_be_bytes());
    first_payload.extend_from_slice(&[0x00, 0x00]);
    first_payload.extend_from_slice(domain_id);

    let service_request = |message_id: u16, selector: u16, endpoint_port: u16| {
        let mut payload = Vec::with_capacity(68);
        payload.extend_from_slice(&[
            0x00, 0x18, 0x00, 0x01, 0x10, 0x00, 0x02, 0x08, 0x08, 0x40, 0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x08, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x2C, 0x20, 0x10,
            0x00, 0x04, 0x00, 0x20,
        ]);
        payload.extend_from_slice(&message_id.to_be_bytes());
        payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        payload.extend_from_slice(&selector.to_be_bytes());
        payload.extend_from_slice(&[0x00, 0x01]);
        payload.extend_from_slice(&endpoint_port.to_be_bytes());
        payload.extend_from_slice(domain_id);
        payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
        payload
    };

    let mut frames = append_frame(NORMAL_MESSAGE, &first_payload)?;
    for (offset, selector, endpoint_port) in
        [(1, 2, 0), (2, 3, 0), (3, 1, notification_port), (4, 8, 0)]
    {
        frames.extend_from_slice(&append_frame(
            NORMAL_MESSAGE,
            &service_request(message_id(offset), selector, endpoint_port),
        )?);
    }

    let interface_request = |request_id: u16, selector: u16| {
        let mut payload = Vec::with_capacity(72);
        payload.extend_from_slice(&[
            0x00, 0x18, 0x00, 0x01, 0x10, 0x00, 0x02, 0x08, 0x08, 0x40, 0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x08, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x30, 0x20, 0x11,
            0x00, 0x04, 0x00, 0x24,
        ]);
        payload.extend_from_slice(&request_id.to_be_bytes());
        payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x60, 0x00, 0x01]);
        payload.extend_from_slice(&selector.to_be_bytes());
        payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        payload.extend_from_slice(domain_id);
        payload.extend_from_slice(&local_ipv4);
        payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        payload
    };
    for (offset, selector) in [(5, 2), (6, 8)] {
        frames.extend_from_slice(&append_frame(
            NORMAL_MESSAGE,
            &interface_request(message_id(offset), selector),
        )?);
    }
    Some(frames)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub fn parse_device_announcement(bytes: &[u8]) -> Option<DeviceAnnouncement> {
    parse_service_announcement(bytes)?;
    let frame = parse_frame(bytes)?;
    let service = crate::protocol::SERVICE_CMC.strip_suffix(".local.")?;
    let service_name_offset = find_bytes(bytes, service.as_bytes())?;
    if !frame.server_to_client
        || frame.message_type != NORMAL_MESSAGE
        || bytes.len() < 72
        || service_name_offset == 0
    {
        return None;
    }
    let marker = find_bytes(bytes, b"id=")? + 3;
    let encoded_id = bytes.get(marker..marker + 16)?;
    if !encoded_id.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    // The selector is part of the service record immediately preceding the
    // service name.  It is not one of the routing/binding words in the DAPI
    // wrapper.  Zero is a valid selector observed for a managed device.
    let target_selector_offset =
        service_name_offset.checked_sub(DEVICE_TARGET_SELECTOR_DISTANCE_FROM_SERVICE_NAME)?;
    let target_selector = read_u16(bytes, target_selector_offset)?;
    Some(DeviceAnnouncement {
        device_id: String::from_utf8(encoded_id.to_ascii_lowercase()).ok()?,
        target_selector,
    })
}

pub fn parse_service_announcement(bytes: &[u8]) -> Option<ServiceAnnouncement> {
    let frame = parse_frame(bytes)?;
    if !frame.server_to_client
        || frame.message_type != NORMAL_MESSAGE
        || bytes.len() < 48
        || bytes.get(12..16)? != [0x00, 0x18, 0x00, 0x01]
        || bytes.get(16..20)? != [0x10, 0x00, 0x02, 0x08]
        || read_u16(bytes, 36)? as usize != bytes.len() - 36
        || read_u16(bytes, 38)? != DEVICE_SERVICE_MESSAGE
        || read_u16(bytes, 40)? != 4
        || read_u16(bytes, 42)? != 0x0018
    {
        return None;
    }
    let message_id = read_u16(bytes, 44)?;
    if message_id == 0 {
        return None;
    }
    Some(ServiceAnnouncement { message_id })
}

pub fn build_service_acknowledgement(announcement_frame: &[u8]) -> Option<Vec<u8>> {
    let announcement = parse_service_announcement(announcement_frame)?;
    let mut payload = Vec::with_capacity(36);
    payload.extend_from_slice(&announcement_frame[12..16]);
    payload.extend_from_slice(&[0x10, 0x00, 0x03, 0x08]);
    payload.extend_from_slice(&announcement_frame[28..30]);
    payload.extend_from_slice(&announcement_frame[22..28]);
    payload.extend_from_slice(&announcement_frame[20..22]);
    payload.extend_from_slice(&announcement_frame[30..36]);
    payload.extend_from_slice(&[0x00, 0x0C, 0x20, 0x0B, 0x00, 0x04, 0x00, 0x00]);
    payload.extend_from_slice(&announcement.message_id.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x00]);
    append_frame(NORMAL_MESSAGE, &payload)
}

pub fn parse_arc_request_header(bytes: &[u8]) -> Option<crate::protocol::DiagnosticPacketHeader> {
    let envelope = valid_arc_packet(bytes)?;

    if envelope.result_code != 0 {
        return None;
    }

    crate::protocol::diagnostic_packet_header(bytes)
}

pub fn build_arc_request(
    target_selector: u16,
    wrapper_id: u16,
    arc_packet: &[u8],
) -> Option<Vec<u8>> {
    parse_arc_request_header(arc_packet)?;

    if wrapper_id == 0 {
        return None;
    }
    let aligned_packet_length = aligned_to_four(arc_packet.len())?;
    let record_length = 20usize.checked_add(aligned_packet_length)?;
    let encoded_record_length = u16::try_from(record_length).ok()?;
    let encoded_packet_length = u16::try_from(arc_packet.len()).ok()?;

    let mut payload = device_request_header(target_selector, 9);
    payload.extend_from_slice(&encoded_record_length.to_be_bytes());
    payload.extend_from_slice(&ARC_MESSAGE.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x04, 0x00, 0x08]);
    payload.extend_from_slice(&wrapper_id.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x00]);
    payload.extend_from_slice(&encoded_packet_length.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x14, 0x00, 0x00, 0x00, 0x00]);
    payload.extend_from_slice(arc_packet);
    payload.resize(24 + record_length, 0);
    append_frame(NORMAL_MESSAGE, &payload)
}

pub fn parse_arc_response(bytes: &[u8]) -> Option<ArcResponse> {
    let frame = normal_record(bytes, ARC_MESSAGE, [0x00, 0x04, 0x00, 0x08])?;
    if !frame.server_to_client
        || read_u16(bytes, WRAPPER_ID_OFFSET)? == 0
        || read_u16(bytes, WRAPPER_ID_OFFSET + 2)? != 0
        || bytes.get(INNER_LENGTH_OFFSET + 2..ARC_PACKET_OFFSET)?
            != [0x00, 0x14, 0x00, 0x00, 0x00, 0x00]
    {
        return None;
    }
    let packet_length = usize::from(read_u16(bytes, INNER_LENGTH_OFFSET)?);
    let packet_end = ARC_PACKET_OFFSET.checked_add(packet_length)?;
    let aligned_end = ARC_PACKET_OFFSET.checked_add(aligned_to_four(packet_length)?)?;
    if aligned_end != bytes.len() || packet_end > aligned_end {
        return None;
    }
    let packet = bytes.get(ARC_PACKET_OFFSET..packet_end)?;
    let envelope = valid_arc_packet(packet)?;
    Some(ArcResponse {
        wrapper_id: read_u16(bytes, WRAPPER_ID_OFFSET)?,
        protocol_id: envelope.protocol_id,
        transaction_id: envelope.transaction_id,
        opcode: envelope.opcode,
        result_code: envelope.result_code,
        packet_hex: hexadecimal(packet),
        alignment_bytes_hex: hexadecimal(bytes.get(packet_end..aligned_end)?),
    })
}

pub fn build_settings_request(
    target_selector: u16,
    wrapper_id: u16,
    settings_packet: &[u8],
) -> Option<Vec<u8>> {
    settings_opcode(settings_packet)?;
    if wrapper_id == 0 {
        return None;
    }
    let aligned_packet_length = aligned_to_four(settings_packet.len())?;
    let record_length = 24usize.checked_add(aligned_packet_length)?;
    let encoded_record_length = u16::try_from(record_length).ok()?;
    let encoded_packet_length = u16::try_from(settings_packet.len()).ok()?;

    let mut payload = device_request_header(target_selector, 10);
    payload.extend_from_slice(&encoded_record_length.to_be_bytes());
    payload.extend_from_slice(&SETTINGS_REQUEST.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x04, 0x00, 0x0C]);
    payload.extend_from_slice(&wrapper_id.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x00]);
    payload.extend_from_slice(&encoded_packet_length.to_be_bytes());
    payload.extend_from_slice(&[0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    payload.extend_from_slice(settings_packet);
    payload.resize(24 + record_length, 0);
    append_frame(NORMAL_MESSAGE, &payload)
}

pub fn parse_settings_acknowledgement(bytes: &[u8]) -> Option<SettingsAcknowledgement> {
    let frame = normal_record(bytes, SETTINGS_REQUEST, [0x00, 0x04, 0x00, 0x00])?;
    if !frame.server_to_client
        || bytes.len() != 48
        || read_u16(bytes, RECORD_OFFSET)? != 12
        || read_u16(bytes, WRAPPER_ID_OFFSET)? == 0
        || read_u16(bytes, WRAPPER_ID_OFFSET + 2)? != 0
    {
        return None;
    }
    Some(SettingsAcknowledgement {
        wrapper_id: read_u16(bytes, WRAPPER_ID_OFFSET)?,
    })
}

pub fn parse_settings_request(bytes: &[u8]) -> Option<SettingsRequest> {
    let frame = normal_record(bytes, SETTINGS_REQUEST, [0x00, 0x04, 0x00, 0x0C])?;
    if frame.server_to_client {
        return None;
    }
    let target_selector = read_u16(bytes, 24)?;
    let wrapper_id = read_u16(bytes, WRAPPER_ID_OFFSET)?;
    let packet_length = usize::from(read_u16(bytes, INNER_LENGTH_OFFSET)?);
    let packet =
        bytes.get(SETTINGS_PACKET_OFFSET..SETTINGS_PACKET_OFFSET.checked_add(packet_length)?)?;
    let opcode = settings_opcode(packet)?;
    // Restrict observation to the established envelope, including alignment.
    // Authentication and unsupported record variants must never be classified
    // as device settings requests.
    if build_settings_request(target_selector, wrapper_id, packet)?.as_slice() != bytes {
        return None;
    }
    Some(SettingsRequest {
        target_selector,
        wrapper_id,
        opcode,
        packet_hex: hexadecimal(packet),
    })
}

fn publication_payload(bytes: &[u8], service: u16) -> Option<(&str, &[u8])> {
    let frame = normal_record(bytes, SETTINGS_PUBLICATION, [0x00, 0x04, 0x00, 0x0C])?;
    if !frame.server_to_client
        || read_u16(bytes, WRAPPER_ID_OFFSET)? != 0
        || read_u16(bytes, WRAPPER_ID_OFFSET + 2)? != 0
        || read_u16(bytes, INNER_LENGTH_OFFSET + 4)? != service
        || bytes.get(INNER_LENGTH_OFFSET + 6..SETTINGS_PACKET_OFFSET)?
            != [0x00, 0x18, 0x00, 0x00, 0x00, 0x00]
    {
        return None;
    }
    let packet_length = usize::from(read_u16(bytes, INNER_LENGTH_OFFSET)?);
    let packet_offset = bytes.len().checked_sub(packet_length)?;
    if packet_offset < SETTINGS_PACKET_OFFSET
        || usize::from(read_u16(bytes, INNER_LENGTH_OFFSET + 2)?)
            != packet_offset.checked_sub(RECORD_OFFSET)?
    {
        return None;
    }
    let target_name_field = bytes.get(SETTINGS_PACKET_OFFSET..packet_offset)?;
    let target_name_length = target_name_field.iter().position(|byte| *byte == 0)?;
    let target_name = std::str::from_utf8(&target_name_field[..target_name_length]).ok()?;
    Some((target_name, bytes.get(packet_offset..)?))
}

pub fn parse_settings_publication(bytes: &[u8]) -> Option<SettingsPublication> {
    let (target_name, packet) = publication_payload(bytes, 2)?;
    let opcode = settings_opcode(packet)?;
    Some(SettingsPublication {
        wrapper_id: 0,
        target_name: target_name.to_owned(),
        device_id: hexadecimal(packet.get(8..16)?),
        message_id: read_u16(packet, 4)?,
        opcode,
        packet_hex: hexadecimal(packet),
    })
}

pub fn parse_signal_presence_publication(bytes: &[u8]) -> Option<SignalPresencePublication> {
    let (_, packet) = publication_payload(bytes, 8)?;
    let identity = crate::heartbeat::parse_heartbeat_device_extended_unique_identifier(packet)?;
    let records = crate::signal_presence::parse_signal_presence_packet(packet)?;
    if records.is_empty() {
        return None;
    }
    Some(SignalPresencePublication {
        device_id: identity,
        records,
    })
}

pub fn build_identify(
    target_selector: u16,
    wrapper_id: u16,
    message_id: u16,
    host_mac: [u8; 6],
) -> Option<Vec<u8>> {
    if wrapper_id == 0 || message_id == 0 {
        return None;
    }
    let mut settings_packet = Vec::with_capacity(32);
    settings_packet.extend_from_slice(&[0xFF, 0xFF, 0x00, 0x20]);
    settings_packet.extend_from_slice(&message_id.to_be_bytes());
    settings_packet.extend_from_slice(&[0x00, 0xA8]);
    settings_packet.extend_from_slice(&host_mac);
    settings_packet.extend_from_slice(&[0x00, 0x00]);
    settings_packet.extend_from_slice(b"Audinate");
    settings_packet.extend_from_slice(&[0x07, 0x3A, 0x00, 0x63, 0x00, 0x00, 0x00, 0x64]);
    build_settings_request(target_selector, wrapper_id, &settings_packet)
}

pub fn parse_identify_confirmation(bytes: &[u8]) -> Option<IdentifyConfirmation> {
    let publication = parse_settings_publication(bytes)?;
    (publication.opcode == IDENTIFY_RESPONSE_OPCODE).then_some(IdentifyConfirmation {
        device_id: publication.device_id,
    })
}
