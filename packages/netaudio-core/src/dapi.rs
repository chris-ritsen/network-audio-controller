use serde::Serialize;
use std::fmt::Write;

const REQUEST_MARKER: [u8; 4] = [0xB9, 0x1A, 0x37, 0x26];
const RESPONSE_MARKER: [u8; 4] = [0xB9, 0x1A, 0x37, 0x25];
const NORMAL_MESSAGE: u32 = 2;
const SESSION_DESCRIPTION: u32 = 3;
const SESSION_OPEN: u32 = 5;
const AUTHENTICATION: u32 = 1;
const OBSERVED_CONTROLLER_TOKEN_BYTES: usize = 43;
const OBSERVED_API_KEY_BYTES: usize = 36;
const MAX_FRAME_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEVICE_SERVICE_MESSAGE: u16 = 0x200B;
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
pub struct ArcResponse {
    pub wrapper_id: u16,
    pub protocol_id: u16,
    pub transaction_id: u16,
    pub opcode: u16,
    pub result_code: u16,
    pub packet_hex: String,
    pub alignment_bytes_hex: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SettingsAcknowledgement {
    pub wrapper_id: u16,
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

fn hexadecimal(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            write!(text, "{byte:02x}").expect("writing to a String cannot fail");
            text
        })
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

pub fn parse_frame(bytes: &[u8]) -> Option<Frame<'_>> {
    if bytes.len() < 12 {
        return None;
    }
    let server_to_client = match bytes.get(..4)? {
        marker if marker == RESPONSE_MARKER => true,
        marker if marker == REQUEST_MARKER => false,
        _ => return None,
    };
    let payload_length = usize::try_from(read_u32(bytes, 8)?).ok()?;
    if payload_length > MAX_FRAME_PAYLOAD_BYTES || bytes.len() != 12 + payload_length {
        return None;
    }
    Some(Frame {
        message_type: read_u32(bytes, 4)?,
        payload: &bytes[12..],
        server_to_client,
    })
}

pub fn build_session_open() -> Vec<u8> {
    append_frame(SESSION_OPEN, &0u32.to_be_bytes()).expect("four-byte payload fits")
}

pub fn build_authentication(credential: &[u8]) -> Option<Vec<u8>> {
    if !matches!(
        credential.len(),
        OBSERVED_CONTROLLER_TOKEN_BYTES | OBSERVED_API_KEY_BYTES
    ) {
        return None;
    }
    append_frame(AUTHENTICATION, credential)
}

pub fn parse_session_description(bytes: &[u8]) -> Option<SessionDescription> {
    let frame = parse_frame(bytes)?;
    if !frame.server_to_client
        || frame.message_type != SESSION_DESCRIPTION
        || bytes.len() < 82
        || read_u16(bytes, 12)? != 0x0018
        || read_u16(bytes, 36)? != 0x007C
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

pub fn build_domain_subscription(domain_id: &[u8], subscription_id: u16) -> Option<Vec<u8>> {
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
    payload.extend_from_slice(&subscription_id.to_be_bytes());
    payload.extend_from_slice(&[
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ]);
    append_frame(NORMAL_MESSAGE, &payload)
}

pub fn build_device_inventory_subscription(domain_id: &[u8]) -> Option<Vec<u8>> {
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
    payload.extend_from_slice(&[
        0x00, 0x0D, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02,
    ]);
    append_frame(NORMAL_MESSAGE, &payload)
}

pub fn build_inventory_initialization(
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
    let service_name_offset = find_bytes(bytes, b"_netaudio-cmc._udp")?;
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

pub fn build_arc_request(
    target_selector: u16,
    wrapper_id: u16,
    arc_packet: &[u8],
) -> Option<Vec<u8>> {
    let envelope = valid_arc_packet(arc_packet)?;
    if wrapper_id == 0 || envelope.result_code != 0 {
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

pub fn parse_settings_publication(bytes: &[u8]) -> Option<SettingsPublication> {
    let frame = normal_record(bytes, SETTINGS_PUBLICATION, [0x00, 0x04, 0x00, 0x0C])?;
    if !frame.server_to_client
        || read_u16(bytes, WRAPPER_ID_OFFSET)? != 0
        || read_u16(bytes, WRAPPER_ID_OFFSET + 2)? != 0
        || bytes.get(INNER_LENGTH_OFFSET + 2..SETTINGS_PACKET_OFFSET)?
            != [0x00, 0x28, 0x00, 0x02, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00]
    {
        return None;
    }
    let packet_length = usize::from(read_u16(bytes, INNER_LENGTH_OFFSET)?);
    let packet_offset = bytes.len().checked_sub(packet_length)?;
    if packet_offset < SETTINGS_PACKET_OFFSET {
        return None;
    }
    let target_name_field = bytes.get(SETTINGS_PACKET_OFFSET..packet_offset)?;
    let target_name_length = target_name_field.iter().position(|byte| *byte == 0)?;
    let target_name = std::str::from_utf8(&target_name_field[..target_name_length])
        .ok()?
        .to_owned();
    let packet = bytes.get(packet_offset..)?;
    let opcode = settings_opcode(packet)?;
    Some(SettingsPublication {
        wrapper_id: 0,
        target_name,
        device_id: hexadecimal(packet.get(8..16)?),
        message_id: read_u16(packet, 4)?,
        opcode,
        packet_hex: hexadecimal(packet),
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

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal packet fixtures derived from protocol-research run
    // ddm-session-auth-20260902-01, TRANSCRIPT.json SHA-256
    // 1ccb72de528670b917df74fe9f67c96b90b6a8b19e179b1e2bb00bd1178bda4a.
    // Decisive client messages: 0, 1 (token masked), 2, 13, 14, and 109; decisive
    // server messages: 0, 17, and 176. The authentication token below is
    // synthetic and the saved transcript contains no credential values.

    fn decode(hex: &str) -> Vec<u8> {
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let text = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(text, 16).unwrap()
            })
            .collect()
    }

    #[test]
    fn session_open_and_authentication_match_observed_framing() {
        assert_eq!(
            build_session_open(),
            decode("b91a3726000000050000000400000000")
        );
        let token = b"0123456789012345678901234567890123456789012";
        let mut expected = decode("b91a3726000000010000002b");
        expected.extend_from_slice(token);
        assert_eq!(build_authentication(token), Some(expected));
        let api_key = b"00000000-0000-4000-8000-000000000000";
        let mut expected_api_key = decode("b91a37260000000100000024");
        expected_api_key.extend_from_slice(api_key);
        assert_eq!(build_authentication(api_key), Some(expected_api_key));
        assert_eq!(build_authentication(b"short"), None);
    }

    #[test]
    fn session_description_exposes_only_the_domain_identifier() {
        let domain_id = decode("ce1d98eb183c45c9967b4ba94fcf9921");
        let mut frame = vec![0u8; 160];
        frame[..4].copy_from_slice(&RESPONSE_MARKER);
        frame[4..8].copy_from_slice(&SESSION_DESCRIPTION.to_be_bytes());
        frame[8..12].copy_from_slice(&148u32.to_be_bytes());
        frame[12..14].copy_from_slice(&0x0018u16.to_be_bytes());
        frame[36..38].copy_from_slice(&0x007Cu16.to_be_bytes());
        frame[42..44].copy_from_slice(&0x000Cu16.to_be_bytes());
        frame[44..46].copy_from_slice(&0x0003u16.to_be_bytes());
        frame[46..48].copy_from_slice(&0x0014u16.to_be_bytes());
        frame[64..66].copy_from_slice(&0x07FFu16.to_be_bytes());
        frame[66..82].copy_from_slice(&domain_id);

        assert_eq!(
            parse_session_description(&frame),
            Some(SessionDescription {
                domain_id: "ce1d98eb183c45c9967b4ba94fcf9921".to_owned()
            })
        );
        frame[8..12].copy_from_slice(&147u32.to_be_bytes());
        assert_eq!(parse_session_description(&frame), None);
    }

    #[test]
    fn device_announcements_map_cmc_identifiers_to_distinct_target_selectors() {
        // Authentic sanitized server messages 42 and 44 from the retained
        // transcript named above.  They differ in both identity and target
        // selector; the wrapper word at byte 68 is 2 in both messages.
        let output = decode(concat!(
            "b91a3725000000020000013400180001100002080841ffffffff00010840010600050009011c200b00040018001a0000",
            "ce1d98eb183c45c9967b4ba94fcf99210009000800020024002c0003003c00786176696f2d6f75747075742d33006469",
            "001f0050006000680070000100000000000000006176696f2d6f75747075742d33000000417564696e61746544414f32",
            "000000000100000200000010000100010080f01b0007008c00a0007c000000005f6e6574617564696f2d636d632e5f75",
            "647000001369643d303031646331666666653530376238640970726f636573733d300f636d63705f766572733d312e32",
            "2e300e636d63705f6d696e3d312e302e30117365727665725f766572733d342e312e30136368616e6e656c733d307836",
            "303030303034640b6d663d417564696e6174650a6d6f64656c3d44414f320000",
        ));
        let input = decode(concat!(
            "b91a3725000000020000013400180001100002080841ffffffff00010840010600050009011c200b00040018001b0000",
            "ce1d98eb183c45c9967b4ba94fcf99210009000900020024002c0003003c00786176696f2d696e7075742d3200006469",
            "001f0050006000680070000100000000000000006176696f2d696e7075742d3200000000417564696e61746544414932",
            "000000000100000000000010000100010080f01b0007008c00a0007c000000005f6e6574617564696f2d636d632e5f75",
            "647000001369643d303031646331666666653530363932650970726f636573733d300f636d63705f766572733d312e32",
            "2e300e636d63705f6d696e3d312e302e30117365727665725f766572733d342e312e30136368616e6e656c733d307836",
            "303030303034640b6d663d417564696e6174650a6d6f64656c3d444149320000",
        ));

        assert_eq!(read_u16(&output, 68), Some(2));
        assert_eq!(read_u16(&input, 68), Some(2));
        assert_eq!(
            parse_device_announcement(&output),
            Some(DeviceAnnouncement {
                device_id: "001dc1fffe507b8d".to_owned(),
                target_selector: 2,
            })
        );
        assert_eq!(
            parse_device_announcement(&input),
            Some(DeviceAnnouncement {
                device_id: "001dc1fffe50692e".to_owned(),
                target_selector: 0,
            })
        );
        assert_eq!(
            parse_service_announcement(&output),
            Some(ServiceAnnouncement { message_id: 26 })
        );
        assert_eq!(
            build_service_acknowledgement(&output),
            Some(decode(
                "b91a3726000000020000002400180001100003080840ffffffff00010841010600050009\
                 000c200b00040000001a0000"
            ))
        );
    }

    #[test]
    fn domain_subscription_matches_the_fresh_session_capture() {
        let domain_id = decode("ce1d98eb183c45c9967b4ba94fcf9921");
        let observed = decode(
            "b91a3726000000020000004400180001100002080840ffffffff00000841000000000002\
             002c20090004002000090000ce1d98eb183c45c9967b4ba94fcf992100020004000000000000000000000000",
        );
        assert_eq!(build_domain_subscription(&domain_id, 2), Some(observed));
    }

    #[test]
    fn device_inventory_subscription_matches_the_fresh_session_capture() {
        let domain_id = decode("ce1d98eb183c45c9967b4ba94fcf9921");
        let observed = decode(
            "b91a3726000000020000004400180001100002080840ffffffff0000084100000000000d\
             002c20090004002000090000ce1d98eb183c45c9967b4ba94fcf9921000d0004000000040000000000000002",
        );
        assert_eq!(
            build_device_inventory_subscription(&domain_id),
            Some(observed)
        );
    }

    #[test]
    fn inventory_initialization_matches_the_fresh_session_capture() {
        let domain_id = decode("ce1d98eb183c45c9967b4ba94fcf9921");
        let observed = decode(
            "b91a3726000000020000003400180001100002080840ffffffff0000084100000000000a\
             001c20120004001000850000ce1d98eb183c45c9967b4ba94fcf9921\
             b91a3726000000020000004400180001100002080840ffffffff0000084100000000000a\
             002c201000040020008600000001000200010000ce1d98eb183c45c9967b4ba94fcf9921\
             0000000100000000\
             b91a3726000000020000004400180001100002080840ffffffff0000084100000000000a\
             002c201000040020008700000001000300010000ce1d98eb183c45c9967b4ba94fcf9921\
             0000000100000000\
             b91a3726000000020000004400180001100002080840ffffffff0000084100000000000a\
             002c20100004002000880000000100010001222fce1d98eb183c45c9967b4ba94fcf9921\
             0000000100000000\
             b91a3726000000020000004400180001100002080840ffffffff0000084100000000000a\
             002c201000040020008900000001000800010000ce1d98eb183c45c9967b4ba94fcf9921\
             0000000100000000\
             b91a3726000000020000004800180001100002080840ffffffff0000084100000000000a\
             0030201100040024008a0000006000010002000000000000ce1d98eb183c45c9967b4ba94fcf9921\
             c0a8013e00000000\
             b91a3726000000020000004800180001100002080840ffffffff0000084100000000000a\
             0030201100040024008b0000006000010008000000000000ce1d98eb183c45c9967b4ba94fcf9921\
             c0a8013e00000000",
        );
        assert_eq!(
            build_inventory_initialization(&domain_id, 0x0085, 0x222F, [192, 168, 1, 62]),
            Some(observed)
        );
    }

    #[test]
    fn managed_identify_matches_the_observed_action() {
        let observed = decode(
            "b91a3726000000020000005000180001100002080840010000020000084100000000000a\
             003820020004000c00ad0000002000180000000000000000ffff0020008600a8842f5774e86d\
             0000417564696e617465073a006300000064",
        );
        assert_eq!(
            build_identify(2, 0x00AD, 0x0086, [0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D]),
            Some(observed)
        );

        let selector_zero = build_identify(0, 0x00AD, 0x0086, [0x84, 0x2F, 0x57, 0x74, 0xE8, 0x6D])
            .expect("selector zero is valid");
        assert_eq!(read_u16(&selector_zero, 24), Some(0));
    }

    #[test]
    fn managed_arc_request_uses_the_observed_envelope_and_zero_alignment() {
        // Status refresh frame 523 from protocol-research run
        // ddm-dapi-arc-read-20260902-01. Source PCAP SHA-256:
        // fda27481bd7cd64f1450b09d29c88daa6e27c68235b353ccd9adceeb4a512954.
        // The two opaque Controller alignment bytes are deliberately encoded
        // as zero; they are outside the declared native ARC packet.
        let arc_packet = decode("2809000a007210000000");
        let expected = decode(
            "b91a37260000000200000038001800011000020808400100000000000841000000000009\
             0020200400040008001b0000000a0014000000002809000a0072100000000000",
        );
        assert_eq!(build_arc_request(0, 27, &arc_packet), Some(expected));
        assert_eq!(build_arc_request(0, 0, &arc_packet), None);

        let mut response_packet = arc_packet;
        response_packet[8..10].copy_from_slice(&1u16.to_be_bytes());
        assert_eq!(build_arc_request(0, 27, &response_packet), None);
    }

    #[test]
    fn managed_arc_response_extracts_the_native_packet_and_correlation_fields() {
        // Status refresh frame 529 from the same retained run.
        let observed = decode(concat!(
            "b91a37250000000200000084001800011000030808400106001500090841010000000005",
            "006c200400040008001b00000058001400000000280900580072100000011df900020000",
            "000000020000000200000002000100010000000000000000000000000030000014140003",
            "000100010002000200000000000200020000000000020002000000000000000000000000",
        ));
        let parsed = parse_arc_response(&observed).unwrap();
        assert_eq!(parsed.wrapper_id, 27);
        assert_eq!(parsed.protocol_id, 0x2809);
        assert_eq!(parsed.transaction_id, 0x0072);
        assert_eq!(parsed.opcode, 0x1000);
        assert_eq!(parsed.result_code, 1);
        assert_eq!(parsed.packet_hex, hexadecimal(&observed[56..144]));
        assert_eq!(parsed.alignment_bytes_hex, "");

        let mut wrong_record_length = observed.clone();
        wrong_record_length[36..38].copy_from_slice(&0x006Au16.to_be_bytes());
        assert_eq!(parse_arc_response(&wrong_record_length), None);
        let mut wrong_inner_length = observed.clone();
        wrong_inner_length[48..50].copy_from_slice(&0x0056u16.to_be_bytes());
        assert_eq!(parse_arc_response(&wrong_inner_length), None);
    }

    #[test]
    fn managed_settings_request_and_acknowledgement_are_transport_only() {
        // Request frame 575 and acknowledgement shape observed in frame 517
        // from ddm-dapi-arc-read-20260902-01.
        let settings_packet =
            decode("ffff0024002d7e3f842f5774e86d0000417564696e617465073a10060000006400000000");
        let expected = decode(concat!(
            "b91a3726000000020000005400180001100002080840010000000000084100000000000a",
            "003c20020004000c00340000002400180000000000000000",
            "ffff0024002d7e3f842f5774e86d0000417564696e617465073a10060000006400000000",
        ));
        assert_eq!(
            build_settings_request(0, 52, &settings_packet),
            Some(expected)
        );

        let acknowledgement = decode(
            "b91a372500000002000000240018000110000308084001060015000a0841010000000004\
             000c200200040000002d0000",
        );
        assert_eq!(
            parse_settings_acknowledgement(&acknowledgement),
            Some(SettingsAcknowledgement { wrapper_id: 45 })
        );
        assert_eq!(parse_settings_publication(&acknowledgement), None);
    }

    #[test]
    fn managed_settings_publication_exposes_the_async_device_answer() {
        // Status refresh frame 581 from ddm-dapi-arc-read-20260902-01.
        let observed = decode(concat!(
            "b91a3725000000020000006400180001100003080841ffffffff00000840010600150010",
            "004c20030004000c000000000024002800020018000000006176696f2d696e7075742d32",
            "002c0003ffff00241c400000001dc1fffe50692e417564696e6174650738100700000000",
            "00000000",
        ));
        let publication = parse_settings_publication(&observed).unwrap();
        assert_eq!(publication.wrapper_id, 0);
        assert_eq!(publication.target_name, "avio-input-2");
        assert_eq!(publication.device_id, "001dc1fffe50692e");
        assert_eq!(publication.message_id, 0x1C40);
        assert_eq!(publication.opcode, 0x1007);
        assert_eq!(publication.packet_hex, hexadecimal(&observed[76..]));
        assert_eq!(parse_settings_acknowledgement(&observed), None);

        let mut wrong_native_length = observed;
        wrong_native_length[48..50].copy_from_slice(&0x0020u16.to_be_bytes());
        assert_eq!(parse_settings_publication(&wrong_native_length), None);
    }

    #[test]
    fn parse_confirmation_rejects_a_different_opcode() {
        let observed = decode(
            "b91a3725000000020000006000180001100003080841ffffffff0000084001060005000a\
             004820030004000c000000000020002800020018000000006176696f2d6f75747075742d33\
             000001ffff002034130000001dc1fffe507b8d417564696e6174650738006200000000",
        );
        assert_eq!(
            parse_identify_confirmation(&observed),
            Some(IdentifyConfirmation {
                device_id: "001dc1fffe507b8d".to_owned()
            })
        );
        let mut wrong_opcode = observed;
        let length = wrong_opcode.len();
        wrong_opcode[length - 6..length - 4].copy_from_slice(&0x0063u16.to_be_bytes());
        assert_eq!(parse_identify_confirmation(&wrong_opcode), None);
    }
}
