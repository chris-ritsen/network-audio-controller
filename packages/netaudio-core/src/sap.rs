use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

pub const MULTICAST_ADDRESS: &str = "239.255.255.255";
pub const PORT: u16 = 9875;

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SapSessionAnnouncement {
    pub message_hash: std::num::NonZeroU16,
    pub raw_sdp: String,
}

/// The host selects the session by parsed origin/session identity and expires its cache.
#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SapTransitionRequest {
    pub delete: bool,
    pub current: SapSessionAnnouncement,
    pub previous: Option<SapSessionAnnouncement>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SapTransition {
    Added,
    Refreshed,
    Replaced,
    Deleted,
}

pub fn transition(request: SapTransitionRequest) -> Option<SapTransition> {
    let previous = request.previous.as_ref();
    let same_message =
        previous.is_some_and(|previous| previous.message_hash == request.current.message_hash);

    if request.delete {
        return same_message.then_some(SapTransition::Deleted);
    }

    Some(match previous {
        None => SapTransition::Added,
        Some(previous) if same_message && previous.raw_sdp == request.current.raw_sdp => {
            SapTransition::Refreshed
        }
        Some(_) => SapTransition::Replaced,
    })
}

const CONTENT_TYPE: &[u8] = b"application/sdp\0";

/// SAP framing and its unmodified SDP payload. SDP interpretation is separate.
#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SapAnnouncement {
    pub version: u8,
    pub delete: bool,
    pub reserved: bool,
    pub authentication_length_words: u8,
    pub message_hash: u16,
    pub origin_address: String,
    pub authentication_data_hexadecimal: String,
    pub content_type: &'static str,
    pub raw_sdp: String,
}

pub fn parse(data: &[u8]) -> Result<SapAnnouncement, &'static str> {
    if data.len() < 8 {
        return Err("SAP packet is shorter than its IPv4 header");
    }

    let flags = data[0];
    let version = flags >> 5;

    if version != 1 {
        return Err("only SAP version 1 is supported");
    }

    if flags & 0x10 != 0 {
        return Err("SAP IPv6 origin addresses are not supported");
    }

    if flags & 0x02 != 0 {
        return Err("encrypted SAP packets are not supported");
    }

    if flags & 0x01 != 0 {
        return Err("compressed SAP packets are not supported");
    }

    let message_hash = u16::from_be_bytes([data[2], data[3]]);

    if message_hash == 0 {
        return Err("SAP message hash must be nonzero");
    }

    let origin = Ipv4Addr::new(data[4], data[5], data[6], data[7]);

    if origin.is_unspecified() {
        return Err("SAP origin address must be nonzero");
    }

    let authentication_length_words = data[1];
    let payload_offset = 8 + usize::from(authentication_length_words) * 4;
    let payload = data
        .get(payload_offset..)
        .ok_or("SAP authentication length exceeds the packet")?;
    let sdp = payload
        .strip_prefix(CONTENT_TYPE)
        .ok_or("SAP payload type must be exactly NUL-terminated application/sdp")?;

    if sdp.is_empty() {
        return Err("SAP packet has no SDP payload");
    }

    let raw_sdp = std::str::from_utf8(sdp)
        .map_err(|_| "SAP SDP payload is not valid UTF-8")?
        .to_owned();

    Ok(SapAnnouncement {
        version,
        delete: flags & 0x04 != 0,
        reserved: flags & 0x08 != 0,
        authentication_length_words,
        message_hash,
        origin_address: origin.to_string(),
        authentication_data_hexadecimal: data[8..payload_offset]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        content_type: "application/sdp",
        raw_sdp,
    })
}
