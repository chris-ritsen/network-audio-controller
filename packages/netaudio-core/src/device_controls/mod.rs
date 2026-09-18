//! Device controls scoped by the supplied interoperability specification.
mod models;
mod protobuf;
use crate::bytes::{read_u16, read_u32};
use crate::protocol::{validate_conmon_envelope, ConmonHeader, NetaudioError};
pub use models::*;
use protobuf as pb;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize)]
pub struct PanelStatus {
    pub record_revision: u16,
    pub common_word: u32,
    pub envelope_length: u16,
    pub envelope_offset: u16,
    pub source_identifier: Vec<u8>,
    pub requester: u32,
    pub sequence: u32,
    pub application_payload: Vec<u8>,
    pub raw_record: Vec<u8>,
    pub observations: Vec<PanelObservation>,
    pub diagnostic_error: Option<String>,
}
pub fn parse_panel_status(data: &[u8], family: Option<&str>) -> Option<PanelStatus> {
    validate_conmon_envelope(data, 0x100e)?;
    let r = data.get(24..)?;
    let len = read_u16(r, 8)?;
    let offset = read_u16(r, 10)?;
    if offset < 12 {
        return None;
    }
    let end = usize::from(offset).checked_add(usize::from(len))?;
    let envelope = pb::parse(r.get(usize::from(offset)..end)?)?;
    if envelope
        .iter()
        .filter(|f| f.number == 1 || f.number == 2)
        .count()
        != 1
        || envelope.iter().any(|f| f.number == 1)
    {
        return None;
    }
    let status = pb::nested(&envelope, 2)??;
    let payload = pb::bytes(&status, 4)?.unwrap_or_default();
    let decoded = family.map(|f| decode_application(f, payload));
    let (observations, diagnostic_error) = match decoded {
        Some(Some(v)) => (v, None),
        Some(None) => (Vec::new(), Some("Malformed application payload".to_owned())),
        None => (Vec::new(), None),
    };
    Some(PanelStatus {
        record_revision: read_u16(r, 0)?,
        common_word: read_u32(r, 4)?,
        envelope_length: len,
        envelope_offset: offset,
        source_identifier: pb::bytes(&status, 1)?.unwrap_or_default().to_vec(),
        requester: pb::uint(&status, 2)?,
        sequence: pb::uint(&status, 3)?,
        application_payload: payload.to_vec(),
        raw_record: r.to_vec(),
        observations,
        diagnostic_error,
    })
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum PanelRequest {
    BluetoothQuery {
        selector: u32,
    },
    BluetoothIdentification {
        name_source: u32,
        custom_name: String,
    },
    BluetoothDiscovery {
        discoverable: bool,
    },
    BluetoothClearPairing {
        confirmed: bool,
    },
    VideoQuery {
        selector: u32,
    },
    VideoFormat {
        format: VideoFormat,
        selection: SelectionMode,
    },
    CodecFormat {
        format: CodecFormat,
    },
    Serial {
        settings: SerialSettings,
    },
    Bandwidth {
        target: u32,
        enabled: bool,
    },
    Hdcp {
        mode: u32,
    },
    VideoViscaQuery,
    VideoViscaFormat {
        format: VideoFormat,
        selection: SelectionMode,
    },
}
fn invalid<T>() -> Result<T, NetaudioError> {
    Err(NetaudioError::UnsupportedProtocolOperation)
}
fn single_bit(v: u32) -> bool {
    v != 0 && v.is_power_of_two() && v <= 32
}
pub fn scoped_visca(
    format: &VideoFormat,
    selection: &SelectionMode,
) -> Result<Vec<u8>, NetaudioError> {
    if !selection.manual_resolution || format.resolution == 0 || format.resolution > u16::MAX.into()
    {
        return invalid();
    }
    let color = if selection.manual_color_space {
        match format.color_space {
            1 => 0,
            16 => 1,
            8 => 2,
            _ => return invalid(),
        }
    } else {
        2
    };
    let depth = if selection.manual_bit_depth {
        match format.bit_depth {
            2 => 0,
            4 => 1,
            8 => 2,
            _ => return invalid(),
        }
    } else {
        2
    };
    let r = (format.resolution as u16).to_be_bytes();
    Ok(vec![0x81, 1, 0x99, 1, r[0], r[1], color, depth, 0xff])
}
impl PanelRequest {
    pub fn family(&self) -> &'static str {
        match self {
            Self::BluetoothQuery { .. }
            | Self::BluetoothIdentification { .. }
            | Self::BluetoothDiscovery { .. }
            | Self::BluetoothClearPairing { .. } => "bluetooth",
            _ => "dante_av",
        }
    }
    pub fn encode(&self) -> Result<Vec<u8>, NetaudioError> {
        let (field, body) = match self {
            Self::BluetoothQuery { selector } => {
                if !(1..=4).contains(selector) {
                    return invalid();
                }
                (1, pb::scalar(1, *selector))
            }
            Self::BluetoothIdentification {
                name_source,
                custom_name,
            } => {
                if !matches!(name_source, 1 | 2) || custom_name.chars().count() > 32 {
                    return invalid();
                }
                let mut b = pb::scalar(1, *name_source);
                pb::put_bytes(&mut b, 2, custom_name.as_bytes());
                (4, pb::message(1, &b))
            }
            Self::BluetoothDiscovery { discoverable } => (
                6,
                pb::message(1, &pb::scalar(1, if *discoverable { 1 } else { 2 })),
            ),
            Self::BluetoothClearPairing { confirmed } => {
                if !confirmed {
                    return invalid();
                }
                (8, pb::scalar(1, 1))
            }
            Self::VideoQuery { selector } => {
                if !(1..=7).contains(selector) {
                    return invalid();
                }
                (1, pb::scalar(1, *selector))
            }
            Self::VideoFormat { format, selection } => {
                if (selection.manual_resolution && format.resolution == 0)
                    || (!selection.manual_resolution && format.resolution != 0)
                    || (selection.manual_bit_depth && !single_bit(format.bit_depth))
                    || (selection.manual_color_space && !single_bit(format.color_space))
                {
                    return invalid();
                }
                let mut b = pb::message(1, &format.encode());
                pb::put_bytes(&mut b, 2, &selection.encode());
                (3, b)
            }
            Self::CodecFormat { format } => {
                if format.codec_type != 1
                    || !matches!(format.profile, 1 | 2)
                    || !single_bit(format.level)
                    || format.level > 16
                {
                    return invalid();
                }
                (5, pb::message(1, &format.encode()))
            }
            Self::Serial { settings: s } => {
                if ![1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200, 230400]
                    .contains(&s.baud_rate)
                    || !matches!(s.data_bits, 7 | 8)
                    || s.parity > 2
                    || !matches!(s.stop_bits, 1 | 2)
                    || s.data_bits == 7 && s.parity == 0
                    || s.hardware_flow_control != 0
                    || s.software_flow_control != 0
                {
                    return invalid();
                }
                let mut b = Vec::new();
                for (n, v) in [
                    (1, s.baud_rate),
                    (2, s.hardware_flow_control),
                    (3, s.software_flow_control),
                    (4, s.data_bits),
                    (5, s.parity),
                    (6, s.stop_bits),
                ] {
                    pb::put_uint_explicit(&mut b, n, v);
                }
                (7, b)
            }
            Self::Bandwidth { target, enabled } => {
                if *enabled && (*target == 0 || *target > 700) || !enabled && *target != 0 {
                    return invalid();
                }
                let mut b = Vec::new();
                pb::put_uint_explicit(&mut b, 1, *target);
                pb::put_uint_explicit(&mut b, 4, (*enabled).into());
                (8, b)
            }
            Self::Hdcp { mode } => {
                if !(1..=3).contains(mode) {
                    return invalid();
                }
                (9, pb::scalar(1, *mode))
            }
            Self::VideoViscaQuery => (10, pb::message(1, &[0x81, 9, 0x99, 2, 0xff])),
            Self::VideoViscaFormat { format, selection } => {
                (10, pb::message(1, &scoped_visca(format, selection)?))
            }
        };
        let b = pb::message(field, &body);
        Ok(if self.family() == "bluetooth" {
            pb::message(1, &b)
        } else {
            b
        })
    }
}
pub fn build_panel_control(
    request: &PanelRequest,
    requester: u32,
    sequence: u32,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let mut content = pb::scalar(1, requester);
    pb::put_uint(&mut content, 2, sequence);
    pb::put_bytes(&mut content, 3, &request.encode()?);
    let envelope = pb::message(1, &content);
    if envelope.len() > 1436 {
        return invalid();
    }
    let mut record = Vec::new();
    record.extend_from_slice(&0x0734u16.to_be_bytes());
    record.extend_from_slice(&0x100du16.to_be_bytes());
    record.extend_from_slice(&0u32.to_be_bytes());
    record.extend_from_slice(&(envelope.len() as u16).to_be_bytes());
    record.extend_from_slice(&12u16.to_be_bytes());
    record.extend(envelope);
    let mut body = vec![0, 0];
    body.extend_from_slice(&mac);
    body.extend_from_slice(&[0, 0]);
    body.extend_from_slice(b"Audinate");
    body.extend(record);
    ConmonHeader {
        message_id,
        protocol_id: 0xffff,
    }
    .packet(&body)
}
#[cfg(test)]
mod tests;
