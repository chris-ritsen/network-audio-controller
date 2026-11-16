use super::protobuf::{self as pb, Message, Value};
use serde::{Deserialize, Serialize};
pub trait Wire: Sized {
    fn decode(message: &Message<'_>) -> Option<Self>;
    fn encode(&self) -> Vec<u8>;
}
macro_rules! numbers {
    ($name:ident { $($field:ident : $n:literal),* $(,)? }) => {
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
        #[serde(deny_unknown_fields)]
        pub struct $name { $(pub $field:u32,)* }
        impl Wire for $name {
            fn decode(message:&Message<'_>)->Option<Self> { Some(Self { $($field:pb::uint(message,$n)?,)* }) }
            fn encode(&self)->Vec<u8> { let mut encoded=Vec::new(); $(pb::put_uint(&mut encoded,$n,self.$field);)* encoded }
        }
    }
}
numbers!(VideoFormat {
    resolution: 1,
    bit_depth: 2,
    color_space: 3
});
numbers!(CodecFormat {
    codec_type: 1,
    profile: 2,
    level: 3
});
numbers!(SerialSettings {
    baud_rate: 1,
    hardware_flow_control: 2,
    software_flow_control: 3,
    data_bits: 4,
    parity: 5,
    stop_bits: 6
});
numbers!(BandwidthSettings {
    target: 1,
    minimum: 2,
    maximum: 3,
    enabled: 4
});
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SelectionMode {
    pub manual_resolution: bool,
    pub manual_bit_depth: bool,
    pub manual_color_space: bool,
}
impl Wire for SelectionMode {
    fn decode(message: &Message<'_>) -> Option<Self> {
        Some(Self {
            manual_resolution: pb::uint(message, 1)? != 0,
            manual_bit_depth: pb::uint(message, 2)? != 0,
            manual_color_space: pb::uint(message, 3)? != 0,
        })
    }
    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        for (number, value) in [
            (1, self.manual_resolution),
            (2, self.manual_bit_depth),
            (3, self.manual_color_space),
        ] {
            pb::put_uint(&mut encoded, number, value.into());
        }
        encoded
    }
}
pub fn child<T: Wire>(message: &Message<'_>, number: u32) -> Option<Option<T>> {
    match pb::nested(message, number)? {
        Some(child_message) => Some(Some(T::decode(&child_message)?)),
        None => Some(None),
    }
}
fn repeated<T: Wire>(message: &Message<'_>, number: u32) -> Option<Vec<T>> {
    message
        .iter()
        .filter(|field| field.number == number)
        .map(|field| {
            let Value::Bytes(encoded) = field.value else {
                return None;
            };
            T::decode(&pb::parse(encoded)?)
        })
        .collect()
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoFormatStatus {
    pub configured: Option<VideoFormat>,
    pub supported: Vec<VideoFormat>,
    pub selection: Option<SelectionMode>,
    pub actual: Option<VideoFormat>,
    pub direction: u32,
}
impl VideoFormatStatus {
    pub fn decode(message: &Message<'_>) -> Option<Self> {
        Some(Self {
            configured: child(message, 1)?,
            supported: repeated(message, 2)?,
            selection: child(message, 3)?,
            actual: child(message, 4)?,
            direction: pb::uint(message, 5)?,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecFormatStatus {
    pub current: Option<CodecFormat>,
    pub supported: Vec<CodecFormat>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoChannelStatus {
    pub direction: u32,
    pub status_code: u32,
    pub observed_hdcp_version: Option<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HdcpSettings {
    pub configured_mode: u32,
    pub supported_modes: Vec<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViscaStatus {
    pub capability: u32,
    pub reply: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BluetoothConnection {
    pub state: u32,
    pub connected: Option<bool>,
    pub peer_name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BluetoothIdentification {
    pub name_source: u32,
    pub custom_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_name_raw_hexadecimal: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "category", content = "value", rename_all = "snake_case")]
pub enum PanelObservation {
    BluetoothConnection(BluetoothConnection),
    BluetoothIdentification(BluetoothIdentification),
    BluetoothDiscovery(u32),
    BluetoothPairing(u32),
    VideoFormat(VideoFormatStatus),
    CodecFormat(CodecFormatStatus),
    VideoChannel(VideoChannelStatus),
    Serial(SerialSettings),
    Bandwidth(BandwidthSettings),
    Hdcp(HdcpSettings),
    Visca(ViscaStatus),
}
pub fn decode_application(family: &str, data: &[u8]) -> Option<Vec<PanelObservation>> {
    let root = pb::parse(data)?;
    let mut out = Vec::new();
    if family == "bluetooth" {
        let Some(bluetooth) = pb::nested(&root, 1)? else {
            return Some(out);
        };
        let known: Vec<_> = bluetooth
            .iter()
            .filter(|field| matches!(field.number, 1..=8))
            .collect();
        if known.len() > 1 {
            return None;
        }
        for field in known {
            let Value::Bytes(encoded) = field.value else {
                return None;
            };
            let message = pb::parse(encoded)?;
            let observation = match field.number {
                2 => {
                    let Some(connection) = pb::nested(&message, 1)? else {
                        continue;
                    };
                    let state = pb::uint(&connection, 1)?;
                    PanelObservation::BluetoothConnection(BluetoothConnection {
                        state,
                        connected: match state {
                            1 => Some(true),
                            2 => Some(false),
                            _ => None,
                        },
                        peer_name: pb::string(&connection, 2)?,
                    })
                }
                3 => {
                    let Some(identification) = pb::nested(&message, 1)? else {
                        continue;
                    };
                    let raw = pb::bytes(&identification, 2)?.unwrap_or_default();
                    let (custom_name, custom_name_raw_hexadecimal) = match std::str::from_utf8(raw)
                    {
                        Ok(name) => (name.to_owned(), None),
                        Err(_) => (
                            String::from_utf8_lossy(raw).into_owned(),
                            Some(crate::bytes::hexadecimal(raw)),
                        ),
                    };
                    PanelObservation::BluetoothIdentification(BluetoothIdentification {
                        name_source: pb::uint(&identification, 1)?,
                        custom_name,
                        custom_name_raw_hexadecimal,
                    })
                }
                5 => {
                    let Some(discovery) = pb::nested(&message, 1)? else {
                        continue;
                    };
                    PanelObservation::BluetoothDiscovery(pb::uint(&discovery, 1)?)
                }
                7 => PanelObservation::BluetoothPairing(pb::uint(&message, 1)?),
                _ => continue,
            };
            out.push(observation);
        }
    } else if family == "dante_av" {
        let known: Vec<_> = root
            .iter()
            .filter(|field| matches!(field.number, 1..=11))
            .collect();
        if known.len() > 1 {
            return None;
        }
        for field in known {
            let Value::Bytes(encoded) = field.value else {
                return None;
            };
            let message = pb::parse(encoded)?;
            let observation = match field.number {
                2 => PanelObservation::VideoFormat(VideoFormatStatus::decode(&message)?),
                4 => PanelObservation::CodecFormat(CodecFormatStatus {
                    current: child(&message, 1)?,
                    supported: repeated(&message, 2)?,
                }),
                6 => PanelObservation::VideoChannel(VideoChannelStatus {
                    direction: pb::uint(&message, 1)?,
                    status_code: pb::uint(&message, 2)?,
                    observed_hdcp_version: if message
                        .iter()
                        .any(|message_field| message_field.number == 3)
                    {
                        Some(pb::uint(&message, 3)?)
                    } else {
                        None
                    },
                }),
                7 => PanelObservation::Serial(SerialSettings::decode(&message)?),
                8 => PanelObservation::Bandwidth(BandwidthSettings::decode(&message)?),
                9 => PanelObservation::Hdcp(HdcpSettings {
                    configured_mode: pb::uint(&message, 1)?,
                    supported_modes: pb::repeated_uint(&message, 2)?,
                }),
                11 => PanelObservation::Visca(ViscaStatus {
                    capability: pb::uint(&message, 1)?,
                    reply: pb::bytes(&message, 2)?.unwrap_or_default().to_vec(),
                }),
                _ => continue,
            };
            out.push(observation);
        }
    } else {
        return None;
    }
    Some(out)
}
