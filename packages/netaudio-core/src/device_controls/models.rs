use super::protobuf::{self as pb, Message, Value};
use serde::{Deserialize, Serialize};
pub trait Wire: Sized {
    fn decode(m: &Message<'_>) -> Option<Self>;
    fn encode(&self) -> Vec<u8>;
}
macro_rules! numbers {
    ($name:ident { $($field:ident : $n:literal),* $(,)? }) => {
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name { $(pub $field:u32,)* }
        impl Wire for $name {
            fn decode(m:&Message<'_>)->Option<Self> { Some(Self { $($field:pb::uint(m,$n)?,)* }) }
            fn encode(&self)->Vec<u8> { let mut b=Vec::new(); $(pb::put_uint(&mut b,$n,self.$field);)* b }
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
#[serde(deny_unknown_fields)]
pub struct SelectionMode {
    pub manual_resolution: bool,
    pub manual_bit_depth: bool,
    pub manual_color_space: bool,
}
impl Wire for SelectionMode {
    fn decode(m: &Message<'_>) -> Option<Self> {
        Some(Self {
            manual_resolution: pb::uint(m, 1)? != 0,
            manual_bit_depth: pb::uint(m, 2)? != 0,
            manual_color_space: pb::uint(m, 3)? != 0,
        })
    }
    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        for (n, v) in [
            (1, self.manual_resolution),
            (2, self.manual_bit_depth),
            (3, self.manual_color_space),
        ] {
            pb::put_uint(&mut b, n, v.into());
        }
        b
    }
}
pub fn child<T: Wire>(m: &Message<'_>, n: u32) -> Option<Option<T>> {
    match pb::nested(m, n)? {
        Some(v) => Some(Some(T::decode(&v)?)),
        None => Some(None),
    }
}
fn repeated<T: Wire>(m: &Message<'_>, n: u32) -> Option<Vec<T>> {
    m.iter()
        .filter(|f| f.number == n)
        .map(|f| {
            let Value::Bytes(b) = f.value else {
                return None;
            };
            T::decode(&pb::parse(b)?)
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
    pub fn decode(m: &Message<'_>) -> Option<Self> {
        Some(Self {
            configured: child(m, 1)?,
            supported: repeated(m, 2)?,
            selection: child(m, 3)?,
            actual: child(m, 4)?,
            direction: pb::uint(m, 5)?,
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
    pub peer_name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BluetoothIdentification {
    pub name_source: u32,
    pub custom_name: String,
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
        let Some(m) = pb::nested(&root, 1)? else {
            return Some(out);
        };
        let known: Vec<_> = m.iter().filter(|f| matches!(f.number, 1..=8)).collect();
        if known.len() > 1 {
            return None;
        }
        for f in known {
            let Value::Bytes(b) = f.value else {
                return None;
            };
            let v = pb::parse(b)?;
            let observation = match f.number {
                2 => {
                    let Some(d) = pb::nested(&v, 1)? else {
                        continue;
                    };
                    PanelObservation::BluetoothConnection(BluetoothConnection {
                        state: pb::uint(&d, 1)?,
                        peer_name: pb::string(&d, 2)?,
                    })
                }
                3 => {
                    let Some(d) = pb::nested(&v, 1)? else {
                        continue;
                    };
                    PanelObservation::BluetoothIdentification(BluetoothIdentification {
                        name_source: pb::uint(&d, 1)?,
                        custom_name: pb::string(&d, 2)?,
                    })
                }
                5 => {
                    let Some(d) = pb::nested(&v, 1)? else {
                        continue;
                    };
                    PanelObservation::BluetoothDiscovery(pb::uint(&d, 1)?)
                }
                7 => PanelObservation::BluetoothPairing(pb::uint(&v, 1)?),
                _ => continue,
            };
            out.push(observation);
        }
    } else if family == "dante_av" {
        let known: Vec<_> = root.iter().filter(|f| matches!(f.number, 1..=11)).collect();
        if known.len() > 1 {
            return None;
        }
        for f in known {
            let Value::Bytes(b) = f.value else {
                return None;
            };
            let v = pb::parse(b)?;
            let observation = match f.number {
                2 => PanelObservation::VideoFormat(VideoFormatStatus::decode(&v)?),
                4 => PanelObservation::CodecFormat(CodecFormatStatus {
                    current: child(&v, 1)?,
                    supported: repeated(&v, 2)?,
                }),
                6 => PanelObservation::VideoChannel(VideoChannelStatus {
                    direction: pb::uint(&v, 1)?,
                    status_code: pb::uint(&v, 2)?,
                    observed_hdcp_version: if v.iter().any(|f| f.number == 3) {
                        Some(pb::uint(&v, 3)?)
                    } else {
                        None
                    },
                }),
                7 => PanelObservation::Serial(SerialSettings::decode(&v)?),
                8 => PanelObservation::Bandwidth(BandwidthSettings::decode(&v)?),
                9 => PanelObservation::Hdcp(HdcpSettings {
                    configured_mode: pb::uint(&v, 1)?,
                    supported_modes: pb::repeated_uint(&v, 2)?,
                }),
                11 => PanelObservation::Visca(ViscaStatus {
                    capability: pb::uint(&v, 1)?,
                    reply: pb::bytes(&v, 2)?.unwrap_or_default().to_vec(),
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
