use std::{collections::BTreeMap, net::Ipv4Addr};

use serde::{Deserialize, Serialize};

use crate::parser::{channel_audio_publication, ChannelAudioConfiguration};
use crate::protocol::{NetaudioPort, SERVICE_ARC};

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct VirtualDeviceAdvertisement {
    pub name: String,
    pub model: String,
    pub manufacturer: String,
    pub address: Ipv4Addr,
    pub arc_port: u16,
    pub tx_channels: Vec<String>,
    pub sample_rate: u32,
    pub encoding: u16,
    pub supported_encodings: Vec<u16>,
    pub configured_latency_ns: u32,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ServiceAdvertisement {
    pub service_type: String,
    pub name: String,
    pub server: String,
    pub address: [u8; 4],
    pub port: u16,
    pub properties: BTreeMap<String, String>,
}

pub fn virtual_device(config: VirtualDeviceAdvertisement) -> Vec<ServiceAdvertisement> {
    let service = |kind: &str, instance: String, port, properties| ServiceAdvertisement {
        service_type: kind.into(),
        name: format!("{instance}.{kind}"),
        server: format!("{}.local.", config.name),
        address: config.address.octets(),
        port,
        properties,
    };
    let properties = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect()
    };
    let identity = u32::from(config.address);
    let mut services = vec![
        service(
            SERVICE_ARC,
            config.name.clone(),
            config.arc_port,
            properties(&[
                ("arcp_vers", "2.7.41"),
                ("arcp_min", "0.2.4"),
                ("router_vers", "4.0.2"),
                ("router_info", &config.model),
                ("mf", &config.manufacturer),
                ("model", &config.model),
            ]),
        ),
        service(
            "_netaudio-cmc._udp.local.",
            config.name.clone(),
            NetaudioPort::Control as u16,
            properties(&[
                ("id", &format!("0000{identity:08x}0000")),
                ("process", "0"),
                ("cmcp_vers", "1.2.0"),
                ("cmcp_min", "1.0.0"),
                ("server_vers", "4.0.2"),
                ("channels", "0x6000004d"),
                ("mf", &config.manufacturer),
                ("model", &config.model),
            ]),
        ),
    ];
    let audio = channel_audio_publication(&ChannelAudioConfiguration {
        sample_rate: config.sample_rate,
        encoding: config.encoding,
        supported_encodings: config.supported_encodings,
    });

    for (index, name) in config.tx_channels.iter().enumerate() {
        let mut channel: BTreeMap<String, String> = properties(&[
            ("txtvers", "2"),
            ("dbcp1", "0x1102"),
            ("dbcp", "0x1004"),
            ("id", &(index + 1).to_string()),
            ("rate", &config.sample_rate.to_string()),
            ("enc", &config.encoding.to_string()),
            ("en", &config.encoding.to_string()),
            ("latency_ns", &config.configured_latency_ns.to_string()),
            ("fpp", "32,2"),
            ("nchan", "8"),
        ]);

        if let Some(audio) = &audio {
            channel.insert("pcm".into(), audio.pcm_property.clone());
        }

        services.push(service(
            "_netaudio-chan._udp.local.",
            format!("{name}@{}", config.name),
            NetaudioPort::ArcSecondary as u16,
            channel,
        ));
    }

    services
}
