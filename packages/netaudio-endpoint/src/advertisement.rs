use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use anyhow::Result;
use mdns_sd::{ServiceInfo, TxtProperty};
use netaudio_core::bytes::hexadecimal;
use netaudio_core::native_audio::endpoint_control::monitoring::Identity;
use netaudio_core::native_audio::endpoint_control::{
    Endpoint, NAMED_CLOCK_DOMAIN, TRANSMIT_LATENCY_NANOSECONDS,
};
use netaudio_core::native_audio::source_control::REFERENCE_REVISION;
use netaudio_core::protocol::{SERVICE_ARC, SERVICE_CHAN, SERVICE_CMC, SERVICE_DBC};

pub struct Advertisement<'a> {
    pub address: Ipv4Addr,
    pub endpoint: &'a Endpoint,
    pub identity: Identity,
    pub manufacturer: &'a str,
    pub model: &'a str,
    pub monitoring_port: u16,
    pub routing_port: u16,
    pub source_port: u16,
}

impl Advertisement<'_> {
    pub fn services(&self) -> Result<Vec<ServiceInfo>> {
        let endpoint = self.endpoint;
        let address = IpAddr::V4(self.address);
        let host = format!("{}.local.", endpoint.name);
        let mut services = Vec::new();
        let routing = HashMap::from([
            ("arcp_vers".to_owned(), "2.7.255".to_owned()),
            ("arcp_min".to_owned(), "2.7.255".to_owned()),
            ("router_vers".to_owned(), "4.0.0".to_owned()),
        ]);
        services.push(ServiceInfo::new(
            SERVICE_ARC,
            &endpoint.name,
            &host,
            address,
            self.routing_port,
            routing,
        )?);
        let monitoring = HashMap::from([
            ("cmcp_vers".to_owned(), "1.2.0".to_owned()),
            ("cmcp_min".to_owned(), "1.2.0".to_owned()),
            ("id".to_owned(), hexadecimal(&self.identity.instance)),
            ("process".to_owned(), self.identity.process.to_string()),
            ("channels".to_owned(), "00000004".to_owned()),
            ("mf".to_owned(), self.manufacturer.to_owned()),
            ("model".to_owned(), self.model.to_owned()),
            ("server_vers".to_owned(), env!("CARGO_PKG_VERSION").to_owned()),
        ]);
        services.push(ServiceInfo::new(
            SERVICE_CMC,
            &endpoint.name,
            &host,
            address,
            self.monitoring_port,
            monitoring,
        )?);
        let source = HashMap::from([
            ("dbcp1".to_owned(), format!("{REFERENCE_REVISION:#06x}")),
            ("dbcp".to_owned(), format!("{REFERENCE_REVISION:#06x}")),
        ]);
        services.push(ServiceInfo::new(
            SERVICE_DBC,
            &endpoint.name,
            &host,
            address,
            self.source_port,
            source,
        )?);

        for (index, name) in endpoint.transmit_names.iter().enumerate() {
            let properties = [
                ("txtvers", "2".to_owned()),
                ("id", (index + 1).to_string()),
                ("nchan", endpoint.profile.flow_slots.to_string()),
                ("dbcp1", format!("{REFERENCE_REVISION:#06x}")),
                ("dbcp", format!("{REFERENCE_REVISION:#06x}")),
                ("rate", endpoint.profile.sample_rate.to_string()),
                ("en", endpoint.profile.sample_bits.to_string()),
                ("enc", endpoint.profile.sample_bits.to_string()),
                (
                    "pcm",
                    format!(
                        "{} {:x}",
                        endpoint.profile.sample_bits / 8,
                        1_u16 << (endpoint.profile.sample_bits / 8 - 1)
                    ),
                ),
                (
                    "fpp",
                    format!(
                        "{},{}-{}",
                        endpoint.profile.frames_per_packet,
                        endpoint.profile.minimum_frames_per_packet(),
                        endpoint.profile.maximum_frames_per_packet()
                    ),
                ),
                ("latency_ns", TRANSMIT_LATENCY_NANOSECONDS.to_string()),
            ];
            let mut properties: Vec<TxtProperty> =
                properties.iter().map(TxtProperty::from).collect();
            properties.push(TxtProperty::from("at2"));

            if !NAMED_CLOCK_DOMAIN.text().is_empty() {
                properties.push(TxtProperty::from(("subdomain", NAMED_CLOCK_DOMAIN.text())));
            }

            services.push(ServiceInfo::new(
                SERVICE_CHAN,
                &format!("{name}@{}", endpoint.name),
                &host,
                address,
                self.source_port,
                properties,
            )?);
        }

        Ok(services)
    }
}
