use super::*;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockControlAvailability {
    pub clock_source: bool,
    pub preferred_leader: bool,
    pub subdomain: bool,
    pub global_unicast_delay_requests: bool,
    pub aggregate_ptpv1_unicast_delay_requests: bool,
    pub follower_only: bool,
    pub priority_mapping: bool,
    pub preferred_protocol: bool,
    pub ptpv2_clock_class: bool,
    pub ptpv2_domain: bool,
    pub ptpv2_priority1: bool,
    pub ptpv2_priority2: bool,
    pub multicast_dscp: bool,
    pub ports: bool,
}

pub fn default_clock_control_profile() -> u16 {
    0x073a
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClockPortControl {
    pub port_id: u16,
    pub ttl: Option<u8>,
    pub sync_interval: Option<i8>,
    pub announce_interval: Option<i8>,
    pub delay_request_interval: Option<i8>,
    pub peer_delay_interval: Option<i8>,
    pub delay_mechanism: Option<u8>,
    pub follower_only: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClockControl {
    #[serde(default = "default_clock_control_profile")]
    pub control_profile: u16,
    pub status_revision: Option<u16>,
    pub clock_capabilities: Option<u16>,
    pub extension_flags: Option<u16>,
    pub clock_source: Option<u16>,
    pub preferred_leader: Option<bool>,
    pub subdomain: Option<Vec<u8>>,
    pub global_unicast_delay_requests: Option<bool>,
    pub aggregate_ptpv1_unicast_delay_requests: Option<bool>,
    pub aggregate_ptpv2_unicast_delay_requests: Option<bool>,
    pub follower_only: Option<bool>,
    pub ptpv1_enabled: Option<bool>,
    pub ptpv2_enabled: Option<bool>,
    pub priority_mapping: Option<u8>,
    pub preferred_protocol: Option<u8>,
    pub ptpv2_clock_class: Option<u8>,
    pub ptpv2_domain: Option<u8>,
    pub ptpv2_priority1: Option<u8>,
    pub ptpv2_priority2: Option<u8>,
    pub multicast_dscp: Option<u8>,
    #[serde(default)]
    pub ports: Vec<ClockPortControl>,
    #[serde(default)]
    pub advanced: bool,
    #[serde(default)]
    pub supported_clock_sources: Vec<u16>,
}

impl Default for ClockControl {
    fn default() -> Self {
        serde_json::from_str("{}").expect("optional clock control fields")
    }
}

impl ClockControl {
    pub fn availability(&self) -> ClockControlAvailability {
        let rev = self.status_revision.unwrap_or(0);

        if rev != 0x0100 && rev < 0x0200 {
            return ClockControlAvailability::default();
        }

        let caps = self.clock_capabilities.unwrap_or(0);
        ClockControlAvailability {
            clock_source: true,
            preferred_leader: self.clock_capabilities.is_some()
                && caps & 0x0120 == 0
                && (rev < 0x072e
                    || self
                        .extension_flags
                        .is_some_and(|flags| flags & 0x1000 == 0)),
            subdomain: caps & 4 != 0,
            global_unicast_delay_requests: (0x0603..=0x071e).contains(&rev)
                && caps & 8 != 0
                && caps & 0x0200 == 0,
            aggregate_ptpv1_unicast_delay_requests: rev >= 0x071f && caps & 0x0200 != 0,
            ..Default::default()
        }
    }

    pub fn is_query(&self) -> bool {
        self.clock_source.is_none()
            && self.preferred_leader.is_none()
            && self.subdomain.is_none()
            && self.global_unicast_delay_requests.is_none()
            && self.aggregate_ptpv1_unicast_delay_requests.is_none()
            && !self.has_extended_changes()
    }

    fn has_extended_changes(&self) -> bool {
        self.aggregate_ptpv2_unicast_delay_requests.is_some()
            || self.follower_only.is_some()
            || self.ptpv1_enabled.is_some()
            || self.ptpv2_enabled.is_some()
            || self.priority_mapping.is_some()
            || self.preferred_protocol.is_some()
            || self.ptpv2_clock_class.is_some()
            || self.ptpv2_domain.is_some()
            || self.ptpv2_priority1.is_some()
            || self.ptpv2_priority2.is_some()
            || self.multicast_dscp.is_some()
            || !self.ports.is_empty()
    }
}

pub fn build_clock_control(
    control: &ClockControl,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let c = control;
    let rev = c.control_profile;
    let reject = NetaudioError::UnsupportedProtocolOperation;
    if !matches!(rev, 0x0734 | 0x073a)
        || (rev == 0x0734 && c.has_extended_changes())
        || c.multicast_dscp.is_some_and(|value| value > 63)
        || (!c.advanced
            && (c.ptpv1_enabled.is_some()
                || c.ptpv2_enabled.is_some()
                || c.aggregate_ptpv2_unicast_delay_requests.is_some()))
    {
        return Err(reject);
    }

    let allowed = c.availability();
    if c.clock_source.is_some() && !c.advanced && !allowed.clock_source {
        return Err(reject);
    }
    if c.preferred_leader.is_some() && !c.advanced && !allowed.preferred_leader {
        return Err(reject);
    }
    if c.subdomain.is_some() && !c.advanced && !allowed.subdomain {
        return Err(reject);
    }
    if c.global_unicast_delay_requests.is_some()
        && !c.advanced
        && !allowed.global_unicast_delay_requests
    {
        return Err(reject);
    }
    if c.aggregate_ptpv1_unicast_delay_requests.is_some()
        && !c.advanced
        && !allowed.aggregate_ptpv1_unicast_delay_requests
    {
        return Err(reject);
    }
    if let Some(source) = c.clock_source {
        if crate::clock_configuration::clock_source_name(source).is_none()
            || (!c.advanced && source != 0 && !c.supported_clock_sources.contains(&source))
        {
            return Err(reject);
        }
    }
    let mut ports = std::collections::BTreeMap::new();
    for port in &c.ports {
        if !(1..=64).contains(&port.port_id) || ports.insert(port.port_id, port).is_some() {
            return Err(reject);
        }
    }
    let length = if rev == 0x0734 {
        40
    } else {
        ports
            .len()
            .checked_mul(12)
            .and_then(|size| size.checked_add(68))
            .ok_or(NetaudioError::PacketTooLarge)?
    };
    let mut record = vec![0u8; length];
    record[0..2].copy_from_slice(&rev.to_be_bytes());
    record[2..4].copy_from_slice(&0x0021u16.to_be_bytes());
    record[4..8].copy_from_slice(&100u32.to_be_bytes());
    let mut primary = 0u16;
    let mut extended = 0u16;
    let mut values = 0u16;
    if let Some(source) = c.clock_source {
        primary |= 1;
        record[10..12].copy_from_slice(&source.to_be_bytes());
    }
    if let Some(preferred) = c.preferred_leader {
        primary |= 2;
        record[12] = u8::from(preferred);
    }
    if let Some(name) = &c.subdomain {
        if name.len() > 16 || (name.len() == 16 && !name.contains(&0)) {
            return Err(reject);
        }
        let end = name.iter().position(|b| *b == 0).unwrap_or(name.len());
        if name[end..].iter().any(|b| *b != 0) {
            return Err(reject);
        }
        primary |= 8;
        record[16..16 + name.len()].copy_from_slice(name);
    }
    for (bit, value) in [
        (1, c.global_unicast_delay_requests),
        (0x0200, c.aggregate_ptpv1_unicast_delay_requests),
        (0x0400, c.aggregate_ptpv2_unicast_delay_requests),
        (0x0004, c.follower_only),
        (0x0008, c.ptpv1_enabled),
        (0x0010, c.ptpv2_enabled),
    ] {
        if let Some(value) = value {
            extended |= bit;
            if value {
                values |= bit;
            }
        }
    }
    record[8..10].copy_from_slice(&primary.to_be_bytes());
    record[32..34].copy_from_slice(&extended.to_be_bytes());
    record[34..36].copy_from_slice(&values.to_be_bytes());

    if rev == 0x073a {
        let mut mask = 0u32;
        for (bit, offset, value) in [
            (0, 52, c.priority_mapping),
            (1, 53, c.preferred_protocol),
            (2, 55, c.ptpv2_clock_class),
            (3, 54, c.ptpv2_domain),
            (4, 56, c.ptpv2_priority1),
            (5, 57, c.ptpv2_priority2),
            (6, 64, c.multicast_dscp),
        ] {
            if let Some(value) = value {
                mask |= 1 << bit;
                record[offset] = value;
            }
        }
        record[40..44].copy_from_slice(&mask.to_be_bytes());

        if !ports.is_empty() {
            record[58..60].copy_from_slice(&(ports.len() as u16).to_be_bytes());
            record[60..62].copy_from_slice(&68u16.to_be_bytes());
            record[62..64].copy_from_slice(&12u16.to_be_bytes());
        }

        for (index, port) in ports.values().enumerate() {
            let p = 68 + index * 12;
            let mut mask = 1u16;
            record[p + 2..p + 4].copy_from_slice(&port.port_id.to_be_bytes());
            for (bit, offset, value) in [
                (2, 4, port.ttl),
                (4, 5, port.sync_interval.map(|v| v as u8)),
                (8, 6, port.announce_interval.map(|v| v as u8)),
                (16, 7, port.delay_request_interval.map(|v| v as u8)),
                (32, 8, port.peer_delay_interval.map(|v| v as u8)),
                (64, 9, port.delay_mechanism),
            ] {
                if let Some(value) = value {
                    mask |= bit;
                    record[p + offset] = value;
                }
            }
            if let Some(value) = port.follower_only {
                record[p + 10] = u8::from(value);
                record[p + 11] = 1;
            }
            record[p..p + 2].copy_from_slice(&mask.to_be_bytes());
        }
    }
    let mut body = vec![0, 0];
    body.extend_from_slice(&mac);
    body.extend_from_slice(&[0, 0]);
    body.extend_from_slice(MAGIC_VENDOR);
    body.extend_from_slice(&record);
    ConmonHeader {
        message_id,
        protocol_id: PROTOCOL_SETTINGS,
    }
    .packet(&body)
}

pub fn build_refresh_clock_status(
    revision: u16,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    build_clock_control(
        &ClockControl {
            control_profile: revision,
            ..Default::default()
        },
        mac,
        message_id,
    )
}
