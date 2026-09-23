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
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockControl {
    pub record_revision: u16,
    pub clock_capabilities: Option<u16>,
    pub extension_flags: Option<u16>,
    pub clock_source: Option<u16>,
    pub preferred_leader: Option<bool>,
    pub subdomain: Option<Vec<u8>>,
    pub global_unicast_delay_requests: Option<bool>,
    pub aggregate_ptpv1_unicast_delay_requests: Option<bool>,
    #[serde(default)]
    pub supported_clock_sources: Vec<u16>,
}

impl ClockControl {
    pub fn availability(&self) -> ClockControlAvailability {
        let rev = self.record_revision;

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
            global_unicast_delay_requests: rev >= 0x0603 && caps & 8 != 0 && caps & 0x0200 == 0,
            aggregate_ptpv1_unicast_delay_requests: rev >= 0x071f && caps & 0x0200 != 0,
        }
    }

    pub fn is_query(&self) -> bool {
        self.clock_source.is_none()
            && self.preferred_leader.is_none()
            && self.subdomain.is_none()
            && self.global_unicast_delay_requests.is_none()
            && self.aggregate_ptpv1_unicast_delay_requests.is_none()
    }
}

pub fn build_clock_control(
    control: &ClockControl,
    mac: [u8; 6],
    message_id: u16,
) -> Result<Vec<u8>, NetaudioError> {
    let c = control;
    let rev = c.record_revision;
    let reject = NetaudioError::UnsupportedProtocolOperation;
    let allowed = c.availability();
    if !allowed.clock_source {
        return Err(reject);
    }
    if c.preferred_leader.is_some() && !allowed.preferred_leader {
        return Err(reject);
    }
    if c.subdomain.is_some() && !allowed.subdomain {
        return Err(reject);
    }
    if c.global_unicast_delay_requests.is_some() && !allowed.global_unicast_delay_requests {
        return Err(reject);
    }
    if c.aggregate_ptpv1_unicast_delay_requests.is_some()
        && !allowed.aggregate_ptpv1_unicast_delay_requests
    {
        return Err(reject);
    }
    if let Some(source) = c.clock_source {
        if crate::clock_configuration::clock_source_name(source).is_none()
            || (source != 0 && !c.supported_clock_sources.contains(&source))
        {
            return Err(reject);
        }
    }
    let mut record = vec![
        0u8;
        if (0x0730..0x0740).contains(&rev) {
            68
        } else {
            40
        }
    ];
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
            record_revision: revision,
            ..Default::default()
        },
        mac,
        message_id,
    )
}
