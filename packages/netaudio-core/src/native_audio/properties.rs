use serde::Serialize;

use crate::responses::DeviceSettings;

pub mod identifier {
    pub const SOURCE_DEFAULT_FRAMES_PER_PACKET: u16 = 0x0210;
    pub const UNICAST_FRAMES_PER_PACKET: u16 = 0x0211;
    pub const SOURCE_MINIMUM_FRAMES_PER_PACKET: u16 = 0x0213;
    pub const SOURCE_MAXIMUM_FRAMES_PER_PACKET: u16 = 0x0214;
    pub const RECEIVE_FRAMES_PER_PACKET: u16 = 0x0310;
    pub const UNICAST_LATENCY: u16 = 0x8205;
    pub const RECEIVE_LATENCY: u16 = 0x8301;
    pub const CLOCK_DOMAIN: u16 = 0x8021;
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PropertyObservation<T> {
    Reported { value: T },
    Unavailable,
    UnqualifiedStatus { code: u16 },
    Missing,
    Ambiguous,
}

#[derive(Clone, Debug, Serialize)]
pub struct AudioProperties {
    pub source_default_frames_per_packet: PropertyObservation<u16>,
    pub configured_unicast_frames_per_packet: PropertyObservation<u16>,
    pub source_lower_frames_per_packet: PropertyObservation<u16>,
    pub source_upper_frames_per_packet: PropertyObservation<u16>,
    pub configured_receive_frames_per_packet: PropertyObservation<u16>,
    pub configured_unicast_latency_nanoseconds: PropertyObservation<u32>,
    pub configured_receive_latency_nanoseconds: PropertyObservation<u32>,
    pub named_clock_domain_hexadecimal: PropertyObservation<String>,
}

fn observation<T>(
    settings: &DeviceSettings,
    property_id: u16,
    value: Option<T>,
) -> PropertyObservation<T> {
    if settings.ambiguous_property_ids.contains(&property_id) {
        PropertyObservation::Ambiguous
    } else if let Some(value) = value {
        PropertyObservation::Reported { value }
    } else if let Some(status) = settings
        .property_statuses
        .iter()
        .find(|status| status.property_id == property_id)
    {
        if status.status_code == 0 {
            PropertyObservation::Unavailable
        } else {
            PropertyObservation::UnqualifiedStatus {
                code: status.status_code,
            }
        }
    } else {
        PropertyObservation::Missing
    }
}

impl AudioProperties {
    pub fn from_settings(settings: &DeviceSettings) -> Self {
        let inline = |property_id| {
            observation(
                settings,
                property_id,
                settings
                    .inline_values
                    .iter()
                    .find(|entry| entry.info_code == property_id)
                    .map(|entry| entry.value),
            )
        };
        Self {
            source_default_frames_per_packet: inline(identifier::SOURCE_DEFAULT_FRAMES_PER_PACKET),
            configured_unicast_frames_per_packet: inline(identifier::UNICAST_FRAMES_PER_PACKET),
            source_lower_frames_per_packet: inline(identifier::SOURCE_MINIMUM_FRAMES_PER_PACKET),
            source_upper_frames_per_packet: inline(identifier::SOURCE_MAXIMUM_FRAMES_PER_PACKET),
            configured_receive_frames_per_packet: inline(identifier::RECEIVE_FRAMES_PER_PACKET),
            configured_unicast_latency_nanoseconds: observation(
                settings,
                identifier::UNICAST_LATENCY,
                settings.configured_latency_ns,
            ),
            configured_receive_latency_nanoseconds: observation(
                settings,
                identifier::RECEIVE_LATENCY,
                settings.active_latency_ns,
            ),
            named_clock_domain_hexadecimal: observation(
                settings,
                identifier::CLOCK_DOMAIN,
                settings
                    .referenced_values
                    .iter()
                    .find(|entry| entry.info_code == identifier::CLOCK_DOMAIN)
                    .map(|entry| entry.value_hexadecimal.clone()),
            ),
        }
    }
}
