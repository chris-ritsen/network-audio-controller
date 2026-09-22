use std::collections::{BTreeMap, HashSet};
use std::net::Ipv4Addr;
use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MediaMode {
    Unknown,
    NativeDante,
    RtpAes67,
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlowType {
    Unicast,
    Multicast,
}

impl FlowType {
    pub(crate) fn may_retire_after_sample_rate_change(self, modern: bool) -> bool {
        modern && matches!(self, Self::Unicast)
    }
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ChannelSlot {
    pub slot: NonZeroU16,
    pub transmitter_channel: NonZeroU16,
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Destination {
    pub address: Ipv4Addr,
    pub port: NonZeroU16,
    pub interface: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Destination {
    pub fn validate(&self, flow_type: FlowType) -> Result<(), String> {
        if self.address.is_unspecified() || self.address.is_broadcast() {
            return Err("flow destination must not be unspecified or limited broadcast".into());
        }

        if matches!(flow_type, FlowType::Multicast) != self.address.is_multicast() {
            return Err("flow destination does not match its unicast or multicast type".into());
        }

        if self
            .interface
            .as_ref()
            .is_some_and(|value| value.is_empty())
        {
            return Err("flow destination interface must not be empty".into());
        }

        Ok(())
    }
}

pub fn validate_slots(slots: &[ChannelSlot]) -> Result<(), String> {
    if slots.is_empty() {
        return Err("channel_slots must be a non-empty ordered list".into());
    }

    if slots.windows(2).any(|pair| pair[0].slot >= pair[1].slot) {
        return Err("flow channel slots must be unique and strictly ascending".into());
    }

    let mut channels = HashSet::new();

    if slots
        .iter()
        .any(|slot| !channels.insert(slot.transmitter_channel))
    {
        return Err("transmitter channels must not contain duplicates".into());
    }

    Ok(())
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RedundancyConstraint {
    #[default]
    DeviceDefault,
    None,
    Optional,
    Required,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowIdentity {
    pub global_flow_id: Option<NonZeroU16>,
    pub media_type_code: Option<u16>,
    pub media_local_flow_id: Option<NonZeroU16>,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProtocolRequirements {
    pub protocol_id: Option<u16>,
    pub protocol_version: Option<String>,
    pub cohort: Option<String>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
}

fn schema_version() -> u32 {
    1
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TransmitFlowSpecification {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub media_mode: MediaMode,
    pub flow_type: FlowType,
    pub channel_slots: Vec<ChannelSlot>,
    pub name: Option<String>,
    pub sample_rate_hz: Option<NonZeroU32>,
    pub encoding_bits: Option<NonZeroU16>,
    pub frames_per_packet: Option<NonZeroU16>,
    pub primary_destination: Option<Destination>,
    pub secondary_destination: Option<Destination>,
    #[serde(default)]
    pub redundancy: RedundancyConstraint,
    #[serde(default)]
    pub identity: FlowIdentity,
    #[serde(default)]
    pub protocol: ProtocolRequirements,
    #[serde(default)]
    pub raw_fields: BTreeMap<String, Value>,
}

impl TransmitFlowSpecification {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != schema_version() {
            return Err("unsupported transmit flow schema_version".into());
        }

        validate_slots(&self.channel_slots)?;

        if self
            .name
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.contains('\0'))
        {
            return Err("flow name must be non-empty and must not contain NUL".into());
        }

        for value in [&self.protocol.protocol_version, &self.protocol.cohort] {
            if value.as_ref().is_some_and(|text| text.is_empty()) {
                return Err("flow protocol version and cohort must not be empty".into());
            }
        }

        let mut capabilities = HashSet::new();

        if self
            .protocol
            .required_capabilities
            .iter()
            .any(|item| item.is_empty() || !capabilities.insert(item))
        {
            return Err("required_capabilities must contain unique non-empty strings".into());
        }

        if self.secondary_destination.is_some() && self.primary_destination.is_none() {
            return Err("secondary destination requires a primary destination".into());
        }

        match self.redundancy {
            RedundancyConstraint::None if self.secondary_destination.is_some() => {
                return Err("redundancy none forbids a secondary destination".into());
            }
            RedundancyConstraint::Required if self.secondary_destination.is_none() => {
                return Err("redundancy required needs a secondary destination".into());
            }
            _ => {}
        }

        for destination in [&self.primary_destination, &self.secondary_destination]
            .into_iter()
            .flatten()
        {
            destination.validate(self.flow_type)?;
        }

        if let (Some(primary), Some(secondary)) =
            (&self.primary_destination, &self.secondary_destination)
        {
            if primary.address == secondary.address
                && primary.port == secondary.port
                && primary.interface == secondary.interface
            {
                return Err("primary and secondary destinations must differ".into());
            }

            if primary.interface.is_some() && primary.interface == secondary.interface {
                return Err(
                    "primary and secondary destinations must use different interfaces".into(),
                );
            }
        }

        Ok(())
    }
}
