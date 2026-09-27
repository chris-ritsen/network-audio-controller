use std::collections::{BTreeMap, HashSet};
use std::num::{NonZeroU16, NonZeroU32};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::flow_specification::{
    validate_slots, ChannelSlot, Destination, FlowIdentity, FlowType, MediaMode,
    ProtocolRequirements, RedundancyConstraint, TransmitFlowSpecification,
};

pub fn format_precondition(
    request: crate::responses::AudioCapabilityReadback,
) -> crate::responses::AudioReadbackResult {
    let mut result = request.resolve();

    // Unlike some audio controls, a flow's sample rate and encoding cannot be zero.
    if result.current_value == Some(0) {
        result.current_value = None;
        result.state = crate::responses::AudioReadbackState::Unavailable;
        result.effective_state_confirmed = false;
    }

    result
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowReadbackRequest {
    pub protocol_id: u16,
    pub record: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowComparisonRequest {
    pub requested: serde_json::Map<String, Value>,
    pub effective: serde_json::Map<String, Value>,
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowDifference {
    pub field: String,
    pub requested: Value,
    pub effective: Value,
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowComparison {
    pub matches: bool,
    pub differences: Vec<FlowDifference>,
    pub unavailable_fields: Vec<String>,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlowMutation {
    Create,
    Delete,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowVerificationRequest {
    pub operation: FlowMutation,
    pub record_present: bool,
    pub comparison: Option<FlowComparison>,
    pub authoring_refreshed: bool,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlowVerificationOutcome {
    Confirmed,
    Contradiction,
    PartiallyObserved,
    NotYetVisible,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowVerification {
    pub outcome: FlowVerificationOutcome,
    pub unavailable_fields: Vec<String>,
    pub authoring_refresh_missing: bool,
}

pub fn verification(request: FlowVerificationRequest) -> FlowVerification {
    use FlowVerificationOutcome::*;
    let comparison = request.comparison.filter(|_| request.record_present);
    let differences = comparison
        .as_ref()
        .is_some_and(|value| !value.differences.is_empty());
    let unavailable_fields = comparison
        .as_ref()
        .map(|value| value.unavailable_fields.clone())
        .unwrap_or_default();
    let effective = match request.operation {
        FlowMutation::Create => {
            comparison.as_ref().is_some_and(|value| value.matches)
                && !differences
                && unavailable_fields.is_empty()
        }
        FlowMutation::Delete => !request.record_present,
    };
    let authoring_refresh_missing = effective && !request.authoring_refreshed;
    let outcome = if differences {
        Contradiction
    } else if effective && request.authoring_refreshed {
        Confirmed
    } else if authoring_refresh_missing || !unavailable_fields.is_empty() {
        PartiallyObserved
    } else {
        NotYetVisible
    };
    FlowVerification {
        outcome,
        unavailable_fields,
        authoring_refresh_missing,
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowInventoryEvidence {
    pub page_disposition: Option<String>,
    pub reported_flow_count: Option<usize>,
    pub flows: Vec<serde_json::Map<String, Value>>,
}

fn record_identity(record: &serde_json::Map<String, Value>) -> Option<u16> {
    record
        .get("global_flow_id")
        .or_else(|| record.get("flow_number"))?
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| *value != 0)
}

impl FlowInventoryEvidence {
    fn unambiguous(&self) -> bool {
        if self
            .reported_flow_count
            .is_some_and(|count| count != self.flows.len())
        {
            return false;
        }
        if self
            .page_disposition
            .as_ref()
            .is_some_and(|value| value != "complete")
        {
            return false;
        }
        let mut identities = HashSet::new();
        self.flows
            .iter()
            .all(|record| record_identity(record).is_some_and(|id| identities.insert(id)))
    }
}

pub fn inventory_complete(value: Value) -> bool {
    serde_json::from_value::<FlowInventoryEvidence>(value)
        .is_ok_and(|inventory| inventory.unambiguous())
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowCreatePreflightRequest {
    inventory: Value,
    requested_flow_id: Option<NonZeroU16>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlowPreflightOutcome {
    Ready,
    Unavailable,
    Rejected,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowCreatePreflight {
    state: FlowPreflightOutcome,
    reason: Option<String>,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowDeletePreflightRequest {
    inventory: Value,
    flow_id: NonZeroU16,
    protocol_id: u16,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowDeletePreflight {
    state: FlowPreflightOutcome,
    reason: Option<String>,
    specification: Option<ObservedTransmitFlowSpecification>,
}

pub fn delete_preflight(request: FlowDeletePreflightRequest) -> FlowDeletePreflight {
    let inventory = serde_json::from_value::<FlowInventoryEvidence>(request.inventory).ok();
    let Some(inventory) = inventory.filter(FlowInventoryEvidence::unambiguous) else {
        return FlowDeletePreflight {
            state: FlowPreflightOutcome::Unavailable,
            reason: Some("fresh preflight inventory was unavailable".into()),
            specification: None,
        };
    };
    let rejected = |reason| FlowDeletePreflight {
        state: FlowPreflightOutcome::Rejected,
        reason: Some(reason),
        specification: None,
    };
    let Some(record) = inventory
        .flows
        .into_iter()
        .find(|record| record_identity(record) == Some(request.flow_id.get()))
    else {
        return rejected(format!("flow {} is not active", request.flow_id));
    };

    if record.get("flow_type").and_then(Value::as_str) != Some("multicast") {
        return rejected("only multicast transmit-flow deletion is supported".into());
    }

    match transmit_flow_specification(&FlowReadbackRequest {
        protocol_id: request.protocol_id,
        record: Value::Object(record),
    }) {
        Ok(specification) => FlowDeletePreflight {
            state: FlowPreflightOutcome::Ready,
            reason: None,
            specification: Some(specification),
        },
        Err(reason) => rejected(reason),
    }
}

pub fn create_preflight(request: FlowCreatePreflightRequest) -> FlowCreatePreflight {
    let capacity = request
        .inventory
        .get("maximum_flow_slots")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok());
    let inventory = serde_json::from_value::<FlowInventoryEvidence>(request.inventory).ok();
    let Some((capacity, inventory)) = capacity
        .zip(inventory)
        .filter(|(_, inventory)| inventory.unambiguous())
    else {
        return FlowCreatePreflight {
            state: FlowPreflightOutcome::Unavailable,
            reason: Some("fresh preflight inventory or capacity was unavailable".into()),
        };
    };
    let requested = request.requested_flow_id.map(NonZeroU16::get);
    let reason = if inventory.flows.len() >= usize::from(capacity) {
        Some("all transmitter flow slots are in use".into())
    } else if let Some(id) = requested.filter(|id| *id > capacity) {
        Some(format!(
            "flow {id} exceeds the device capacity of {capacity}"
        ))
    } else {
        requested
            .filter(|id| {
                inventory
                    .flows
                    .iter()
                    .any(|record| record_identity(record) == Some(*id))
            })
            .map(|id| format!("flow {id} is already active"))
    };

    FlowCreatePreflight {
        state: if reason.is_some() {
            FlowPreflightOutcome::Rejected
        } else {
            FlowPreflightOutcome::Ready
        },
        reason,
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowTopologyChangeRequest {
    pub before: FlowInventoryEvidence,
    pub after: FlowInventoryEvidence,
    pub protocol_id: u16,
    pub target_flow_id: Option<u16>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowTopologyChange {
    pub before: Vec<serde_json::Map<String, Value>>,
    pub after: Vec<serde_json::Map<String, Value>>,
}

fn stable_inventory(
    inventory: &FlowInventoryEvidence,
    protocol_id: u16,
    excluded: Option<u16>,
) -> Vec<serde_json::Map<String, Value>> {
    let mut projected = Vec::new();
    for record in &inventory.flows {
        let identity = record_identity(record);
        if excluded.is_some() && identity == excluded {
            continue;
        }

        let specification = transmit_flow_specification(&FlowReadbackRequest {
            protocol_id,
            record: Value::Object(record.clone()),
        });
        let mut stable = serde_json::Map::new();
        match specification {
            Ok(value) => {
                let value = serde_json::to_value(value).expect("flow specifications serialize");
                for field in [
                    "identity",
                    "media_mode",
                    "flow_type",
                    "name",
                    "channel_slots",
                    "sample_rate_hz",
                    "encoding_bits",
                    "frames_per_packet",
                    "primary_destination",
                    "secondary_destination",
                    "redundancy",
                ] {
                    stable.insert(field.into(), value[field].clone());
                }
                let protocol = &value["protocol"];
                stable.insert("protocol".into(), json!({"protocol_id": protocol["protocol_id"], "protocol_version": protocol["protocol_version"], "cohort": protocol["cohort"]}));
                stable.insert("parsed".into(), json!(true));
            }
            Err(_) => {
                // Unknown record payloads cannot establish durable configuration equality.
                stable.insert("identity".into(), json!({"global_flow_id": identity}));
                stable.insert("parsed".into(), json!(false));
            }
        }
        projected.push(stable);
    }
    projected
        .sort_by_cached_key(|value| serde_json::to_string(value).expect("JSON values serialize"));
    projected
}

pub fn topology_change(
    request: FlowTopologyChangeRequest,
) -> Result<Option<FlowTopologyChange>, String> {
    if !request.before.unambiguous() || !request.after.unambiguous() {
        return Err("complete, unambiguous flow inventories are required".into());
    }
    let before = stable_inventory(&request.before, request.protocol_id, request.target_flow_id);
    let after = stable_inventory(&request.after, request.protocol_id, request.target_flow_id);
    Ok((before != after).then_some(FlowTopologyChange { before, after }))
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowCandidateRequest {
    pub before: FlowInventoryEvidence,
    pub after: FlowInventoryEvidence,
    pub requested: TransmitFlowSpecification,
    pub protocol_id: u16,
    pub correlated_flow_id: Option<u16>,
}

#[derive(Default, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowCandidate {
    pub flow_id: Option<u16>,
    pub record: Option<serde_json::Map<String, Value>>,
    pub comparison: Option<FlowComparison>,
}

pub fn creation_candidate(request: FlowCandidateRequest) -> Result<FlowCandidate, String> {
    request.requested.validate()?;
    let absent = || FlowCandidate {
        flow_id: request.correlated_flow_id,
        ..Default::default()
    };
    if !request.before.unambiguous() || !request.after.unambiguous() {
        return Ok(absent());
    }
    let requested = serde_json::to_value(&request.requested).map_err(|error| error.to_string())?;
    let requested = requested
        .as_object()
        .ok_or("flow specification must be an object")?;
    let before_ids: HashSet<_> = request
        .before
        .flows
        .iter()
        .filter_map(record_identity)
        .collect();
    let local_id = request
        .requested
        .identity
        .media_local_flow_id
        .map(|value| u64::from(value.get()));
    let mut candidates = Vec::new();

    for record in &request.after.flows {
        let Some(id) = record_identity(record) else {
            continue;
        };
        if let Some(correlated) = request.correlated_flow_id {
            if id != correlated {
                continue;
            }
        } else if before_ids.contains(&id)
            || local_id.is_some_and(|local| {
                record.get("media_local_flow_id").and_then(Value::as_u64) != Some(local)
            })
        {
            continue;
        }
        let effective = match transmit_flow_specification(&FlowReadbackRequest {
            protocol_id: request.protocol_id,
            record: Value::Object(record.clone()),
        }) {
            Ok(value) => value,
            Err(error) if request.correlated_flow_id.is_some() => return Err(error),
            Err(_) => continue,
        };
        let comparison = compare_transmit_flows(&FlowComparisonRequest {
            requested: requested.clone(),
            effective: serde_json::to_value(effective)
                .map_err(|error| error.to_string())?
                .as_object()
                .ok_or("effective flow specification must be an object")?
                .clone(),
        });
        if request.correlated_flow_id.is_some() || local_id.is_some() || comparison.matches {
            candidates.push(FlowCandidate {
                flow_id: Some(id),
                record: Some(record.clone()),
                comparison: Some(comparison),
            });
        }
    }

    Ok(if candidates.len() == 1 {
        candidates.pop().expect("one candidate")
    } else {
        absent()
    })
}

fn field_value<'a>(
    specification: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Option<&'a Value> {
    match field.split_once('.') {
        Some((parent, child)) => specification.get(parent)?.get(child),
        None => specification.get(field),
    }
}

fn comparison_value(specification: &serde_json::Map<String, Value>, field: &str) -> Option<Value> {
    let value = field_value(specification, field)?;

    if value.is_null() {
        return Some(Value::Null);
    }

    if field == "media_mode" {
        return serde_json::to_value(serde_json::from_value::<MediaMode>(value.clone()).ok()?).ok();
    }

    if field == "flow_type" {
        return serde_json::to_value(serde_json::from_value::<FlowType>(value.clone()).ok()?).ok();
    }

    if field == "channel_slots" {
        let mut slots: Vec<ChannelSlot> = serde_json::from_value(value.clone()).ok()?;
        validate_slots(&slots).ok()?;

        for slot in &mut slots {
            slot.extra.clear();
        }

        return serde_json::to_value(slots).ok();
    }

    if matches!(field, "primary_destination" | "secondary_destination") {
        let destination: Destination = serde_json::from_value(value.clone()).ok()?;
        let flow_type = serde_json::from_value(specification.get("flow_type")?.clone()).ok()?;
        destination.validate(flow_type).ok()?;

        return Some(
            json!({"address": destination.address, "port": destination.port,
            "interface": destination.interface}),
        );
    }

    if field == "name" {
        let name = value.as_str()?;

        return (!name.is_empty() && !name.contains('\0')).then(|| value.clone());
    }

    if field == "redundancy" {
        return matches!(
            value.as_str()?,
            "device_default" | "none" | "optional" | "required"
        )
        .then(|| value.clone());
    }

    let number = value.as_u64()?;
    let maximum = if field == "sample_rate_hz" {
        u32::MAX as u64
    } else {
        u16::MAX as u64
    };

    (number <= maximum && (number > 0 || field == "identity.media_type_code"))
        .then(|| value.clone())
}

pub fn compare_transmit_flows(request: &FlowComparisonRequest) -> FlowComparison {
    let mut differences = Vec::new();
    let mut unavailable = Vec::new();
    let inventory_evidence = request
        .effective
        .get("raw_fields")
        .and_then(Value::as_object)
        .is_some_and(|fields| !fields.is_empty());
    let observed = request
        .effective
        .get("observed_fields")
        .and_then(Value::as_array);

    for (field, optional) in [
        ("media_mode", false),
        ("flow_type", false),
        ("name", true),
        ("channel_slots", false),
        ("sample_rate_hz", true),
        ("encoding_bits", true),
        ("frames_per_packet", true),
        ("primary_destination", true),
        ("secondary_destination", true),
        ("redundancy", true),
        ("identity.global_flow_id", true),
        ("identity.media_type_code", true),
        ("identity.media_local_flow_id", true),
        ("protocol.protocol_id", true),
    ] {
        if optional && field_value(&request.requested, field).is_none_or(Value::is_null) {
            continue;
        }

        let requested = comparison_value(&request.requested, field);

        if field == "redundancy"
            && requested.as_ref().and_then(Value::as_str) == Some("device_default")
        {
            continue;
        }

        let effective = comparison_value(&request.effective, field);
        let field_observed = !inventory_evidence
            || observed.is_some_and(|fields| fields.iter().any(|value| value == field));

        if !field_observed
            || requested.is_none()
            || effective.is_none()
            || (!optional
                && (requested.as_ref().is_some_and(Value::is_null)
                    || effective.as_ref().is_some_and(Value::is_null)))
        {
            unavailable.push(field.to_owned());
        } else if requested != effective {
            differences.push(FlowDifference {
                field: field.to_owned(),
                requested: requested.unwrap_or(Value::Null),
                effective: effective.unwrap_or(Value::Null),
            });
        }
    }

    FlowComparison {
        matches: differences.is_empty() && unavailable.is_empty(),
        differences,
        unavailable_fields: unavailable,
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowTopology {
    pub flow_number: NonZeroU16,
    pub flow_type: FlowType,
    pub channel_count: NonZeroU16,
    pub channel_members: Vec<u16>,
    pub sample_rate_hertz: NonZeroU32,
    pub encoding: NonZeroU16,
    pub frames_per_packet: Option<NonZeroU16>,
    pub may_retire_after_sample_rate_change: bool,
}

#[derive(Deserialize)]
struct InventoryRecord {
    inventory_layout: Option<String>,
    flow_type: FlowType,
    media_mode: Option<MediaMode>,
    flow_name: Option<String>,
    transmitter_channel_ids_by_slot: Option<Vec<u16>>,
    channels: Option<Vec<u16>>,
    channel_count: Option<NonZeroU16>,
    channel_slot_count: Option<NonZeroU16>,
    sample_rate: Option<NonZeroU32>,
    encoding: Option<NonZeroU16>,
    frames_per_packet: Option<NonZeroU16>,
    global_flow_id: Option<NonZeroU16>,
    flow_number: Option<NonZeroU16>,
    media_type_code: Option<u16>,
    media_local_flow_id: Option<NonZeroU16>,
    primary_destination: Option<Destination>,
    secondary_destination: Option<Destination>,
    destination_internet_protocol_version_four_address: Option<String>,
    destination_user_datagram_port: Option<u16>,
}

pub(crate) fn readback_protocol(protocol_id: u16) -> Result<(&'static str, bool), String> {
    use crate::commands::{PROTOCOL_DANTE_FLOW, PROTOCOL_DANTE_FLOW_2801};
    use crate::protocol::{PROTOCOL_ARC_2809, PROTOCOL_ARC_280F};

    Ok(match protocol_id {
        PROTOCOL_DANTE_FLOW => ("legacy_2729", false),
        PROTOCOL_DANTE_FLOW_2801 => ("legacy_2801", false),
        PROTOCOL_ARC_2809 => ("modern_2809", true),
        PROTOCOL_ARC_280F => ("modern_280f", true),
        _ => return Err("unsupported transmitter flow readback protocol".into()),
    })
}

impl FlowReadbackRequest {
    fn record(&self) -> Result<InventoryRecord, String> {
        serde_json::from_value(self.record.clone())
            .map_err(|error| format!("invalid transmitter flow record: {error}"))
    }
}

pub fn transmit_flow_topology(request: &FlowReadbackRequest) -> Result<FlowTopology, String> {
    let (_, modern) = readback_protocol(request.protocol_id)?;
    let record = request.record()?;
    let modern = modern && record.inventory_layout.as_deref() != Some("fixed");
    let (identifier, count, members) = if modern {
        if record.media_type_code != Some(3) {
            return Err("transmitter flow is not a supported audio flow".into());
        }

        (
            record.global_flow_id,
            record.channel_slot_count,
            record.transmitter_channel_ids_by_slot,
        )
    } else {
        (record.flow_number, record.channel_count, record.channels)
    };
    let identifier = identifier.ok_or("transmitter flow identifier is unavailable")?;
    let count = count.ok_or("transmitter flow channel count is unavailable")?;
    let members = members.ok_or("transmitter flow members are unavailable")?;

    if (modern || matches!(record.flow_type, FlowType::Multicast))
        && members.len() != usize::from(count.get())
    {
        return Err("transmitter flow member count does not match its channel count".into());
    }

    let mut seen = HashSet::new();

    if members
        .iter()
        .filter(|member| **member != 0)
        .any(|member| !seen.insert(member))
    {
        return Err("transmitter flow channel members contain duplicates".into());
    }

    let sample_rate = record
        .sample_rate
        .ok_or("transmitter flow sample rate is unavailable")?;
    let encoding = record
        .encoding
        .ok_or("transmitter flow encoding is unavailable")?;
    let frames_per_packet = if modern {
        None
    } else {
        Some(
            record
                .frames_per_packet
                .ok_or("transmitter flow frames per packet is unavailable")?,
        )
    };

    Ok(FlowTopology {
        flow_number: identifier,
        flow_type: record.flow_type,
        channel_count: count,
        channel_members: members,
        sample_rate_hertz: sample_rate,
        encoding,
        frames_per_packet,
        may_retire_after_sample_rate_change: record
            .flow_type
            .may_retire_after_sample_rate_change(modern),
    })
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ObservedTransmitFlowSpecification {
    #[serde(flatten)]
    pub specification: TransmitFlowSpecification,
    pub observed_fields: Vec<&'static str>,
}

pub fn transmit_flow_specification(
    request: &FlowReadbackRequest,
) -> Result<ObservedTransmitFlowSpecification, String> {
    let (cohort, _) = readback_protocol(request.protocol_id)?;
    let record = request.record()?;
    let channels = record
        .transmitter_channel_ids_by_slot
        .or(record.channels)
        .ok_or("transmitter flow inventory has no ordered channel-slot mapping")?;
    let mut populated = Vec::new();

    for (index, channel) in channels.iter().enumerate() {
        let slot = u16::try_from(index + 1)
            .map_err(|_| "flow channel slot exceeds its supported range")?;

        let Some(channel) = NonZeroU16::new(*channel) else {
            continue;
        };

        populated.push(ChannelSlot {
            slot: NonZeroU16::new(slot).ok_or("invalid flow channel slot")?,
            transmitter_channel: channel,
            extra: Default::default(),
        });
    }

    let primary = match record.primary_destination {
        Some(destination) => Some(destination),
        None => match (
            record.destination_internet_protocol_version_four_address,
            record
                .destination_user_datagram_port
                .and_then(NonZeroU16::new),
        ) {
            (Some(address), Some(port)) if !address.is_empty() => Some(Destination {
                address: address
                    .parse()
                    .map_err(|_| "invalid flow destination IPv4 address")?,
                port,
                interface: None,
                extra: BTreeMap::new(),
            }),
            _ => None,
        },
    };

    let global_identifier = if request.record.get("global_flow_id").is_some() {
        record.global_flow_id
    } else {
        record.flow_number
    };

    let media_mode = record.media_mode.unwrap_or(MediaMode::Unknown);
    let observed_fields: Vec<&str> = [
        ("media_mode", !matches!(media_mode, MediaMode::Unknown)),
        ("flow_type", true),
        ("channel_slots", true),
        ("name", record.flow_name.is_some()),
        ("sample_rate_hz", record.sample_rate.is_some()),
        ("encoding_bits", record.encoding.is_some()),
        ("frames_per_packet", record.frames_per_packet.is_some()),
        ("primary_destination", primary.is_some()),
        (
            "secondary_destination",
            request.record.get("secondary_destination").is_some(),
        ),
        ("identity.global_flow_id", global_identifier.is_some()),
        ("identity.media_type_code", record.media_type_code.is_some()),
        (
            "identity.media_local_flow_id",
            record.media_local_flow_id.is_some(),
        ),
        ("protocol.protocol_id", true),
    ]
    .into_iter()
    .filter_map(|(field, known)| known.then_some(field))
    .collect();

    let specification = TransmitFlowSpecification {
        schema_version: 1,
        media_mode,
        flow_type: record.flow_type,
        name: record.flow_name,
        channel_slots: populated,
        sample_rate_hz: record.sample_rate,
        encoding_bits: record.encoding,
        frames_per_packet: record.frames_per_packet,
        primary_destination: primary,
        secondary_destination: record.secondary_destination,
        redundancy: RedundancyConstraint::DeviceDefault,
        identity: FlowIdentity {
            global_flow_id: global_identifier,
            media_type_code: record.media_type_code,
            media_local_flow_id: record.media_local_flow_id,
            extra: Default::default(),
        },
        protocol: ProtocolRequirements {
            protocol_id: Some(request.protocol_id),
            protocol_version: None,
            cohort: Some(cohort.into()),
            required_capabilities: Vec::new(),
            extra: Default::default(),
        },
        raw_fields: serde_json::from_value(request.record.clone())
            .map_err(|error| error.to_string())?,
        extra: Default::default(),
    };
    specification.validate()?;
    Ok(ObservedTransmitFlowSpecification {
        specification,
        observed_fields,
    })
}
