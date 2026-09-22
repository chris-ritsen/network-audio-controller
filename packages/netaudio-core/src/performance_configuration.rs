use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::commands::{
    self, PropertyValue, PERFORMANCE_PROPERTY_IDS, PROPERTY_PRE_3_COMPATIBILITY,
};
use crate::protocol::NetaudioError;

#[derive(Clone, Copy, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PerformanceCompletionKind {
    Configuration,
    Storage,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PerformanceCompletionRequest {
    pub kind: PerformanceCompletionKind,
    #[serde(default)]
    pub requested: BTreeMap<u16, u32>,
    pub acknowledgement: Option<Vec<u8>>,
    pub readback: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PerformanceCompletionState {
    Rejected,
    Confirmed,
    Contradicted,
    RequestAcknowledged,
    Unverified,
}

#[derive(Clone, Copy, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PerformanceReadbackOutcome {
    NoResponse,
    Unparseable,
    Matched,
    Mismatch,
    Incomplete,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PerformanceCompletion {
    pub state: PerformanceCompletionState,
    pub readback_outcome: Option<PerformanceReadbackOutcome>,
    pub observed_properties: BTreeMap<u16, u32>,
    pub effective_properties: BTreeMap<u16, u32>,
    pub missing_property_ids: Vec<u16>,
    pub mismatched_properties: BTreeMap<u16, u32>,
    pub effective_state_confirmation: Option<bool>,
    pub persistence_confirmation: Option<bool>,
    pub message: String,
}

pub fn completion(
    request: PerformanceCompletionRequest,
) -> Result<PerformanceCompletion, crate::spec::SpecError> {
    use PerformanceCompletionState as State;
    use PerformanceReadbackOutcome as Outcome;

    let storage = matches!(request.kind, PerformanceCompletionKind::Storage);

    if (storage && (!request.requested.is_empty() || request.readback.is_some()))
        || (!storage && request.requested.is_empty())
    {
        return Err(crate::spec::SpecError::InvalidJson(
            "configuration requires requested properties; storage accepts neither properties nor readback".into(),
        ));
    }

    let accepted = request
        .acknowledgement
        .as_deref()
        .and_then(crate::responses::parse_command_acknowledgement)
        .map(|acknowledgement| acknowledgement.accepted);
    let mut result = PerformanceCompletion {
        state: match accepted {
            Some(false) => State::Rejected,
            Some(true) => State::RequestAcknowledged,
            None => State::Unverified,
        },
        readback_outcome: None,
        observed_properties: BTreeMap::new(),
        effective_properties: BTreeMap::new(),
        missing_property_ids: Vec::new(),
        mismatched_properties: BTreeMap::new(),
        effective_state_confirmation: None,
        persistence_confirmation: None,
        message: String::new(),
    };

    if storage {
        result.message = if accepted == Some(true) {
            "storage request acknowledged; persistence requires an independent signal or post-reboot readback"
        } else {
            "configuration storage was not acknowledged"
        }.into();
        return Ok(result);
    }

    let parsed = request
        .readback
        .as_deref()
        .and_then(crate::responses::parse_device_settings);
    let outcome = if let Some(settings) = parsed {
        result.observed_properties = settings
            .performance_values
            .into_iter()
            .map(|entry| (entry.property_id, entry.value))
            .collect();

        for (property, requested) in request.requested {
            match result.observed_properties.get(&property).copied() {
                Some(observed) => {
                    result.effective_properties.insert(property, observed);

                    if observed != requested {
                        result.mismatched_properties.insert(property, observed);
                    }
                }
                None => result.missing_property_ids.push(property),
            }
        }

        if !result.mismatched_properties.is_empty() {
            result.effective_state_confirmation = Some(false);
            Outcome::Mismatch
        } else if result.missing_property_ids.is_empty() {
            result.effective_state_confirmation = Some(true);
            Outcome::Matched
        } else {
            Outcome::Incomplete
        }
    } else if request.readback.is_some() {
        Outcome::Unparseable
    } else {
        Outcome::NoResponse
    };

    if accepted != Some(false) {
        match result.effective_state_confirmation {
            Some(true) => result.state = State::Confirmed,
            Some(false) => result.state = State::Contradicted,
            None => {}
        }
    }

    let description = match outcome {
        Outcome::NoResponse => "was unavailable",
        Outcome::Unparseable => "was not parseable",
        Outcome::Matched => "matched every affected property",
        Outcome::Mismatch => "found a correlated value mismatch",
        Outcome::Incomplete => "omitted one or more affected properties",
    };
    let unavailable = matches!(outcome, Outcome::NoResponse | Outcome::Unparseable);
    result.message = match (accepted, unavailable) {
        (Some(false), true) => format!("device rejected the performance request; fresh property readback {description}"),
        (Some(false), false) => format!("device rejected the performance request; fresh readback {description}"),
        (Some(true), true) => format!("request was acknowledged; fresh property readback {description}"),
        (None, true) => format!("request was sent once without an accepted acknowledgement; fresh readback {description}"),
        (None, false) if outcome == Outcome::Matched => format!("fresh readback {description}; request acknowledgement was not established"),
        _ => format!("fresh readback {description}"),
    };
    result.readback_outcome = Some(outcome);
    Ok(result)
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PerformanceFacts {
    protocol_id: Option<u16>,
    managed: bool,
    property_ids: Option<Vec<serde_json::Value>>,
    platform_software_version: Option<String>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PerformanceCapabilities {
    platform_software_version: Option<[u16; 3]>,
    supported_property_ids: Option<Vec<u16>>,
    operations: BTreeMap<&'static str, PerformanceAvailability>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
struct PerformanceAvailability {
    supported: bool,
    readable: bool,
    writable: bool,
    reasons: Vec<&'static str>,
}

impl PerformanceAvailability {
    fn new(readable: bool, reasons: Vec<&'static str>) -> Self {
        Self {
            supported: reasons.is_empty(),
            readable,
            writable: reasons.is_empty(),
            reasons,
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Receive,
    Transmit,
    Unicast,
    DefaultSlots,
}

const OPERATIONS: [(&str, Operation); 4] = [
    ("receive_flow_performance", Operation::Receive),
    ("transmit_flow_performance", Operation::Transmit),
    ("unicast_performance", Operation::Unicast),
    ("receive_flow_default_slots", Operation::DefaultSlots),
];

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PerformanceSnapshotFacts {
    property_ids: Vec<serde_json::Value>,
    values: BTreeMap<u16, serde_json::Value>,
}

fn known_property_ids(values: &[serde_json::Value]) -> Vec<u16> {
    values
        .iter()
        .filter_map(|value| value.as_u64().and_then(|value| u16::try_from(value).ok()))
        .filter(|value| PERFORMANCE_PROPERTY_IDS.contains(value))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn consistent_value(mut values: impl Iterator<Item = Option<u32>>) -> Option<u32> {
    let first = values.next()??;
    values.all(|value| value == Some(first)).then_some(first)
}

#[derive(Default, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PerformanceSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    receive_flow_performance: Option<PacketPerformance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transmit_flow_performance: Option<PacketPerformance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unicast_performance: Option<PacketPerformance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receive_flow_default_slots: Option<u16>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PacketPerformance {
    latency_microseconds: u32,
    frames_per_packet: u16,
}

pub fn snapshot(facts: PerformanceSnapshotFacts) -> PerformanceSnapshot {
    let supported = known_property_ids(&facts.property_ids);
    let mut configuration = PerformanceSnapshot::default();

    for (_, operation) in OPERATIONS {
        // Select the same control fields as writes, without software-compatibility writes.
        let Ok(properties) = operation.properties(&supported, [3, 0, 0]) else {
            continue;
        };
        let observed = |property: &u16| {
            facts
                .values
                .get(property)?
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
        };
        let inline_value = consistent_value(
            properties
                .iter()
                .filter(|(_, value)| matches!(value, PropertyValue::InlineU16(_)))
                .map(|(property, _)| observed(property)),
        )
        .and_then(|value| u16::try_from(value).ok());

        let Some(inline_value) = inline_value else {
            continue;
        };

        if matches!(operation, Operation::DefaultSlots) {
            configuration.receive_flow_default_slots = Some(inline_value);
            continue;
        }

        let latency_ns = consistent_value(
            properties
                .iter()
                .filter(|(_, value)| matches!(value, PropertyValue::ReferencedU32(_)))
                .map(|(property, _)| observed(property)),
        );

        if let Some(latency_ns) = latency_ns.filter(|value| value % 1_000 == 0) {
            let value = Some(PacketPerformance {
                latency_microseconds: latency_ns / 1_000,
                frames_per_packet: inline_value,
            });

            match operation {
                Operation::Receive => configuration.receive_flow_performance = value,
                Operation::Transmit => configuration.transmit_flow_performance = value,
                Operation::Unicast => configuration.unicast_performance = value,
                Operation::DefaultSlots => unreachable!("default slots have no latency"),
            }
        }
    }

    configuration
}

impl Operation {
    fn needs_version(self) -> bool {
        matches!(self, Self::Receive | Self::Unicast)
    }

    fn properties(
        self,
        supported: &[u16],
        version: [u16; 3],
    ) -> Result<Vec<(u16, PropertyValue)>, NetaudioError> {
        match self {
            Self::Receive => {
                commands::receive_flow_performance_properties(supported, 1, 1, version)
            }
            Self::Transmit => commands::transmit_flow_performance_properties(supported, 1, 1),
            Self::Unicast => commands::unicast_performance_properties(supported, 1, 1, version),
            Self::DefaultSlots => commands::receive_flow_default_slot_properties(supported, 1),
        }
    }
}

fn software_version(value: &str) -> Option<[u16; 3]> {
    let mut parts = value.split('.');
    let mut version = [0; 3];

    for component in &mut version {
        let part = parts.next()?;

        if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }

        *component = part.parse().ok()?;
    }

    parts.next().is_none().then_some(version)
}

pub fn capabilities(facts: PerformanceFacts) -> Result<PerformanceCapabilities, NetaudioError> {
    let version = facts
        .platform_software_version
        .as_deref()
        .and_then(software_version);
    let supported_property_ids = facts.property_ids.map(|values| known_property_ids(&values));
    let supported = supported_property_ids.as_deref().unwrap_or_default();
    let mut transport_reasons = Vec::new();

    if facts.managed {
        transport_reasons.push("managed_transport_unavailable");
    }

    if facts.protocol_id.is_none() {
        transport_reasons.push("protocol_unknown");
    } else if facts
        .protocol_id
        .is_some_and(|protocol| commands::require_performance_protocol(protocol).is_err())
    {
        transport_reasons.push("protocol_unsupported");
    }

    let mut operations = BTreeMap::new();
    operations.insert(
        "store_current_configuration",
        PerformanceAvailability::new(false, transport_reasons.clone()),
    );

    for (name, operation) in OPERATIONS {
        let mut reasons = transport_reasons.clone();

        if supported_property_ids.is_none() {
            reasons.push("property_directory_unknown");
        } else if operation
            .properties(supported, version.unwrap_or([3, 0, 0]))
            .is_err()
        {
            if operation.needs_version()
                && version.is_some_and(|version| version < [3, 0, 0])
                && !supported.contains(&PROPERTY_PRE_3_COMPATIBILITY)
            {
                reasons.push("compatibility_property_not_advertised");
            } else {
                reasons.push("properties_not_advertised");
            }
        }

        if operation.needs_version() && version.is_none() {
            reasons.push("platform_software_version_unknown");
        }

        // Readability concerns normal control fields, not older-software compatibility writes.
        let readable = operation
            .properties(&PERFORMANCE_PROPERTY_IDS, [3, 0, 0])?
            .iter()
            .any(|(property, _)| supported.contains(property));
        operations.insert(name, PerformanceAvailability::new(readable, reasons));
    }

    Ok(PerformanceCapabilities {
        platform_software_version: version,
        supported_property_ids,
        operations,
    })
}
