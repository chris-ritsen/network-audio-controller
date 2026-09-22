use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{command_spec::CommandSpec, parse_command_spec, SpecError};
use crate::commands::EXTERNAL_RTP_DEFAULT_PORT;

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalReadbackRequest {
    Command {
        specification: Value,
        inventory: Option<Value>,
    },
    Identities {
        identities: Vec<Identity>,
        inventory: Option<Value>,
    },
}

// Compare semantic endpoint identity, not packet pointers or descriptor bytes.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
struct Endpoint {
    ipv4_address: std::net::Ipv4Addr,
    udp_port: u16,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Identity {
    receiver_channel: u16,
    flow_slot: u16,
    source_ipv4: std::net::Ipv4Addr,
    session_id: u64,
    interface_endpoints: Vec<Endpoint>,
}

#[derive(Deserialize)]
struct Correlation {
    matched: bool,
}

#[derive(Deserialize)]
struct Flow {
    effective_subscription_identities: Vec<Identity>,
    sdp_correlation: Option<Correlation>,
}

#[derive(Deserialize)]
struct Inventory {
    result_code: u16,
    page_disposition: String,
    flows: Vec<Flow>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExternalSubscriptionReadback {
    requested_effective_identities: Vec<Identity>,
    observed_effective_identities: Vec<Identity>,
    arc_effective_state_confirmed: Option<bool>,
    sdp_correlation_confirmed: Option<bool>,
}

pub fn external_subscription_readback(
    input: &str,
) -> Result<ExternalSubscriptionReadback, SpecError> {
    let request: ExternalReadbackRequest =
        serde_json::from_str(input).map_err(|error| SpecError::InvalidJson(error.to_string()))?;
    let (expected, scope, inventory) = match request {
        ExternalReadbackRequest::Command {
            specification,
            inventory,
        } => {
            let (expected, scope) = command_intent(specification)?;
            (expected, scope, inventory)
        }
        ExternalReadbackRequest::Identities {
            identities,
            inventory,
        } => {
            let receivers = identities
                .iter()
                .map(|identity| identity.receiver_channel)
                .collect();
            (identities, Scope::Receivers(receivers), inventory)
        }
    };

    Ok(evaluate(expected, scope, inventory))
}

enum Scope {
    Receivers(Vec<u16>),
    Flow {
        receivers: Vec<u16>,
        source_ipv4: std::net::Ipv4Addr,
        session_id: u64,
    },
}

impl Scope {
    fn includes(&self, identity: &Identity) -> bool {
        match self {
            Self::Receivers(receivers) => receivers.contains(&identity.receiver_channel),
            Self::Flow {
                receivers,
                source_ipv4,
                session_id,
            } => {
                receivers.contains(&identity.receiver_channel)
                    && identity.source_ipv4 == *source_ipv4
                    && identity.session_id == *session_id
            }
        }
    }
}

fn command_intent(specification: Value) -> Result<(Vec<Identity>, Scope), SpecError> {
    let command_json = specification.to_string();
    super::build_command_from_json(&command_json)?;
    let CommandSpec::SubscribeExternalRtp {
        receiver_channel_ids,
        flow_slot_assignments,
        primary_destination,
        secondary_destination,
        source_address,
        session_id,
        ..
    } = parse_command_spec(&command_json)?
    else {
        return Err(SpecError::InvalidJson(
            "expected an external RTP subscription command".into(),
        ));
    };

    let source_ipv4 = source_address.parse().map_err(|_| SpecError::InvalidIp)?;
    let interface_endpoints = std::iter::once(primary_destination)
        .chain(secondary_destination)
        .map(|destination| {
            Ok(Endpoint {
                ipv4_address: destination
                    .address
                    .parse()
                    .map_err(|_| SpecError::InvalidIp)?,
                udp_port: if destination.port == 0 {
                    EXTERNAL_RTP_DEFAULT_PORT
                } else {
                    destination.port
                },
            })
        })
        .collect::<Result<Vec<_>, SpecError>>()?;
    let expected: Vec<_> = receiver_channel_ids
        .iter()
        .zip(flow_slot_assignments)
        .filter(|(_, slot)| *slot != 0)
        .map(|(&receiver_channel, flow_slot)| Identity {
            receiver_channel,
            flow_slot,
            source_ipv4,
            session_id,
            interface_endpoints: interface_endpoints.clone(),
        })
        .collect();

    Ok((
        expected,
        Scope::Flow {
            receivers: receiver_channel_ids,
            source_ipv4,
            session_id,
        },
    ))
}

fn evaluate(
    expected: Vec<Identity>,
    scope: Scope,
    inventory: Option<Value>,
) -> ExternalSubscriptionReadback {
    // Missing, partial, failed, or malformed inventories cannot prove absence.
    let inventory = inventory
        .and_then(|value| serde_json::from_value::<Inventory>(value).ok())
        .filter(|inventory| {
            inventory.result_code == crate::protocol::RESULT_CODE_SUCCESS
                && inventory.page_disposition == "complete"
        });
    let mut observed = Vec::new();
    let mut correlated = true;

    if let Some(inventory) = &inventory {
        for flow in &inventory.flows {
            for identity in &flow.effective_subscription_identities {
                if scope.includes(identity) {
                    observed.push(identity.clone());
                    correlated &= flow
                        .sdp_correlation
                        .as_ref()
                        .is_some_and(|value| value.matched);
                }
            }
        }
    }

    let confirmed = inventory.map(|_| {
        let mut expected = expected.clone();
        let mut observed = observed.clone();
        expected.sort();
        observed.sort();
        expected == observed
    });
    let sdp_confirmed = (confirmed == Some(true) && !observed.is_empty()).then_some(correlated);

    ExternalSubscriptionReadback {
        requested_effective_identities: expected,
        observed_effective_identities: observed,
        arc_effective_state_confirmed: confirmed,
        sdp_correlation_confirmed: sdp_confirmed,
    }
}
