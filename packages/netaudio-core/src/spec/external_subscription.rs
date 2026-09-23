use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::SpecError;
use crate::commands::EXTERNAL_RTP_DEFAULT_PORT;

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ExternalRtpDestinationSpec {
    pub address: String,
    #[serde(default)]
    pub port: u16,
}

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExternalSubscriptionParameters {
    pub advertised_flow_slot_count: u16,
    #[serde(default)]
    pub clock_offset: Option<u32>,
    pub device_protocol: u16,
    pub flow_slot_assignments: Vec<u16>,
    #[serde(default)]
    pub message_id: u16,
    pub primary_destination: ExternalRtpDestinationSpec,
    pub receiver_channel_ids: Vec<u16>,
    pub session_id: u64,
    pub source_address: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ExternalSubscriptionSpec {
    #[serde(flatten)]
    pub parameters: ExternalSubscriptionParameters,
    #[serde(default)]
    pub advertisement_supports_multiple_interfaces: bool,
    #[serde(default)]
    pub receiver_supports_multiple_interfaces: bool,
    #[serde(default)]
    pub secondary_destination: Option<ExternalRtpDestinationSpec>,
}

impl ExternalSubscriptionSpec {
    pub fn build(&self) -> Result<Vec<u8>, SpecError> {
        use crate::commands::{
            ExternalFlowIdentity, ExternalReceiverSubscription, ExternalRtpDestination,
        };

        let destination =
            |value: &ExternalRtpDestinationSpec| -> Result<ExternalRtpDestination, SpecError> {
                Ok(ExternalRtpDestination {
                    address: value.address.parse().map_err(|_| SpecError::InvalidIp)?,
                    port: value.port,
                })
            };
        let p = &self.parameters;
        Ok(crate::commands::build_external_receiver_subscription(
            &ExternalReceiverSubscription {
                device_protocol: p.device_protocol,
                receiver_channel_ids: &p.receiver_channel_ids,
                flow_slot_assignments: &p.flow_slot_assignments,
                advertised_flow_slot_count: p.advertised_flow_slot_count,
                flow_identity: ExternalFlowIdentity {
                    source_address: p.source_address.parse().map_err(|_| SpecError::InvalidIp)?,
                    session_id: p.session_id,
                },
                clock_offset: p.clock_offset.unwrap_or(0),
                primary_destination: destination(&p.primary_destination)?,
                secondary_destination: self
                    .secondary_destination
                    .as_ref()
                    .map(destination)
                    .transpose()?,
                advertisement_supports_multiple_interfaces: self
                    .advertisement_supports_multiple_interfaces,
                receiver_supports_multiple_interfaces: self.receiver_supports_multiple_interfaces,
            },
            p.message_id,
        )?)
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ExternalSubscriptionPlanRequest {
    #[serde(flatten)]
    pub parameters: ExternalSubscriptionParameters,
    pub secondary_address: Option<String>,
    pub secondary_port: Option<u16>,
    pub receiver_supports_multiple_interfaces: bool,
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalSubscriptionCommand {
    SubscribeExternalRtp(ExternalSubscriptionSpec),
}

pub fn plan_external_subscription(
    request: ExternalSubscriptionPlanRequest,
) -> Result<ExternalSubscriptionCommand, SpecError> {
    let advertised = request.secondary_address.zip(request.secondary_port);
    let specification = ExternalSubscriptionSpec {
        parameters: request.parameters,
        advertisement_supports_multiple_interfaces: advertised.is_some(),
        receiver_supports_multiple_interfaces: request.receiver_supports_multiple_interfaces,
        secondary_destination: advertised
            .filter(|_| request.receiver_supports_multiple_interfaces)
            .map(|(address, port)| ExternalRtpDestinationSpec { address, port }),
    };
    specification.build()?;
    Ok(ExternalSubscriptionCommand::SubscribeExternalRtp(
        specification,
    ))
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalReadbackRequest {
    Command {
        specification: ExternalSubscriptionCommand,
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

fn command_intent(
    specification: ExternalSubscriptionCommand,
) -> Result<(Vec<Identity>, Scope), SpecError> {
    let ExternalSubscriptionCommand::SubscribeExternalRtp(specification) = specification;
    specification.build()?;
    let ExternalSubscriptionSpec {
        parameters:
            ExternalSubscriptionParameters {
                receiver_channel_ids,
                flow_slot_assignments,
                primary_destination,
                source_address,
                session_id,
                ..
            },
        secondary_destination,
        ..
    } = specification;

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
