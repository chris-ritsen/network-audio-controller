use super::super::{controller::Exchange, device_session::DeviceSession, status::Query};
use super::channel::Change;
use crate::{
    devices::{
        audio_configuration::{AudioConfiguration, AudioKind},
        capabilities, multicast_prefix, network,
        operations::{ControlError, ControlOperation},
        status_configuration,
    },
    monitoring::requests::StatusQuery,
};
use serde_json::{Value, json};

pub(in crate::managed) enum Configuration<'a> {
    Status(&'a status_configuration::Configuration),
    Network(&'a network::Change),
    Audio(&'a AudioConfiguration),
    Latency(f64),
    MulticastPrefix(&'a multicast_prefix::Configuration),
    Channel(Change<'a>),
}

impl<'a> Configuration<'a> {
    pub fn from_operation(operation: &'a ControlOperation) -> Option<Self> {
        if let Some(change) = Change::from_operation(operation) {
            return Some(Self::Channel(change));
        }
        match operation {
            ControlOperation::MulticastPrefix(configuration) => {
                Some(Self::MulticastPrefix(configuration))
            }
            ControlOperation::SetLatency { milliseconds } => Some(Self::Latency(*milliseconds)),
            ControlOperation::StatusConfiguration(configuration) => {
                Some(Self::Status(configuration))
            }
            ControlOperation::NetworkConfiguration(configuration) => {
                Some(Self::Network(configuration))
            }
            ControlOperation::AudioConfiguration(configuration) => Some(Self::Audio(configuration)),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Channel(_) => "rename_channel",
            Self::Latency(_) => "set_latency",
            Self::MulticastPrefix(_) => "set_aes67_multicast_prefix",
            Self::Status(configuration) => configuration.name(),
            Self::Network(configuration) => configuration.name(),
            Self::Audio(configuration) => match configuration.kind {
                AudioKind::Encoding => "set_encoding",
                AudioKind::SampleRatePullup => "set_sample_rate_pullup",
            },
        }
    }

    pub fn requested(&self) -> Value {
        match self {
            Self::Channel(change) => change.requested(),
            Self::Latency(milliseconds) => json!({"latency": milliseconds}),
            Self::MulticastPrefix(configuration) => json!({"prefix": configuration.prefix}),
            Self::Status(configuration) => configuration.requested(),
            Self::Network(configuration) => configuration.requested(),
            Self::Audio(configuration) => {
                json!({configuration.kind.parameter(): configuration.requested})
            }
        }
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        match self {
            Self::Channel(change) => change.command(record),
            Self::Latency(milliseconds) => super::latency::command(*milliseconds, record),
            Self::MulticastPrefix(configuration) => configuration.command(record),
            Self::Status(configuration) => configuration.command(record),
            Self::Network(configuration) => configuration.command(record),
            Self::Audio(configuration) => configuration.command(record),
        }
    }

    pub fn preflight_queries(&self) -> Vec<Query> {
        match self {
            Self::Channel(_) => vec![StatusQuery::Lock.into()],
            Self::Latency(_) => vec![StatusQuery::Lock.into(), Query::Latency],
            Self::MulticastPrefix(_) => vec![
                StatusQuery::Lock.into(),
                StatusQuery::Aes67.into(),
                Query::MulticastPrefix,
            ],
            Self::Status(configuration) => {
                vec![StatusQuery::Lock.into(), configuration.query().into()]
            }
            Self::Audio(configuration) => {
                vec![StatusQuery::Lock.into(), configuration.kind.query().into()]
            }
            Self::Network(_) => vec![
                StatusQuery::Lock.into(),
                StatusQuery::Platform.into(),
                StatusQuery::Interface.into(),
            ],
        }
    }

    pub fn affected_queries(&self) -> Vec<Query> {
        let mut queries = self.preflight_queries();
        if matches!(self, Self::Network(_)) {
            queries.push(StatusQuery::SwitchConfiguration.into());
        }
        queries
    }

    pub fn inventory_readback(&self) -> bool {
        matches!(self, Self::Channel(_))
    }

    pub async fn prepare(&self, session: &mut DeviceSession) -> Result<(), ControlError> {
        if let Self::Channel(change) = self {
            change.probe(session).await?;
        }
        Ok(())
    }

    pub fn validate_readback(&self, before: &Value, after: &Value) -> Result<(), ControlError> {
        if let Self::Channel(change) = self {
            change.validate_readback(before, after)?;
        }
        Ok(())
    }

    pub fn needs_switch_status(&self, record: &Value) -> Result<bool, ControlError> {
        Ok(matches!(self, Self::Network(_))
            && capabilities::operation(record, "redundancy")?.supported != Some(false))
    }

    pub fn readback_queries(&self, before: &Value) -> Vec<Query> {
        match self {
            Self::Channel(_) => Vec::new(),
            Self::Latency(_) => vec![Query::Latency],
            Self::MulticastPrefix(_) => vec![Query::MulticastPrefix],
            Self::Status(configuration) => vec![configuration.query().into()],
            Self::Audio(configuration) => vec![configuration.kind.query().into()],
            Self::Network(_) if network::Change::uses_switch_status(before) => {
                vec![
                    StatusQuery::Interface.into(),
                    StatusQuery::SwitchConfiguration.into(),
                ]
            }
            Self::Network(_) => vec![StatusQuery::Interface.into()],
        }
    }

    pub fn completion(
        &self,
        before: &Value,
        after: Option<&Value>,
        sent: bool,
        acknowledgement: &Value,
    ) -> Result<Value, ControlError> {
        Ok(match self {
            Self::Channel(change) => {
                change.completion(after, acknowledgement["accepted"].as_bool())
            }
            Self::MulticastPrefix(configuration) => {
                let mut result = configuration.completion(
                    after.and_then(|record| record["aes67_multicast_prefix"].as_str()),
                    sent,
                    &acknowledgement["request_acknowledgement"],
                );
                result["effective_state_confirmed"] = Value::Null;
                result
            }
            Self::Latency(milliseconds) => super::latency::completion(
                *milliseconds,
                after,
                acknowledgement["accepted"].as_bool(),
            )?,
            Self::Status(configuration) => {
                configuration.completion(after.and_then(Value::as_object), sent)
            }
            Self::Network(configuration) => configuration.completion(before, after, sent),
            Self::Audio(configuration) => {
                let mut result =
                    configuration.completion(after.and_then(Value::as_object), sent)?;
                result["current"] = result["current_value"].clone();
                result["configured"] = result["status"]["requested_value"].clone();
                result["configuration_confirmed"] = json!(
                    result["configured"]
                        .as_u64()
                        .map(|value| value == u64::from(configuration.requested))
                );
                if after.is_none() {
                    result["effective_state_confirmed"] = Value::Null;
                }
                result["persistence_confirmed"] = Value::Null;
                result
            }
        })
    }

    pub fn acknowledge(&self, exchange: &Exchange, result: &mut Value) -> Result<(), ControlError> {
        if matches!(
            self,
            Self::Latency(_) | Self::MulticastPrefix(_) | Self::Channel(_)
        ) {
            let receipt = exchange
                .packet
                .as_deref()
                .and_then(netaudio_core::responses::parse_command_acknowledgement)
                .ok_or_else(|| {
                    ControlError::ObservationUnavailable(
                        "Controller ARC acknowledgement is malformed; do not replay the command"
                            .into(),
                    )
                })?;
            result["accepted"] = json!(receipt.accepted);
            result["result_code"] = json!(receipt.result_code);
            result["request_acknowledgement"] = json!(receipt);
            if !receipt.accepted {
                result["error"] = json!("Device rejected the ARC command");
            }
        } else {
            result["accepted"] = json!(true);
        }
        Ok(())
    }

    pub fn confirmation(&self, result: &Value) -> Option<bool> {
        if matches!(self, Self::Latency(_) | Self::Channel(_)) {
            return match result["accepted"].as_bool() {
                Some(false) => Some(false),
                Some(true) => result["configuration_confirmed"].as_bool(),
                None => None,
            };
        }
        let configured = result["configuration_confirmed"].as_bool()?;
        if matches!(self, Self::Audio(_)) {
            Some(configured && result["effective_state_confirmed"].as_bool()?)
        } else {
            Some(configured)
        }
    }

    pub fn observe(&self, record: &Value, result: &mut Value) -> Result<(), ControlError> {
        if self.inventory_readback() {
            result["readback_transport"] = json!("managed_api");
            result["inventory_observation"] = json!({
                "transport": "managed_api", "observed_at_unix": record["ddm_last_sync"],
                "fresh": record["ddm_fresh"],
            });
        }
        if let Self::Network(configuration) = self {
            result["network"] = capabilities::network(record)?;
            if matches!(configuration, network::Change::Redundancy(_)) {
                result["effective_readback"] = capabilities::redundancy(record)?;
            }
        }
        Ok(())
    }

    pub fn observation(&self, record: &Value, before: &Value) -> Option<Value> {
        Some(match self {
            Self::Channel(_) => return None,
            Self::Latency(_) => record["managed_status"]["latency"].clone(),
            Self::MulticastPrefix(_) => record["managed_status"]["aes67_multicast_prefix"].clone(),
            Self::Status(configuration) => {
                record["managed_status"][configuration.query().kind().as_str()].clone()
            }
            Self::Audio(configuration) => {
                record["managed_status"][configuration.kind.query().kind().as_str()].clone()
            }
            Self::Network(_) => json!(
                self.readback_queries(before)
                    .into_iter()
                    .map(|query| {
                        let name = query.kind().as_str();
                        (name.to_owned(), record["managed_status"][name].clone())
                    })
                    .collect::<serde_json::Map<_, _>>()
            ),
        })
    }
}
