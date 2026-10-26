use crate::devices::operations::{
    ChannelDirection, ControlCompletion, ControlError, ControlOperation, SubscriptionChange,
};
use crate::devices::transport::TransportError;
use serde_json::{Value, json};

pub struct Observation {
    pub confirmed: Option<bool>,
    pub effective: Option<Value>,
}

pub fn observe(
    operation: &ControlOperation,
    record: &Value,
    completion: &ControlCompletion,
) -> Observation {
    if !completion.readback_completed {
        return Observation {
            confirmed: None,
            effective: None,
        };
    }

    let observed = completion.observation.as_ref().unwrap_or(&Value::Null);
    let (confirmed, effective) = match operation {
        ControlOperation::Panel(_) => (
            observed["effective_state_confirmed"].as_bool(),
            observed.clone(),
        ),
        ControlOperation::Locking(_) => (observed["state_verified"].as_bool(), observed.clone()),
        ControlOperation::Maintenance(_) => (None, observed.clone()),
        ControlOperation::SampleRate(_) => (observed["state_verified"].as_bool(), observed.clone()),
        ControlOperation::MulticastPrefix(_)
        | ControlOperation::StatusConfiguration(_)
        | ControlOperation::NetworkConfiguration(_) => (
            observed["configuration_confirmed"].as_bool(),
            observed.clone(),
        ),
        ControlOperation::FlowAuthoring(_) => (
            observed["effective_state_confirmation"].as_bool(),
            observed.clone(),
        ),
        ControlOperation::ExternalSubscription(_) => (
            observed["arc_effective_state_confirmed"].as_bool(),
            observed.clone(),
        ),
        ControlOperation::RenameDevice { name } => (
            record["name"].as_str().map(|observed| observed == name),
            json!({"name": record["name"]}),
        ),
        ControlOperation::RenameChannel {
            direction,
            number,
            name,
        } => {
            let group = match direction {
                ChannelDirection::Receiver => "receivers",
                ChannelDirection::Transmitter => "transmitters",
            };
            let channel = &record["channels"][group][number.to_string()];
            let observed = channel["friendly_name"]
                .as_str()
                .or_else(|| channel["name"].as_str());
            (
                observed.map(|observed| observed == name),
                json!({"name": observed}),
            )
        }
        ControlOperation::Subscriptions { changes } => {
            let routes = record["subscriptions"].as_array();
            let observed = changes
                .iter()
                .map(|change| {
                    routes.and_then(|routes| {
                        routes.iter().find(|route| {
                            route["rx_channel_number"].as_u64()
                                == Some(u64::from(change.receiver_channel()))
                        })
                    })
                })
                .collect::<Vec<_>>();
            let confirmed = observed
                .iter()
                .zip(changes)
                .map(|(route, change)| {
                    let route = (*route)?;
                    match change {
                        SubscriptionChange::Set {
                            transmitter_channel,
                            transmitter_device,
                            ..
                        } => Some(
                            route["tx_channel"] == *transmitter_channel
                                && route["tx_device"] == *transmitter_device,
                        ),
                        SubscriptionChange::Clear { .. } => Some(
                            [route.get("tx_channel"), route.get("tx_device")]
                                .into_iter()
                                .all(|field| {
                                    matches!(field, Some(Value::Null))
                                        || field.and_then(Value::as_str) == Some("")
                                }),
                        ),
                    }
                })
                .collect::<Option<Vec<_>>>()
                .map(|matches| matches.into_iter().all(|matches| matches));
            (confirmed, json!({"subscriptions": observed}))
        }
        ControlOperation::SetLatency { .. }
        | ControlOperation::Clock(_)
        | ControlOperation::AudioConfiguration(_) => (
            observed["effective_state_confirmed"].as_bool(),
            observed.clone(),
        ),
        ControlOperation::Performance(_) => (
            observed["effective_state_confirmation"].as_bool(),
            observed["effective_properties"].clone(),
        ),
        ControlOperation::Identify
        | ControlOperation::InspectMulticastPrefix
        | ControlOperation::InspectStatus(_)
        | ControlOperation::RefreshExternalReceiver
        | ControlOperation::RefreshAudio(_)
        | ControlOperation::RefreshClock { .. }
        | ControlOperation::InspectTransmitFlows => (None, Value::Null),
    };
    Observation {
        confirmed,
        effective: Some(effective),
    }
}

pub fn failure(error: &ControlError) -> Value {
    let mut result = json!({"message": error.to_string()});

    match error {
        ControlError::Rejected {
            result_code,
            acknowledged_commands,
        } => {
            result["result_code"] = json!(result_code);
            result["acknowledged_commands"] = json!(acknowledged_commands);
        }
        ControlError::Transport {
            error,
            acknowledged_commands,
        } => {
            result["acknowledged_commands"] = json!(acknowledged_commands);
            result["outcome_unknown"] =
                json!(matches!(error, TransportError::WriteOutcomeUnknown(_)));
        }
        ControlError::Subscription(subscription) => {
            result["cause"] = failure(&subscription.cause);
            result["acknowledged_receivers"] = json!(subscription.acknowledged_receivers);
            result["attempted_receivers"] = json!(subscription.attempted_receivers);
            result["readback_completed"] = json!(subscription.readback_completed);
            result["readback_errors"] = json!(subscription.readback_errors);
        }
        _ => {}
    }

    result
}
