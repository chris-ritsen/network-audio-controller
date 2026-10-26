use crate::devices::operations::{ControlError, ControlOperation, SubscriptionChange};
use crate::devices::record::DeviceRecord;
use anyhow::{Context, Result, ensure};
use netaudio_core::presets::planning::{Action, ActionKind, ActionState, RequestedAction};
use serde_json::{Map, Value, json};

pub fn compare(
    request: &RequestedAction,
    operations: &[ControlOperation],
    record: &DeviceRecord,
) -> Result<Action> {
    compare_presented(request, operations, &record.presentation()?, |operation| {
        operation.plan(record).map(|_| ())
    })
}

pub fn compare_presented(
    request: &RequestedAction,
    operations: &[ControlOperation],
    presented: &Value,
    mut validator: impl FnMut(&ControlOperation) -> Result<(), ControlError>,
) -> Result<Action> {
    let operation = operations
        .first()
        .context("preset operation is unavailable")?;
    let current = match operation {
        ControlOperation::RenameDevice { .. } => presented["name"].clone(),
        ControlOperation::AudioConfiguration(configuration) => {
            let current = presented[configuration.kind.current_field()].clone();
            ensure!(
                current.is_u64(),
                "fresh audio status did not report a current value"
            );
            current
        }
        ControlOperation::SetLatency { .. } => {
            let nanoseconds = presented["configured_latency_ns"]
                .as_u64()
                .context("fresh configured latency is unavailable")?;
            json!(nanoseconds as f64 / 1_000_000.0)
        }
        ControlOperation::NetworkConfiguration(configuration) => {
            let (current, matches) = match configuration {
                crate::devices::network::Change::Interface(configuration) => {
                    let current = configuration.selected(&presented["interfaces"]).context("fresh interface identity is unavailable or ambiguous")?["configured"].clone();
                    let expected = configuration.requested()["configuration"].clone();
                    ensure!(
                        matches!(current["mode"].as_str(), Some("dynamic" | "static")),
                        "fresh configured interface mode is unavailable"
                    );
                    ensure!(
                        current["mode"] != expected["mode"]
                            || expected.as_object().is_some_and(|fields| fields
                                .keys()
                                .all(|name| !current[name].is_null())),
                        "fresh configured interface state is incomplete"
                    );
                    let matches = configuration.matches(&current);
                    (current, matches)
                }
                crate::devices::network::Change::Redundancy(configuration) => {
                    let current = &presented["dante_redundancy"];
                    ensure!(
                        current["state_fresh"] == true && !current["configured"].is_null(),
                        "fresh redundancy mode is unavailable"
                    );
                    (current.clone(), configuration.matches(current))
                }
            };
            let mut action = request.compare(current, matches);
            action.details = configuration.completion(presented, Some(presented), false);
            return validate(action, operations, &mut validator);
        }
        ControlOperation::Clock(configuration) => {
            let plan = configuration.plan(presented)?;
            return validate(
                request.compare(json!(plan.before), plan.changes.is_empty()),
                operations,
                &mut validator,
            );
        }
        ControlOperation::Performance(configuration) => {
            let snapshot = crate::devices::performance::snapshot(presented)?;
            let current = snapshot[configuration.name()].clone();
            ensure!(
                !current.is_null(),
                "fresh performance properties are incomplete or inconsistent"
            );
            let matches = current == request.requested;
            return validate(
                request.compare(current, matches),
                operations,
                &mut validator,
            );
        }
        ControlOperation::RenameChannel { direction, .. } => {
            let channels = if *direction == crate::devices::operations::ChannelDirection::Receiver {
                &presented["channels"]["receivers"]
            } else {
                &presented["channels"]["transmitters"]
            };
            let mut current = Map::new();
            for number in request
                .requested
                .as_object()
                .context("channel names must be an object")?
                .keys()
            {
                let channel = channels.get(number).with_context(|| {
                    format!("fresh channel inventory does not contain channel {number}")
                })?;
                let name = channel
                    .get("friendly_name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .or_else(|| channel["name"].as_str())
                    .context("fresh channel label is unavailable")?;
                current.insert(number.clone(), json!(name));
            }
            Value::Object(current)
        }
        ControlOperation::Subscriptions { changes } => {
            let mut current = Map::new();
            for change in changes {
                let number = change.receiver_channel();
                ensure!(
                    presented["channels"]["receivers"]
                        .get(number.to_string())
                        .is_some(),
                    "fresh receiver inventory does not contain channel {number}"
                );
                let route = presented["subscriptions"]
                    .as_array()
                    .context("fresh subscriptions are unavailable")?
                    .iter()
                    .find(|route| route["rx_channel_number"] == number)
                    .context("fresh receiver subscription is unavailable")?;
                let device = route["tx_device"].as_str().filter(|name| !name.is_empty());
                let channel = route["tx_channel"].as_str().filter(|name| !name.is_empty());
                ensure!(
                    device.is_some() == channel.is_some(),
                    "fresh receiver subscription is incomplete"
                );
                let external = presented
                    .get("receiver_flows")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .flat_map(|flow| {
                        flow["effective_subscription_identities"]
                            .as_array()
                            .into_iter()
                            .flatten()
                    })
                    .find(|identity| identity["receiver_channel"] == number);
                ensure!(
                    device.is_none() || external.is_none(),
                    "receiver channel reports both native and external sources"
                );
                let value = if let (Some(device), Some(channel)) = (device, channel) {
                    json!({"kind": "native_dante", "tx_device": device, "tx_channel": channel})
                } else if let Some(identity) = external {
                    json!({"kind": "external_rtp", "identity": identity})
                } else {
                    Value::Null
                };
                current.insert(number.to_string(), value);
            }
            let mut requested = request.requested.clone();
            for change in changes {
                if let SubscriptionChange::Set {
                    receiver_channel,
                    transmitter_device,
                    ..
                } = change
                {
                    requested[receiver_channel.to_string()]["tx_device"] =
                        json!(if transmitter_device == "." {
                            presented["name"]
                                .as_str()
                                .context("device name is unavailable")?
                        } else {
                            transmitter_device
                        });
                }
            }
            return validate(
                request.compare(json!(current), json!(current) == requested),
                operations,
                &mut validator,
            );
        }
        ControlOperation::StatusConfiguration(
            crate::devices::status_configuration::Configuration::Analog(configuration),
        ) => {
            let plan = configuration.plan(&presented["gain_adapter"])?;
            let plan = serde_json::to_value(plan)?;
            let state = serde_json::from_value(plan["action"].clone())?;
            let mut result = request.result(
                state,
                plan["before"].clone(),
                plan["reason"]
                    .as_str()
                    .unwrap_or("Fresh analog level plan."),
            );
            result.details = plan;
            return validate(result, operations, &mut validator);
        }
        _ => anyhow::bail!("this preset action requires a specialized planner"),
    };
    let matches = match request.kind {
        ActionKind::Latency => {
            let expected = request
                .requested
                .as_f64()
                .context("invalid requested latency")?;
            presented["configured_latency_ns"]
                == netaudio_core::latency_configuration::requested_nanoseconds(expected)
                    .map_err(anyhow::Error::msg)?
        }
        _ => current == request.requested,
    };
    validate(
        request.compare(current, matches),
        operations,
        &mut validator,
    )
}

fn validate(
    mut action: Action,
    operations: &[ControlOperation],
    validator: &mut impl FnMut(&ControlOperation) -> Result<(), ControlError>,
) -> Result<Action> {
    if action.state != ActionState::Change {
        return Ok(action);
    }
    for operation in operations {
        if let Err(error) = validator(operation) {
            action.state = if matches!(error, ControlError::Invalid(_)) {
                ActionState::Unsupported
            } else {
                ActionState::Unavailable
            };
            action.reason = error.to_string();
            break;
        }
    }
    Ok(action)
}
