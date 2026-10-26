use super::operations::{ControlCompletion, ControlError, ControlOperation, SubscriptionChange};
use super::registry::DeviceRegistry;
use serde_json::{Value, json};
use std::sync::Arc;

pub mod batch;
pub mod outcome;

pub struct SubscriptionRequest {
    pub receiver: String,
    pub changes: Vec<SubscriptionChange>,
    pub count: Option<usize>,
}

impl SubscriptionRequest {
    pub fn set(parameters: &Value) -> Result<Self, ControlError> {
        let receiver = receiver(parameters)?;
        let batch = parameters.get("subscriptions");
        let entries = match batch {
            Some(Value::Array(entries)) if !entries.is_empty() => entries.clone(),
            Some(_) => return Err(invalid("subscriptions must be a non-empty list")),
            None => vec![json!({
                "rx_channel": parameters.get("rx_channel"),
                "tx_channel": parameters.get("tx_channel"),
                "tx_device": parameters.get("tx_device"),
            })],
        };
        let mut changes = Vec::with_capacity(entries.len());

        for entry in entries {
            let mut entry = entry
                .as_object()
                .cloned()
                .ok_or_else(|| invalid("invalid subscription entry"))?;

            if entry
                .keys()
                .any(|key| !["rx_channel", "tx_channel", "tx_device"].contains(&key.as_str()))
            {
                return Err(invalid("invalid subscription entry"));
            }

            if ["tx_channel", "tx_device"].iter().any(|field| {
                entry
                    .get(*field)
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
            }) {
                return Err(invalid("tx_channel and tx_device are required"));
            }

            entry.insert("action".to_owned(), json!("set"));
            changes.push(
                serde_json::from_value(Value::Object(entry))
                    .map_err(|error| invalid(error.to_string()))?,
            );
        }

        Ok(Self {
            receiver,
            count: batch.map(|_| changes.len()),
            changes,
        })
    }

    pub fn clear(parameters: &Value) -> Result<Self, ControlError> {
        let receiver = receiver(parameters)?;
        let batch = parameters.get("rx_channels");
        let numbers = match batch {
            Some(Value::Array(numbers)) if !numbers.is_empty() => numbers.clone(),
            Some(_) => return Err(invalid("rx_channels must be a non-empty list")),
            None => vec![parameters.get("rx_channel").cloned().unwrap_or_default()],
        };
        let changes = numbers
            .into_iter()
            .map(|number| {
                let receiver_channel = number
                    .as_u64()
                    .and_then(|number| u16::try_from(number).ok())
                    .filter(|number| *number != 0)
                    .ok_or_else(|| invalid("rx_channel must be a positive channel number"))?;
                Ok(SubscriptionChange::Clear { receiver_channel })
            })
            .collect::<Result<Vec<_>, ControlError>>()?;
        Ok(Self {
            receiver,
            count: batch.map(|_| changes.len()),
            changes,
        })
    }

    pub async fn apply(
        self,
        registry: &Arc<DeviceRegistry>,
    ) -> Result<(ControlCompletion, Option<usize>), ControlError> {
        let completion = registry
            .apply(
                &self.receiver,
                ControlOperation::Subscriptions {
                    changes: self.changes,
                },
            )
            .await?;
        Ok((completion, self.count))
    }
}

fn invalid(message: impl Into<String>) -> ControlError {
    ControlError::Invalid(message.into())
}

fn receiver(parameters: &Value) -> Result<String, ControlError> {
    parameters["rx_device"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("rx_device must be a string"))
}
