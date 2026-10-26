use super::{ControlError, ControlOperation, DeviceRegistry, SubscriptionChange, invalid};
use crate::state::ReadModel;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

const MAXIMUM_ROUTES: usize = 4096;
const DEVICE_CONCURRENCY: usize = 16;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Route {
    #[serde(rename = "rx_device")]
    receiver: String,
    #[serde(rename = "rx_channel")]
    channel: NonZeroU16,
    #[serde(rename = "tx_device")]
    source: Option<String>,
    #[serde(rename = "tx_channel")]
    source_channel: Option<String>,
}

impl Route {
    fn change(&self) -> SubscriptionChange {
        match (&self.source, &self.source_channel) {
            (Some(device), Some(channel)) => SubscriptionChange::Set {
                receiver_channel: self.channel.get(),
                transmitter_channel: channel.clone(),
                transmitter_device: device.clone(),
            },
            _ => SubscriptionChange::Clear {
                receiver_channel: self.channel.get(),
            },
        }
    }

    fn result(&self) -> Value {
        let mut result = json!(self);
        result["action"] = json!(if self.source.is_some() {
            "set"
        } else {
            "clear"
        });
        result["ok"] = json!(false);
        result
    }
}

pub struct BatchRequest {
    routes: Vec<Route>,
}

impl BatchRequest {
    pub fn parse(value: &Value) -> Result<Self, ControlError> {
        let entries = value
            .as_array()
            .filter(|entries| !entries.is_empty())
            .ok_or_else(|| invalid("routes must be a non-empty list"))?;

        if entries.len() > MAXIMUM_ROUTES {
            return Err(invalid(format!(
                "routes must contain at most {MAXIMUM_ROUTES} entries"
            )));
        }

        let mut routes = Vec::with_capacity(entries.len());
        let mut seen = BTreeMap::new();

        for (index, entry) in entries.iter().enumerate() {
            let mut route: Route = serde_json::from_value(entry.clone())
                .map_err(|error| invalid(format!("routes[{index}]: {error}")))?;
            route.receiver = route.receiver.trim().to_owned();

            if route.receiver.is_empty() {
                return Err(invalid(format!(
                    "routes[{index}].rx_device must be a device name"
                )));
            }

            match (&route.source, &route.source_channel) {
                (None, None) => {}
                (Some(device), Some(channel))
                    if !device.trim().is_empty() && !channel.trim().is_empty() => {}
                _ => {
                    return Err(invalid(format!(
                        "routes[{index}] must give both tx_device and tx_channel as non-empty strings, or neither to clear"
                    )));
                }
            }

            let key = (route.receiver.clone(), route.channel);

            if let Some(previous) = seen.insert(key, index) {
                return Err(invalid(format!(
                    "routes[{index}] repeats {} channel {} from routes[{previous}]",
                    route.receiver, route.channel
                )));
            }

            routes.push(route);
        }

        Ok(Self { routes })
    }

    pub async fn apply(
        self,
        registry: &Arc<DeviceRegistry>,
        model: &ReadModel,
    ) -> Result<Value, ControlError> {
        let mut receivers = BTreeSet::new();
        let mut results: Vec<_> = self.routes.iter().map(Route::result).collect();
        let mut groups = BTreeMap::<String, Vec<usize>>::new();

        for (index, route) in self.routes.iter().enumerate() {
            let Some(record) = model.device(&route.receiver)? else {
                results[index]["error"] = json!("rx device not found");
                continue;
            };
            let identity = record["server_name"]
                .as_str()
                .ok_or_else(|| invalid("receiver has no stable device identity"))?;

            if !receivers.insert((identity.to_owned(), route.channel)) {
                return Err(invalid(
                    "routes address the same receiver channel more than once",
                ));
            }

            groups
                .entry(route.receiver.clone())
                .or_default()
                .push(index);
        }

        let mut batches = BTreeMap::new();
        let mut plans = Vec::new();

        for (receiver, indices) in groups {
            let changes = indices
                .iter()
                .map(|&index| self.routes[index].change())
                .collect();
            let operation = ControlOperation::Subscriptions { changes };
            let count = registry.planned_batch_count(&receiver, &operation)?;
            batches.insert(receiver.clone(), count);
            plans.push((receiver, indices, operation));
        }

        let mut pending = stream::iter(plans.into_iter().map(|(receiver, indices, operation)| {
            let registry = Arc::clone(registry);
            async move { (indices, registry.apply(&receiver, operation).await) }
        }))
        .buffer_unordered(DEVICE_CONCURRENCY);

        while let Some((indices, outcome)) = pending.next().await {
            for index in indices {
                if let Some(managed) = crate::managed::control::outcome(&outcome) {
                    results[index]["managed"] = crate::managed::control::evidence(managed);
                }
                match &outcome {
                    Ok(completion) => {
                        results[index]["ok"] = json!(true);
                        results[index]["readback_completed"] = json!(completion.readback_completed);

                        if !completion.readback_errors.is_empty() {
                            results[index]["readback_errors"] = json!(completion.readback_errors);
                        }
                    }
                    Err(ControlError::Subscription(failure)) => {
                        let outcome = failure.route(self.routes[index].channel.get());
                        results[index]
                            .as_object_mut()
                            .expect("route result is an object")
                            .extend(
                                outcome
                                    .as_object()
                                    .expect("route outcome is an object")
                                    .clone(),
                            );
                    }
                    Err(error) => {
                        results[index]["error"] = json!(error.to_string());

                        if let ControlError::Transport {
                            error: super::super::transport::TransportError::WriteOutcomeUnknown(_),
                            ..
                        } = error
                        {
                            results[index]["outcome_unknown"] = json!(true);
                        }
                    }
                }
            }
        }

        let applied = results.iter().filter(|route| route["ok"] == true).count();
        Ok(json!({
            "success": applied == results.len(),
            "applied": applied,
            "failed": results.len() - applied,
            "batches": batches,
            "routes": results,
        }))
    }
}
