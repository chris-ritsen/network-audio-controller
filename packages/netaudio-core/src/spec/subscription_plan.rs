use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use serde_json::{json, Value};

use super::{command_spec::SubscriptionPageEntry, SpecError};
use crate::commands::SUBSCRIPTION_PAGE_CAPACITY;

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Receiver {
    pub number: u16,
    pub media_type_code: Option<u16>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SubscriptionPlanRequest {
    pub protocol_id: u16,
    pub channels: Vec<Receiver>,
    pub records: Vec<SubscriptionPageEntry>,
}

pub struct SubscriptionCommand {
    pub command: Value,
    pub receiver_channels: Vec<u16>,
}

pub fn plan_subscription_command_groups(
    input: &str,
) -> Result<Vec<SubscriptionCommand>, SpecError> {
    let request: SubscriptionPlanRequest =
        serde_json::from_str(input).map_err(|error| SpecError::InvalidJson(error.to_string()))?;

    plan_grouped_request(request)
}

pub fn plan_subscription_commands(input: &str) -> Result<Vec<Value>, SpecError> {
    let request: SubscriptionPlanRequest =
        serde_json::from_str(input).map_err(|error| SpecError::InvalidJson(error.to_string()))?;

    plan_subscription_request(request)
}

pub(crate) fn plan_subscription_request(
    request: SubscriptionPlanRequest,
) -> Result<Vec<Value>, SpecError> {
    Ok(plan_grouped_request(request)?
        .into_iter()
        .map(|group| group.command)
        .collect())
}

fn plan_grouped_request(
    request: SubscriptionPlanRequest,
) -> Result<Vec<SubscriptionCommand>, SpecError> {
    let protocol = crate::protocol::arc_protocol_for_identifier(request.protocol_id, false)
        .map_err(|error| SpecError::InvalidJson(error.into()))?;

    if request.channels.is_empty() || request.records.is_empty() {
        return Err(SpecError::InvalidJson(
            "subscription plan requires receiver channels and records".into(),
        ));
    }

    let capacity = request.channels.len().min(SUBSCRIPTION_PAGE_CAPACITY);
    let mut receivers = HashMap::new();

    for channel in request.channels {
        if channel.number == 0
            || receivers
                .insert(channel.number, channel.media_type_code)
                .is_some()
        {
            return Err(SpecError::InvalidJson(
                "receiver channel numbers must be nonzero and unique".into(),
            ));
        }
    }

    let mut seen = HashSet::new();
    let mut groups: Vec<(u16, Vec<SubscriptionPageEntry>)> = Vec::new();
    let mut clears = Vec::new();
    let mut subscriptions = Vec::new();

    for record in request.records {
        let number = match &record {
            SubscriptionPageEntry::Set { rx_channel, .. }
            | SubscriptionPageEntry::Clear { rx_channel } => *rx_channel,
        };

        if !seen.insert(number) {
            return Err(SpecError::InvalidJson(format!(
                "receiver channel {number} appears more than once"
            )));
        }

        let media = receivers.get(&number).ok_or_else(|| {
            SpecError::InvalidJson(format!("receiver channel {number} is unavailable"))
        })?;

        if !protocol.subscription_page {
            match record {
                SubscriptionPageEntry::Clear { rx_channel } => clears.push(rx_channel),
                SubscriptionPageEntry::Set {
                    rx_channel,
                    tx_channel,
                    tx_device,
                } => {
                    subscriptions.push((
                        rx_channel,
                        json!({
                            "rx_channel": rx_channel,
                            "tx_channel": tx_channel,
                            "tx_device": tx_device,
                        }),
                    ));
                }
            }

            continue;
        }

        let media = media.ok_or_else(|| {
            SpecError::InvalidJson(format!(
                "receiver channel {number} has no advertised media type"
            ))
        })?;

        if let Some((_, records)) = groups.iter_mut().find(|(kind, _)| *kind == media) {
            records.push(record);
        } else {
            groups.push((media, vec![record]));
        }
    }

    let mut pages = Vec::new();

    for receiver_channel in clears {
        pages.push(SubscriptionCommand {
            command: json!({
                "command": "remove_subscriptions",
                "rx_channels": [receiver_channel],
            }),
            receiver_channels: vec![receiver_channel],
        });
    }

    for chunk in subscriptions.chunks(protocol.subscription_batch_limit) {
        pages.push(SubscriptionCommand {
            command: json!({
                "command": "add_subscriptions",
                "subscriptions": chunk.iter().map(|(_, value)| value).collect::<Vec<_>>(),
            }),
            receiver_channels: chunk.iter().map(|(number, _)| *number).collect(),
        });
    }

    for (media, records) in groups {
        for chunk in records.chunks(capacity) {
            let command = json!({
                "command": "modern_arc_subscription_page",
                "protocol_id": request.protocol_id,
                "page_capacity": capacity,
                "media_type_code": media,
                "records": chunk,
            });

            pages.push(SubscriptionCommand {
                command,
                receiver_channels: chunk
                    .iter()
                    .map(|record| match record {
                        SubscriptionPageEntry::Set { rx_channel, .. }
                        | SubscriptionPageEntry::Clear { rx_channel } => *rx_channel,
                    })
                    .collect(),
            });
        }
    }

    // No command escapes the planner until every page passes the actual encoder.
    for group in &pages {
        super::build_command_from_json(&group.command.to_string())?;
    }

    Ok(pages)
}
