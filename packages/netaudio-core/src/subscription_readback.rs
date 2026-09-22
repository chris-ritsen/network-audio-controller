use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::spec::SpecError;
use crate::subscription_status::{classification_for_identifier, decode};

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ObservedSubscription {
    pub number: Option<u16>,
    pub tx_channel: Option<String>,
    pub tx_device: Option<String>,
    pub status_code: Option<u16>,
    pub receiver_status_code: Option<u16>,
    pub managed_status: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ExpectedSubscription {
    pub number: u16,
    pub source: Option<[String; 2]>,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub channels: Vec<u16>,
    pub subscriptions: Vec<ObservedSubscription>,
    pub expected: Vec<ExpectedSubscription>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ChannelReadback {
    pub number: u16,
    pub source: Option<[String; 2]>,
    pub matched: bool,
    pub settled: bool,
    pub connection_state: &'static str,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SubscriptionReadback {
    pub matched: bool,
    pub settled: bool,
    pub channels: Vec<ChannelReadback>,
}

fn invalid(message: &str) -> SpecError {
    SpecError::InvalidJson(message.into())
}

pub(crate) fn source(
    channel: Option<&str>,
    device: Option<&str>,
) -> Result<Option<[String; 2]>, SpecError> {
    match (
        channel.filter(|value| !value.is_empty()),
        device.filter(|value| !value.is_empty()),
    ) {
        (None, None) => Ok(None),
        (Some(channel), Some(device)) => Ok(Some([channel.into(), device.into()])),
        _ => Err(invalid(
            "receiver subscription source is incomplete during readback",
        )),
    }
}

/// Evaluate one fresh inventory. The caller owns freshness, deadlines, and I/O;
/// configuration equality and terminal connection status are separate results.
pub fn evaluate(input: &str) -> Result<SubscriptionReadback, SpecError> {
    let request: Request =
        serde_json::from_str(input).map_err(|error| SpecError::InvalidJson(error.to_string()))?;

    evaluate_request(request)
}

pub fn evaluate_request(request: Request) -> Result<SubscriptionReadback, SpecError> {
    let mut receivers = HashSet::new();

    for number in request.channels {
        if number == 0 || !receivers.insert(number) {
            return Err(invalid(
                "receiver channel numbers must be nonzero and unique",
            ));
        }
    }

    let mut observed = HashMap::new();

    for subscription in request.subscriptions {
        let number = subscription
            .number
            .ok_or_else(|| invalid("subscription receiver identity is unavailable"))?;

        if !receivers.contains(&number) || observed.contains_key(&number) {
            return Err(invalid("subscription receiver is unavailable or ambiguous"));
        }

        let configured = source(
            subscription.tx_channel.as_deref(),
            subscription.tx_device.as_deref(),
        )?;
        let (state, settled) = if let Some(code) = subscription.status_code {
            let status = decode(code, subscription.receiver_status_code);
            (status.state, status.settled)
        } else {
            let status =
                classification_for_identifier(subscription.managed_status.as_deref().unwrap_or(""));
            (status.state, status.settled)
        };

        observed.insert(number, (configured, state, settled));
    }

    let mut channels = Vec::new();
    let mut requested = HashSet::new();

    for expected in request.expected {
        if !receivers.contains(&expected.number) || !requested.insert(expected.number) {
            return Err(invalid("requested receiver is unavailable or ambiguous"));
        }

        let desired = match expected.source {
            Some([channel, device]) => source(Some(&channel), Some(&device))?,
            None => None,
        };
        let (configured, state, terminal) = observed
            .remove(&expected.number)
            .unwrap_or((None, "none", true));
        let matched = configured == desired;
        let settled = matched && (configured.is_none() || terminal);

        channels.push(ChannelReadback {
            number: expected.number,
            source: configured,
            matched,
            settled,
            connection_state: state,
        });
    }

    Ok(SubscriptionReadback {
        matched: channels.iter().all(|channel| channel.matched),
        settled: channels.iter().all(|channel| channel.settled),
        channels,
    })
}
