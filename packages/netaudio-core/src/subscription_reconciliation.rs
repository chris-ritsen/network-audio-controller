use std::collections::HashSet;

use crate::spec::{
    plan_subscription_request, Receiver, SpecError, SubscriptionPageEntry, SubscriptionPlanRequest,
};
use crate::subscription_readback::{self, ExpectedSubscription, ObservedSubscription};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub protocol_id: u16,
    pub managed: bool,
    pub channels: Vec<Receiver>,
    pub subscriptions: Vec<ObservedSubscription>,
    pub expected: Vec<ExpectedSubscription>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Clear,
    Set,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Batch {
    pub action: Action,
    pub expected: Vec<ExpectedSubscription>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Plan {
    pub unchanged: Vec<ExpectedSubscription>,
    pub batches: Vec<Batch>,
}

/// Plan against a fresh inventory. Configuration equality does not imply a
/// connected source; connection completion remains a separate readback result.
pub fn plan(mut request: Request) -> Result<Plan, SpecError> {
    let protocol =
        crate::protocol::arc_protocol_for_identifier(request.protocol_id, request.managed)
            .map_err(|error| SpecError::InvalidJson(error.into()))?;

    for expected in &mut request.expected {
        if let Some([channel, device]) = &expected.source {
            expected.source = subscription_readback::source(Some(channel), Some(device))?;
        }
    }

    let readback = subscription_readback::evaluate_request(subscription_readback::Request {
        channels: request
            .channels
            .iter()
            .map(|channel| channel.number)
            .collect(),
        subscriptions: request.subscriptions,
        expected: request.expected.clone(),
    })?;
    let matched: HashSet<_> = readback
        .channels
        .iter()
        .filter(|channel| channel.matched)
        .map(|channel| channel.number)
        .collect();
    let (unchanged, pending): (Vec<_>, Vec<_>) = request
        .expected
        .into_iter()
        .partition(|expected| matched.contains(&expected.number));

    if !request.managed && !pending.is_empty() {
        let records: Vec<_> = pending
            .iter()
            .map(|expected| match &expected.source {
                None => SubscriptionPageEntry::Clear {
                    rx_channel: expected.number,
                },
                Some([channel, device]) => SubscriptionPageEntry::Set {
                    rx_channel: expected.number,
                    tx_channel: channel.clone(),
                    tx_device: device.clone(),
                },
            })
            .collect();

        // Validate the entire pending intent, including the final page, before
        // allowing the caller to execute the first batch.
        plan_subscription_request(SubscriptionPlanRequest {
            protocol_id: request.protocol_id,
            channels: request.channels,
            records,
        })?;
    }

    let mut batches = Vec::new();

    for clearing in [true, false] {
        let selected: Vec<_> = pending
            .iter()
            .filter(|expected| expected.source.is_none() == clearing)
            .cloned()
            .collect();

        for chunk in selected.chunks(protocol.subscription_batch_limit) {
            batches.push(Batch {
                action: if clearing { Action::Clear } else { Action::Set },
                expected: chunk.to_vec(),
            });
        }
    }

    Ok(Plan { unchanged, batches })
}
