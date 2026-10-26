use super::{Observation, counters::flow_map};
use crate::monitoring::journal::model::{Derivation, EventKind, Severity, Thresholds};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const BITS_PER_MEGABIT: f64 = 1_000_000.0;
const PERCENT: f64 = 100.0;

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub enum Condition {
    FlowLatency,
    InterfaceUtilization,
}

type Conditions = BTreeMap<(String, Condition, String), bool>;

pub fn observe(
    observation: &mut Observation<'_>,
    identity: &str,
    conditions: &mut Conditions,
    thresholds: Thresholds,
) -> Result<()> {
    if observation.current["online"] == false {
        return Ok(());
    }

    flow_latency(observation, identity, conditions, thresholds)?;
    interface_utilization(observation, identity, conditions, thresholds)?;
    Ok(())
}

fn flow_latency(
    observation: &mut Observation<'_>,
    identity: &str,
    conditions: &mut Conditions,
    thresholds: Thresholds,
) -> Result<()> {
    let health = &observation.current["receiver_flow_connection_health"];
    let flows = flow_map(health);

    if observation.current["receiver_flow_completeness"] == "complete" {
        let current: BTreeSet<_> = flows.keys().cloned().collect();
        conditions.retain(|(device, kind, flow), _| {
            device != identity || *kind != Condition::FlowLatency || current.contains(flow)
        });
    }

    for (flow_identity, flow) in flows {
        let series = &flow["latency"];
        let sample = &series["current"];
        let evidence = &sample["evidence"];
        let configured = evidence["configured_latency_nanoseconds"]
            .as_u64()
            .filter(|value| *value != 0);
        let latency = sample["value"]
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0);

        if matches!(
            observation.current["receiver_flow_completeness"].as_str(),
            Some("partial" | "unknown")
        ) || series["fresh"] != true
            || !sample["raw"].as_u64().is_some_and(|value| value != 0)
            || evidence["comparison_key"] != flow["evidence"]["comparison_key"]
        {
            continue;
        }

        let (Some(configured), Some(latency)) = (configured, latency) else {
            continue;
        };
        let warning = (configured as f64 * thresholds.latency_warning).round_ties_even();
        let recovery = (configured as f64 * thresholds.latency_recovery).round_ties_even();
        let key = (
            identity.to_owned(),
            Condition::FlowLatency,
            flow_identity.clone(),
        );
        let Some((previous, current)) = transition(conditions, key, latency, warning, recovery)
        else {
            continue;
        };
        let mut event = observation.event(
            if current {
                EventKind::ReceiverFlowLatencyHigh
            } else {
                EventKind::ReceiverFlowLatencyRecovered
            },
            if current {
                Severity::Warning
            } else {
                Severity::Info
            },
            json!(previous),
            json!(current),
            "heartbeat_receiver_flow_latency",
        )?;
        event.derivation_status = Derivation::Derived;
        event.flow_identity = Some(flow_identity);
        event.raw.extend([
            ("flow_measurement".to_owned(), flow.clone()),
            (
                "configured_latency_nanoseconds".to_owned(),
                json!(configured),
            ),
            (
                "warning_threshold_nanoseconds".to_owned(),
                json!(warning as u64),
            ),
            (
                "recovery_threshold_nanoseconds".to_owned(),
                json!(recovery as u64),
            ),
            ("latency_observation".to_owned(), sample.clone()),
            (
                "source_records".to_owned(),
                health["source_records"].clone(),
            ),
        ]);
        observation.events.push(event);
    }

    Ok(())
}

fn interface_utilization(
    observation: &mut Observation<'_>,
    identity: &str,
    conditions: &mut Conditions,
    thresholds: Thresholds,
) -> Result<()> {
    let Some(speed) =
        nonnegative(&observation.current["link_speed_mbps"]).filter(|value| *value > 0.0)
    else {
        return Ok(());
    };
    let traffic = &observation.current["network_interface_traffic"];
    let capacity = speed * BITS_PER_MEGABIT;

    for (index, interface) in traffic["interfaces"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let (Some(transmit), Some(receive)) = (
            nonnegative(&interface["transmit_rate_bits_per_second"]),
            nonnegative(&interface["receive_rate_bits_per_second"]),
        ) else {
            continue;
        };
        let utilization = transmit.max(receive) / capacity * PERCENT;
        let interface_identity = format!("interface:{}", index + 1);
        let key = (
            identity.to_owned(),
            Condition::InterfaceUtilization,
            interface_identity.clone(),
        );
        let Some((previous, current)) = transition(
            conditions,
            key,
            utilization,
            thresholds.utilization_warning,
            thresholds.utilization_recovery,
        ) else {
            continue;
        };
        let mut event = observation.event(
            if current {
                EventKind::InterfaceUtilizationHigh
            } else {
                EventKind::InterfaceUtilizationRecovered
            },
            if current {
                Severity::Warning
            } else {
                Severity::Info
            },
            json!(previous),
            json!(current),
            "heartbeat_interface_statistics",
        )?;
        event.derivation_status = Derivation::Derived;
        event.interface_identity = Some(interface_identity);
        event.raw.extend([
            ("interface_measurement".to_owned(), interface.clone()),
            ("link_speed_mbps".to_owned(), json!(speed)),
            ("utilization_percent".to_owned(), json!(utilization)),
            (
                "warning_threshold_percent".to_owned(),
                json!(thresholds.utilization_warning),
            ),
            (
                "recovery_threshold_percent".to_owned(),
                json!(thresholds.utilization_recovery),
            ),
            ("traffic_sequence".to_owned(), traffic["sequence"].clone()),
        ]);
        observation.events.push(event);
    }

    Ok(())
}

fn nonnegative(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
}

fn transition(
    conditions: &mut Conditions,
    key: (String, Condition, String),
    value: f64,
    warning: f64,
    recovery: f64,
) -> Option<(bool, bool)> {
    let previous = conditions.get(&key).copied();
    let current = match previous {
        Some(true) => value > recovery,
        _ => value >= warning,
    };
    conditions.insert(key, current);
    previous
        .filter(|previous| *previous != current)
        .map(|previous| (previous, current))
}
