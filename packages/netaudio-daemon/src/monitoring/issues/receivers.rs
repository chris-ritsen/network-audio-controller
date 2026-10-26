use super::model::{EvidenceClass, Finding, Issue, Kind, Scope};
use crate::monitoring::journal::model::{Severity, Thresholds};
use crate::monitoring::journal::observe::{flow_map, subscription_failure, subscription_map};
use anyhow::Result;
use caseless::Caseless;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub fn detect(
    record: &Value,
    timestamp: &str,
    active: &BTreeMap<String, Issue>,
    snapshots: &BTreeMap<String, Arc<Value>>,
    thresholds: Thresholds,
) -> Result<Vec<Issue>> {
    let mut issues = Vec::new();
    for (identity, subscription) in subscription_map(&record["subscriptions"]) {
        if subscription_failure(subscription) != Some(true) || waiting(subscription, snapshots) {
            continue;
        }
        let status = &subscription["status"];
        let mut scope = Scope::device(record)?;
        scope.channel_identity = Some(identity);
        let finding = Finding {
            kind: Kind::SubscriptionFailure,
            severity: if status["severity"] == "error" {
                Severity::Error
            } else {
                Severity::Warning
            },
            title: "Receiver subscription is not healthy".to_owned(),
            summary: status["detail"]
                .as_str()
                .or_else(|| status["status"].as_str())
                .unwrap_or("receiver reports a failure")
                .to_owned(),
            raw: subscription.clone(),
            source: "receiver_subscription_status",
            evidence_class: EvidenceClass::DirectObservation,
            remediation: "Inspect the source identity, format, clock domain and network path before changing routing.",
        };
        issues.push(Issue::candidate(scope, timestamp, finding)?);
    }

    for (identity, flow) in flow_map(&record["receiver_flow_connection_health"]) {
        for (metric, kind, title) in [
            (
                "latency",
                Kind::ReceiverLatencyPressure,
                "Receiver latency budget pressure",
            ),
            (
                "late_packets",
                Kind::ReceiverLatePackets,
                "Receiver late packets increased",
            ),
        ] {
            let Some(sample) = comparable(flow, metric) else {
                continue;
            };
            let mut scope = Scope::device(record)?;
            scope.flow_identity = Some(identity.clone());
            scope.interface_identity = Some(format!("network:{}", flow["network_interface_index"]));
            let bad = if kind == Kind::ReceiverLatencyPressure {
                let previous = active.values().any(|issue| {
                    issue.kind == kind
                        && issue.scope.device_identity == scope.device_identity
                        && issue.scope.flow_identity == scope.flow_identity
                });
                let threshold = if previous {
                    thresholds.latency_recovery
                } else {
                    thresholds.latency_warning
                };
                latency(record, sample).is_some_and(|(value, budget)| {
                    if previous {
                        value > budget * threshold
                    } else {
                        value >= budget * threshold
                    }
                })
            } else {
                flow[metric]["delta"]
                    .as_u64()
                    .is_some_and(|delta| delta > 0)
            };
            if bad {
                issues.push(Issue::candidate(
                    scope,
                    timestamp,
                    Finding {
                        kind,
                        severity: Severity::Warning,
                        title: title.to_owned(),
                        summary: format!(
                            "Audio flow {}, network {}",
                            flow["audio_receiver_flow_id"],
                            flow["network_interface_index"].as_u64().unwrap_or_default() + 1
                        ),
                        raw: json!({"path": flow, "observation": sample}),
                        source: "receiver_path_telemetry",
                        evidence_class: EvidenceClass::DerivedState,
                        remediation: "Inspect the source and receiver network path.",
                    },
                )?);
            }
        }

        let degraded = [
            "status",
            "severity",
            "connected",
            "healthy",
            "error",
            "error_code",
        ]
        .into_iter()
        .any(|field| match field {
            "connected" | "healthy" => flow[field] == false,
            "error" | "error_code" => {
                !matches!(&flow[field], Value::Null | Value::Bool(false))
                    && flow[field] != 0
                    && flow[field] != ""
            }
            _ => flow[field].as_str().is_some_and(|value| {
                matches!(
                    value.to_lowercase().as_str(),
                    "degraded" | "error" | "failed" | "warning"
                )
            }),
        });
        let fresh = ["latency", "late_packets"]
            .iter()
            .any(|metric| comparable(flow, metric).is_some());
        if degraded && fresh {
            let mut scope = Scope::device(record)?;
            scope.flow_identity = Some(identity.clone());
            let finding = Finding {
                kind: Kind::ReceiverHealthDegraded,
                severity: Severity::Warning,
                title: "Receiver flow health is degraded".to_owned(),
                summary: format!("Receiver flow {identity} reports degraded state"),
                raw: json!({"flow": flow, "health": record["receiver_flow_connection_health"]}),
                source: "receiver_flow_connection_health",
                evidence_class: EvidenceClass::DirectObservation,
                remediation: "Inspect receiver status and path counters; keep subscription status and flow health separate.",
            };
            issues.push(Issue::candidate(scope, timestamp, finding)?);
        }
    }
    Ok(issues)
}

pub fn waiting(subscription: &Value, snapshots: &BTreeMap<String, Arc<Value>>) -> bool {
    if matches!(
        subscription["status"]["status"].as_str(),
        Some("UNRESOLVED" | "RESOLVED_NONE")
    ) {
        return true;
    }
    let Some(source) = subscription["tx_device"]
        .as_str()
        .filter(|source| !source.is_empty())
    else {
        return false;
    };
    snapshots
        .values()
        .any(|record| record["online"] == false && matches_source(record, source))
}

pub fn matches_source(record: &Value, source: &str) -> bool {
    ["name", "server_name"].iter().any(|field| {
        record[*field].as_str().is_some_and(|name| {
            name.chars()
                .default_case_fold()
                .eq(source.chars().default_case_fold())
        })
    })
}

pub fn comparable<'a>(flow: &'a Value, metric: &str) -> Option<&'a Value> {
    let series = &flow[metric];
    let sample = &series["current"];
    let key = &flow["evidence"]["comparison_key"];
    (series["fresh"] == true && !key.is_null() && sample["evidence"]["comparison_key"] == *key)
        .then_some(sample)
}

fn latency(record: &Value, sample: &Value) -> Option<(f64, f64)> {
    if matches!(
        record["receiver_flow_completeness"].as_str(),
        Some("partial" | "unknown")
    ) || !sample["raw"].as_u64().is_some_and(|value| value > 0)
    {
        return None;
    }
    let budget = sample["evidence"]["configured_latency_nanoseconds"]
        .as_f64()
        .filter(|budget| *budget > 0.0)?;
    let value = sample["value"].as_f64().filter(|value| *value >= 0.0)?;
    Some((value, budget))
}

pub fn recovered(issue: &Issue, record: &Value, thresholds: Thresholds) -> bool {
    if issue.kind == Kind::SubscriptionFailure {
        return issue
            .scope
            .channel_identity
            .as_ref()
            .and_then(|identity| {
                subscription_map(&record["subscriptions"])
                    .get(identity)
                    .copied()
            })
            .is_some_and(|subscription| {
                subscription_failure(subscription) == Some(false)
                    || subscription["status"]["state"] == "none"
            });
    }
    let flows = flow_map(&record["receiver_flow_connection_health"]);
    if matches!(
        issue.kind,
        Kind::ReceiverLatencyPressure | Kind::ReceiverLatePackets
    ) && matches!(
        record["receiver_flow_completeness"].as_str(),
        Some("partial" | "unknown")
    ) {
        return false;
    }
    let Some(flow) = issue
        .scope
        .flow_identity
        .as_ref()
        .and_then(|identity| flows.get(identity))
    else {
        return false;
    };
    match issue.kind {
        Kind::ReceiverHealthDegraded => {
            let previous = &issue.raw_source_fields["flow"];
            let fields = [
                "status",
                "severity",
                "connected",
                "healthy",
                "error",
                "error_code",
            ];
            fields
                .iter()
                .filter(|field| !previous[**field].is_null())
                .all(|field| match *field {
                    "connected" | "healthy" => flow[*field] == true,
                    "error" | "error_code" => {
                        flow[*field] == false || flow[*field] == 0 || flow[*field] == ""
                    }
                    _ => flow[*field].as_str().is_some_and(|state| {
                        matches!(state, "ok" | "healthy" | "info" | "connected")
                    }),
                })
                && ["latency", "late_packets"]
                    .iter()
                    .any(|metric| comparable(flow, metric).is_some())
        }
        Kind::ReceiverLatePackets => {
            comparable(flow, "late_packets").is_some() && flow["late_packets"]["delta"] == 0
        }
        Kind::ReceiverLatencyPressure => comparable(flow, "latency")
            .and_then(|sample| latency(record, sample))
            .is_some_and(|(value, budget)| value <= budget * thresholds.latency_recovery),
        _ => false,
    }
}
