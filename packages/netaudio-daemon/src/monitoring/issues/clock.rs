use super::model::{EvidenceClass, Finding, Issue, Kind, Scope};
use crate::monitoring::journal::model::{Severity, device_identity};
use anyhow::{Context, Result};
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const CLOCK_FRESHNESS_SECONDS: i64 = 5;
const CLOCK_RECOVERY_SECONDS: i64 = 5;
const PANEL_FRESHNESS_SECONDS: f64 = 10.0;
const BLUETOOTH_LOST_STATE: u64 = 3;
const BLUETOOTH_CONNECTED_STATES: &[u64] = &[1, 2];
const VIDEO_HEALTHY_STATES: &[u64] = &[16, 17];
const VIDEO_FAILURE_STATES: &[u64] = &[32, 33, 34, 36, 48, 49, 50, 51, 52, 53, 54];

#[derive(Default)]
pub struct Recovery {
    observations: BTreeMap<(String, Kind), (DateTime<Utc>, DateTime<Utc>)>,
    recovered: BTreeSet<(String, Kind)>,
}

impl Recovery {
    pub fn forget(&mut self, identity: &str) {
        self.observations
            .retain(|(device, _), _| device != identity);
        self.recovered.retain(|(device, _)| device != identity);
    }

    pub fn confirmed(&self, issue: &Issue) -> bool {
        self.recovered
            .contains(&(issue.scope.device_identity.clone(), issue.kind))
    }

    pub fn detect(
        &mut self,
        record: &Value,
        timestamp: &str,
        active: &BTreeMap<String, Issue>,
    ) -> Result<Vec<Issue>> {
        let mut issues = variation(record, timestamp)?;
        let identity = device_identity(record)?;
        if record["ddm_clocking_state"].is_object() {
            issues.extend(managed(record, timestamp)?);
            self.forget(&identity);
            return Ok(issues);
        }

        let Some(observed) = fresh_clock(record, timestamp) else {
            self.forget(&identity);
            return Ok(issues);
        };
        let status = &record["clock_status"];
        for (kind, bad, good, title) in [
            (
                Kind::ClockSynchronization,
                status["synchronization"] == "lost",
                status["synchronization"] == "synchronized",
                "Clock synchronization lost",
            ),
            (
                Kind::ClockMuted,
                status["mute_flags"]
                    .as_u64()
                    .is_some_and(|value| value != 0),
                status["mute_flags"] == 0,
                "Audio muted by clock state",
            ),
        ] {
            let key = (identity.clone(), kind);
            self.recovered.remove(&key);

            if bad {
                self.observations.remove(&key);
                let finding = Finding {
                    kind,
                    severity: Severity::Error,
                    title: title.to_owned(),
                    summary: if kind == Kind::ClockMuted {
                        status["mute_reasons"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("; ")
                    } else {
                        "The device reports synchronization loss.".to_owned()
                    },
                    raw: json!({"clock_status": status, "clock_observed_at": record["clock_observed_at"]}),
                    source: "device_clock_status",
                    evidence_class: if kind == Kind::ClockMuted {
                        EvidenceClass::DirectObservation
                    } else {
                        EvidenceClass::DerivedState
                    },
                    remediation: "Check the clock source and network connections.",
                };
                issues.push(Issue::candidate(
                    Scope::device(record)?,
                    timestamp,
                    finding,
                )?);
            } else if good {
                let (mut first, last) = self
                    .observations
                    .get(&key)
                    .copied()
                    .unwrap_or((observed, observed));
                if observed < last || observed - last > TimeDelta::seconds(CLOCK_FRESHNESS_SECONDS)
                {
                    first = observed;
                }
                self.observations.insert(key.clone(), (first, observed));
                if observed - first >= TimeDelta::seconds(CLOCK_RECOVERY_SECONDS) {
                    self.recovered.insert(key);
                } else if let Some(previous) = active
                    .values()
                    .find(|issue| issue.scope.device_identity == identity && issue.kind == kind)
                {
                    let mut current = previous.clone();
                    current.observation_state = super::model::ObservationState::Observed;
                    current.summary =
                        "Recovering; waiting for five seconds of continuous healthy clock status."
                            .to_owned();
                    current.raw_source_fields = json!({"clock_status": status, "clock_observed_at": record["clock_observed_at"]});
                    issues.push(current);
                }
            } else {
                self.observations.remove(&key);
            }
        }
        Ok(issues)
    }
}

pub fn fresh_clock(record: &Value, timestamp: &str) -> Option<DateTime<Utc>> {
    if record["online"] == false
        || record["clock_status"]["status_supported"] != true
        || record["clock_status"]["observation_state"] == "stale"
    {
        return None;
    }
    let observed = DateTime::parse_from_rfc3339(record["clock_observed_at"].as_str()?)
        .ok()?
        .with_timezone(&Utc);
    let now = DateTime::parse_from_rfc3339(timestamp)
        .ok()?
        .with_timezone(&Utc);
    let age = now - observed;
    (age >= TimeDelta::zero() && age <= TimeDelta::seconds(CLOCK_FRESHNESS_SECONDS))
        .then_some(observed)
}

fn managed(record: &Value, timestamp: &str) -> Result<Vec<Issue>> {
    let managed = &record["ddm_clocking_state"];
    let locked = &managed["locked"];
    let bad = locked == false
        || locked.as_str().is_some_and(|state| {
            !matches!(state.to_lowercase().as_str(), "locked" | "true" | "ok")
        });
    if !bad && managed["follower_without_leader"] != true {
        return Ok(Vec::new());
    }
    let (title, summary, remediation) = if bad {
        (
            "Clock is not synchronized",
            format!("Managed clock lock state is {locked}"),
            "Inspect leader availability, clock paths and pull-up settings.",
        )
    } else {
        (
            "Follower has no leader",
            "The managed clocking state reports follower_without_leader".to_owned(),
            "Restore a reachable leader and inspect the clock domain configuration.",
        )
    };
    Ok(vec![Issue::candidate(
        Scope::device(record)?,
        timestamp,
        Finding {
            kind: Kind::ClockSynchronization,
            severity: Severity::Error,
            title: title.to_owned(),
            summary,
            raw: json!({"ddm_clocking_state": managed}),
            source: "ddm_clocking_state",
            evidence_class: EvidenceClass::DirectObservation,
            remediation,
        },
    )?])
}

fn variation(record: &Value, timestamp: &str) -> Result<Vec<Issue>> {
    let clock = &record["clock_observations"];
    let mut issues = Vec::new();
    if clock["warning_enabled"] != true {
        return Ok(issues);
    }
    for (source, policy) in clock["variation"].as_object().into_iter().flatten() {
        if policy["active"] != true || policy["observable"] != true {
            continue;
        }
        let mut scope = Scope::device(record)?;
        scope.interface_identity = Some(source.clone());
        issues.push(Issue::candidate(
            scope,
            timestamp,
            Finding {
                kind: Kind::ClockFrequencyVariation,
                severity: Severity::Warning,
                title: "Clock frequency variation".to_owned(),
                summary: format!("{source} frequency variation exceeds the local threshold"),
                raw: json!({"policy": policy, "observations": clock[source]["history"]}),
                source: "clock_frequency_statistics",
                evidence_class: EvidenceClass::Inference,
                remediation: "Inspect clock-source and network stability.",
            },
        )?);
    }
    Ok(issues)
}

pub fn panel(record: &Value, timestamp: &str) -> Result<Vec<Issue>> {
    let mut issues = Vec::new();
    for (category, kind, title, field) in [
        (
            "bluetooth_connection",
            Kind::BluetoothLinkLost,
            "Bluetooth link lost",
            "state",
        ),
        (
            "video_channel",
            Kind::VideoSignalFailure,
            "Video signal unavailable",
            "status_code",
        ),
    ] {
        let Some(observation) = fresh_panel(record, category, timestamp)? else {
            continue;
        };
        let Some(value) = observation["value"][field].as_u64() else {
            continue;
        };
        let bad = if kind == Kind::BluetoothLinkLost {
            value == BLUETOOTH_LOST_STATE
        } else {
            VIDEO_FAILURE_STATES.contains(&value)
        };
        if bad {
            let finding = Finding {
                kind,
                severity: Severity::Warning,
                title: title.to_owned(),
                summary: format!("Device reports {} {value}.", field.replace('_', " ")),
                raw: json!({"category": category, field: value, "observed_at_unix": observation["observed_at_unix"]}),
                source: "device_control_status",
                evidence_class: EvidenceClass::DirectObservation,
                remediation: "Check the connected peer or video source and its device settings.",
            };
            issues.push(Issue::candidate(
                Scope::device(record)?,
                timestamp,
                finding,
            )?);
        }
    }
    Ok(issues)
}

fn fresh_panel<'a>(
    record: &'a Value,
    category: &str,
    timestamp: &str,
) -> Result<Option<&'a Value>> {
    let observation = &record["device_controls"]["observations"][category];
    if observation["available"] != true || observation["fresh"] == false {
        return Ok(None);
    }
    let now = DateTime::parse_from_rfc3339(timestamp)
        .context("invalid issue observation time")?
        .timestamp_micros() as f64
        / 1_000_000.0;
    Ok(observation["observed_at_unix"]
        .as_f64()
        .filter(|observed| (0.0..=PANEL_FRESHNESS_SECONDS).contains(&(now - observed)))
        .map(|_| observation))
}

pub fn panel_recovered(issue: &Issue, record: &Value, timestamp: &str) -> Result<bool> {
    let (category, field, states) = match issue.kind {
        Kind::BluetoothLinkLost => ("bluetooth_connection", "state", BLUETOOTH_CONNECTED_STATES),
        Kind::VideoSignalFailure => ("video_channel", "status_code", VIDEO_HEALTHY_STATES),
        _ => return Ok(false),
    };
    Ok(fresh_panel(record, category, timestamp)?
        .and_then(|observation| observation["value"][field].as_u64())
        .is_some_and(|value| states.contains(&value)))
}
