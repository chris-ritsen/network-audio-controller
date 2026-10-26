mod clock;
mod configuration;
pub mod model;
mod network;
mod receivers;
mod resolution;
pub mod store;

use crate::monitoring::journal::model::{JournalEvent, Thresholds, device_identity};
use anyhow::{Context, Result};
use model::{Issue, Kind, Lifecycle, ObservationState};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const CONFIGURATION_FIELDS: &[&str] = &[
    "is_licensed",
    "license_valid",
    "ddm_status",
    "availability_state",
    "safe_mode",
    "upgrade_required",
    "interface_reboot_required",
    "dante_redundancy",
    "configuration_partition_status",
    "persistence_status",
    "failed_queries",
    "error",
    "receiver_flow_connection_health",
    "network_interface_traffic",
    "requested_sample_rate",
    "sample_rate_hz",
    "requested_encoding",
    "encoding",
    "requested_sample_rate_pullup_raw_value",
    "sample_rate_pullup_raw_value",
    "aes67_configured",
    "aes67_current",
    "interfaces",
];
const CLOCK_FIELDS: &[&str] = &[
    "clock_status",
    "clock_observed_at",
    "clock_observations",
    "ddm_clocking_state",
];
const RECEIVER_FIELDS: &[&str] = &[
    "subscriptions",
    "receiver_flow_connection_health",
    "receiver_flow_completeness",
];
const CROSS_FIELDS: &[&str] = &[
    "interfaces",
    "sample_rate_pullup_raw_value",
    "ptpv1_master_uuid",
    "clock_subdomain",
    "management_state",
    "ddm_domain_id",
    "ddm_server_profile",
];

pub struct Change {
    pub issue: Issue,
    pub event: Option<JournalEvent>,
}

pub struct Engine {
    snapshots: BTreeMap<String, Arc<Value>>,
    active: BTreeMap<String, Issue>,
    clock: clock::Recovery,
    thresholds: Thresholds,
}

impl Engine {
    pub fn new(thresholds: Thresholds, restored: Vec<Issue>) -> Self {
        let active = restored
            .into_iter()
            .filter(|issue| issue.state == Lifecycle::Open)
            .map(|mut issue| {
                issue.observation_state = ObservationState::Unobservable;
                (issue.issue_id.clone(), issue)
            })
            .collect();
        Self {
            snapshots: BTreeMap::new(),
            active,
            clock: clock::Recovery::default(),
            thresholds,
        }
    }

    pub fn observe(&mut self, record: Arc<Value>, timestamp: &str) -> Result<Vec<Change>> {
        let identity = device_identity(&record)?;
        let previous = self.snapshots.insert(identity.clone(), Arc::clone(&record));
        if record["online"] == false {
            self.clock.forget(&identity);
            return self.unobservable(&identity, timestamp);
        }
        let previous = previous
            .as_deref()
            .filter(|previous| previous["online"] != false);
        let changed = |fields: &[&str]| {
            previous.is_none_or(|previous| {
                ["name", "server_name", "device_identity"]
                    .iter()
                    .chain(fields.iter())
                    .any(|field| previous[*field] != record[*field])
            })
        };
        let mut candidates = Vec::new();
        let mut local_kinds = BTreeSet::new();

        if changed(CONFIGURATION_FIELDS) {
            candidates.extend(configuration::detect(&record, timestamp)?);
            local_kinds.extend([
                Kind::LicensingFailure,
                Kind::SafeState,
                Kind::UpgradeRequired,
                Kind::RebootRequired,
                Kind::ConfigurationPartitionFault,
                Kind::PersistenceFault,
                Kind::TelemetryStale,
                Kind::TelemetryMissing,
                Kind::ConfigurationDivergence,
            ]);
        }
        if changed(CLOCK_FIELDS) {
            candidates.extend(self.clock.detect(&record, timestamp, &self.active)?);
            local_kinds.extend([
                Kind::ClockSynchronization,
                Kind::ClockMuted,
                Kind::ClockFrequencyVariation,
            ]);
        }
        if changed(&["device_controls"]) {
            candidates.extend(clock::panel(&record, timestamp)?);
            local_kinds.extend([Kind::BluetoothLinkLost, Kind::VideoSignalFailure]);
        }
        if changed(RECEIVER_FIELDS) {
            candidates.extend(receivers::detect(
                &record,
                timestamp,
                &self.active,
                &self.snapshots,
                self.thresholds,
            )?);
            local_kinds.extend([
                Kind::SubscriptionFailure,
                Kind::ReceiverHealthDegraded,
                Kind::ReceiverLatencyPressure,
                Kind::ReceiverLatePackets,
            ]);
        }
        if changed(&["interfaces"]) {
            candidates.extend(network::local(&record, timestamp)?);
            local_kinds.insert(Kind::SubnetConflict);
        }

        let cross_changed = changed(CROSS_FIELDS)
            || previous.and_then(network::clock_group) != network::clock_group(&record);
        if cross_changed {
            candidates.extend(
                network::cross(&self.snapshots, timestamp)?
                    .into_iter()
                    .filter(|issue| resolution::depends_on(issue, &identity)),
            );
        }
        let candidates = candidates
            .into_iter()
            .map(|issue| (issue.issue_id.clone(), issue))
            .collect::<BTreeMap<_, _>>();
        let relevant = self
            .active
            .iter()
            .filter(|(_, issue)| {
                issue.scope.device_identity == identity && local_kinds.contains(&issue.kind)
                    || cross_changed
                        && matches!(issue.kind, Kind::AddressConflict | Kind::PullupMismatch)
                        && resolution::depends_on(issue, &identity)
            })
            .map(|(identity, _)| identity.clone())
            .collect::<BTreeSet<_>>();
        let mut changes = Vec::new();

        for (identifier, mut candidate) in candidates
            .iter()
            .map(|(identity, issue)| (identity.clone(), issue.clone()))
        {
            let previous = self.active.get(&identifier);
            if let Some(previous) = previous {
                candidate.first_seen.clone_from(&previous.first_seen);
                candidate.occurrence_count = previous.occurrence_count;
                candidate.last_seen.clone_from(&previous.last_seen);

                if candidate.raw_source_fields != previous.raw_source_fields
                    || previous.observation_state == ObservationState::Unobservable
                {
                    candidate.occurrence_count = candidate
                        .occurrence_count
                        .checked_add(1)
                        .context("issue occurrence counter exhausted")?;
                    candidate.last_seen = timestamp.to_owned();
                }
            }

            let event = if previous.is_none_or(|previous| !candidate.materially_matches(previous)) {
                Some(candidate.event(previous, timestamp)?)
            } else {
                None
            };
            let changed = event.is_some()
                || previous.is_none_or(|previous| {
                    candidate.last_seen != previous.last_seen
                        || candidate.raw_source_fields != previous.raw_source_fields
                });
            if changed {
                changes.push(Change {
                    issue: candidate.clone(),
                    event,
                });
            }
            self.active.insert(identifier, candidate);
        }

        for identifier in relevant
            .into_iter()
            .filter(|identifier| !candidates.contains_key(identifier))
        {
            let previous = self
                .active
                .get(&identifier)
                .context("active issue disappeared during reconciliation")?;
            let resolved = resolution::recovered(
                previous,
                &self.snapshots,
                &self.clock,
                self.thresholds,
                timestamp,
            )?;
            let mut current = previous.clone();
            if resolved {
                current.state = Lifecycle::Resolved;
                current.observation_state = ObservationState::Observed;
                current.resolved_at = Some(timestamp.to_owned());
            } else if current.observation_state == ObservationState::Observed {
                current.observation_state = ObservationState::Unobservable;
            } else {
                continue;
            }

            let event = current.event(Some(previous), timestamp)?;
            changes.push(Change {
                issue: current.clone(),
                event: Some(event),
            });
            if resolved {
                self.active.remove(&identifier);
            } else {
                self.active.insert(identifier, current);
            }
        }
        Ok(changes)
    }

    pub fn remove(&mut self, identity: &str, timestamp: &str) -> Result<Vec<Change>> {
        let changes = self.unobservable(identity, timestamp)?;
        self.snapshots.remove(identity);
        self.clock.forget(identity);
        Ok(changes)
    }

    pub fn reset(&mut self, timestamp: &str) -> Result<Vec<Change>> {
        self.snapshots.clear();
        self.clock = clock::Recovery::default();
        let identities = self
            .active
            .values()
            .map(|issue| issue.scope.device_identity.clone())
            .collect::<BTreeSet<_>>();
        let mut changes = Vec::new();
        for identity in identities {
            changes.extend(self.unobservable(&identity, timestamp)?);
        }
        Ok(changes)
    }

    fn unobservable(&mut self, identity: &str, timestamp: &str) -> Result<Vec<Change>> {
        let mut changes = Vec::new();
        let record = self.snapshots.get(identity);
        for issue in self.active.values_mut().filter(|issue| {
            resolution::depends_on(issue, identity)
                || issue.kind == Kind::SubscriptionFailure
                    && record.is_some_and(|record| {
                        issue.raw_source_fields["tx_device"]
                            .as_str()
                            .is_some_and(|source| receivers::matches_source(record, source))
                    })
        }) {
            if issue.observation_state == ObservationState::Unobservable {
                continue;
            }
            let previous = issue.clone();
            issue.observation_state = ObservationState::Unobservable;
            changes.push(Change {
                issue: issue.clone(),
                event: Some(issue.event(Some(&previous), timestamp)?),
            });
        }
        Ok(changes)
    }
}
