use super::{
    Error,
    configuration::{Context, Server},
};
use chrono::{DateTime, Utc};
use netaudio_core::managed_api::{
    Issue, Response,
    inventory::{Observation, primary_mac},
    models::{Device, Domain, Inventory},
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Default)]
pub struct InventoryState {
    domains: BTreeMap<String, Domain>,
    unenrolled: BTreeMap<String, Device>,
    superseded: BTreeSet<String>,
    domains_observed: Option<ObservationTime>,
    unenrolled_observed: Option<ObservationTime>,
    domains_current: bool,
    unenrolled_current: bool,
    authoritative: bool,
    last_attempt: Option<DateTime<Utc>>,
    last_success: Option<DateTime<Utc>>,
    last_error: Option<String>,
    errors: Vec<Issue>,
    signal_membership: SignalMembership,
}

#[derive(Clone, Default)]
pub(super) struct SignalMembership {
    pub revision: u64,
    pub observed_at: Option<DateTime<Utc>>,
    pub domains: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

#[derive(Clone, Copy)]
struct ObservationTime {
    wall: DateTime<Utc>,
    deadline: Instant,
}

impl InventoryState {
    pub fn attempting(&mut self) {
        self.last_attempt = Some(Utc::now());
    }

    pub fn fail(&mut self, error: &Error) {
        self.domains_current = false;
        self.unenrolled_current = false;
        self.authoritative = false;
        self.last_error = Some(error.to_string());
        self.errors.clear();
    }

    pub fn apply(
        &mut self,
        response: Response,
        inventory: Option<Inventory>,
        freshness: Duration,
    ) -> Result<bool, Error> {
        let Some(inventory) = inventory else {
            self.fail(&Error::Response("Managed API returned no inventory data"));
            self.errors = response.errors;
            return Ok(false);
        };
        let complete_membership = inventory.domains.as_ref().is_some_and(|domains| {
            domains.iter().all(|domain| {
                domain.as_ref().is_some_and(|domain| {
                    domain
                        .devices
                        .as_ref()
                        .is_some_and(|devices| devices.iter().all(Option::is_some))
                })
            })
        });
        let domains = inventory
            .domains
            .filter(|_| complete_membership)
            .map(domain_map)
            .transpose()?;
        let unenrolled = inventory.unenrolled_devices.map(device_map).transpose()?;
        let now = ObservationTime {
            wall: Utc::now(),
            deadline: Instant::now() + freshness,
        };
        self.domains_current = domains.is_some();
        self.unenrolled_current = unenrolled.is_some();
        if let Some(domains) = domains {
            self.domains = domains;
            self.domains_observed = Some(now);
        }
        if let Some(unenrolled) = unenrolled {
            self.unenrolled = unenrolled;
            self.unenrolled_observed = Some(now);
        }
        self.authoritative =
            response.successful() && self.domains_current && self.unenrolled_current;
        if self.authoritative {
            self.last_success = Some(now.wall);
        }
        if self.authoritative && complete_membership {
            self.signal_membership = SignalMembership {
                observed_at: Some(now.wall),
                revision: self.signal_membership.revision.checked_add(1)
                    .ok_or(Error::Conflict("Managed signal membership revision exhausted"))?,
                domains: self.domains.iter().map(|(identifier, domain)| {
                    let targets = domain.devices.iter().flatten().flatten()
                        .filter(|device| device.connection.as_ref().and_then(|connection| connection.state.as_deref())
                            .is_some_and(|state| state.eq_ignore_ascii_case("READY")))
                        .map(|device| {
                            let identifier = netaudio_core::device_identity::managed_device_id(&device.id)
                                .or_else(|| primary_mac(device).and_then(|address| {
                                    netaudio_core::device_identity::normalize(
                                        netaudio_core::device_identity::DeviceIdentityRequest::ManagedPrimary(json!(address)))
                                }));
                            (device.id.clone(), identifier)
                        }).collect();
                    (identifier.clone(), targets)
                }).collect(),
            };
        }
        self.last_error = (!response.errors.is_empty()).then(|| {
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        });
        self.errors = response.errors;
        self.update_superseded();
        Ok(true)
    }

    pub fn fresh(&self) -> bool {
        self.authoritative && self.domains_fresh() && self.unenrolled_fresh()
    }

    pub(super) fn signal_membership(&self) -> &SignalMembership {
        &self.signal_membership
    }

    fn domains_fresh(&self) -> bool {
        self.domains_current
            && self
                .domains_observed
                .is_some_and(|time| time.deadline > Instant::now())
    }

    fn unenrolled_fresh(&self) -> bool {
        self.unenrolled_current
            && self
                .unenrolled_observed
                .is_some_and(|time| time.deadline > Instant::now())
    }

    pub fn deadline_after(&self, now: Instant) -> Option<Instant> {
        [
            (self.domains_current, self.domains_observed),
            (self.unenrolled_current, self.unenrolled_observed),
        ]
        .into_iter()
        .filter_map(|(current, time)| {
            time.filter(|time| current && time.deadline > now)
                .map(|time| time.deadline)
        })
        .min()
    }

    pub fn observations(
        &self,
        server: &str,
        contexts: &BTreeMap<String, Context>,
    ) -> Vec<Observation> {
        let mut observations = BTreeMap::new();
        for domain in self.domains.values() {
            let context = contexts
                .iter()
                .find(|(_, context)| context.server == server && context.domain_id == domain.id);
            for device in domain.devices.iter().flatten().flatten() {
                observations.insert(
                    device.id.clone(),
                    Observation {
                        device: device.clone(),
                        domain_id: Some(domain.id.clone()),
                        domain_name: domain.name.clone(),
                        observed_at: self.domains_observed.map(|time| seconds(time.wall)),
                        fresh: self.domains_fresh(),
                        server_profile: server.into(),
                        context_name: context.map(|(name, _)| name.clone()),
                        operation_permissions: context
                            .and_then(|(_, context)| context.operation_permissions.clone()),
                    },
                );
            }
        }
        for device in self.unenrolled.values() {
            if observations
                .get(&device.id)
                .is_some_and(|existing| existing.fresh || !self.unenrolled_fresh())
            {
                continue;
            }
            observations.insert(
                device.id.clone(),
                Observation {
                    device: device.clone(),
                    domain_id: None,
                    domain_name: None,
                    observed_at: self.unenrolled_observed.map(|time| seconds(time.wall)),
                    fresh: self.unenrolled_fresh(),
                    server_profile: server.into(),
                    context_name: None,
                    operation_permissions: None,
                },
            );
        }
        observations
            .into_values()
            .filter(|observation| !self.superseded.contains(&observation.device.id))
            .collect()
    }

    pub fn domains(&self, server: &str, contexts: &BTreeMap<String, Context>) -> Vec<Value> {
        let mut domains: Vec<_> = self.domains.values().collect();
        domains.sort_by_key(|domain| domain.name.as_deref().unwrap_or(&domain.id));
        domains
            .into_iter()
            .map(|domain| {
                let mut record = json!(domain);
                record["ddm_server_profile"] = json!(server);
                record["ddm_context"] = json!(
                    contexts
                        .iter()
                        .find(|(_, context)| context.server == server
                            && context.domain_id == domain.id)
                        .map(|(name, _)| name)
                );
                record["ddm_fresh"] = json!(self.domains_fresh());
                record["ddm_last_sync"] =
                    json!(self.domains_observed.map(|time| seconds(time.wall)));
                record
            })
            .collect()
    }

    pub fn status(
        &self,
        name: &str,
        server: &Server,
        contexts: &BTreeMap<String, Context>,
    ) -> Value {
        let observations = self.observations(name, contexts);
        let state = if !server.enabled() {
            "disabled"
        } else if self.last_attempt.is_none() {
            "starting"
        } else if self.last_error.is_some() || self.last_success.is_none() {
            "degraded"
        } else if !self.fresh() {
            "stale"
        } else {
            "ready"
        };
        json!({
            "enabled": server.enabled(), "state": state, "url": server.url, "server_profile": name,
            "refresh_interval": null, "refresh_mode": "explicit", "freshness_seconds": freshness(server).as_secs_f64(),
            "fresh": self.fresh(), "authoritative": self.fresh(), "domains_fresh": self.domains_fresh(), "unenrolled_fresh": self.unenrolled_fresh(),
            "last_attempt": self.last_attempt.map(seconds), "last_success": self.last_success.map(seconds),
            "observed_at": self.last_success.map(|time| time.to_rfc3339()), "last_error": self.last_error,
            "graphql_errors": self.errors, "domain_count": self.domains.len(), "device_count": observations.len(),
            "signal_membership_revision": self.signal_membership.revision,
            "signal_membership_observed_at": self.signal_membership.observed_at.map(|time| time.to_rfc3339()),
            "enrolled_device_count": observations.iter().filter(|item| item.domain_id.is_some()).count(),
            "unenrolled_device_count": observations.iter().filter(|item| item.domain_id.is_none()).count(),
        })
    }

    fn update_superseded(&mut self) {
        let retained: BTreeSet<_> = self
            .domains
            .values()
            .flat_map(|domain| domain.devices.iter().flatten().flatten())
            .filter(|device| {
                device
                    .connection
                    .as_ref()
                    .and_then(|connection| connection.state.as_deref())
                    == Some("DISCONNECTED")
            })
            .map(|device| device.id.clone())
            .collect();
        self.superseded
            .retain(|identity| retained.contains(identity));
        let observations = self.observations("", &BTreeMap::new());
        let mut groups = BTreeMap::<String, Vec<&Observation>>::new();
        for observation in &observations {
            if let Some(address) = primary_mac(&observation.device) {
                groups.entry(address).or_default().push(observation);
            }
        }
        for group in groups.values().filter(|group| group.len() == 2) {
            let current: Vec<_> = group
                .iter()
                .filter(|item| {
                    item.fresh
                        && item.domain_id.is_none()
                        && item.device.enrolment_state.as_deref() == Some("UNENROLLED")
                        && item
                            .device
                            .connection
                            .as_ref()
                            .and_then(|connection| connection.state.as_deref())
                            == Some("READY")
                })
                .collect();
            if current.len() != 1 {
                continue;
            }
            if let Some(previous) = group.iter().find(|item| {
                item.domain_id.is_some()
                    && item
                        .device
                        .connection
                        .as_ref()
                        .and_then(|connection| connection.state.as_deref())
                        == Some("DISCONNECTED")
            }) {
                self.superseded.insert(previous.device.id.clone());
            }
        }
    }
}

pub fn freshness(server: &Server) -> Duration {
    Duration::from_secs_f64((server.refresh_interval * 3.0).max(30.0))
}

fn seconds(time: DateTime<Utc>) -> f64 {
    time.timestamp_micros() as f64 / 1_000_000.0
}

fn domain_map(domains: Vec<Option<Domain>>) -> Result<BTreeMap<String, Domain>, Error> {
    let mut result = BTreeMap::new();
    let mut devices = BTreeSet::new();
    for domain in domains.into_iter().flatten() {
        for device in domain.devices.iter().flatten().flatten() {
            if !devices.insert(device.id.clone()) {
                return Err(Error::Response(
                    "Managed inventory repeats a device identity in domains",
                ));
            }
        }
        if result.insert(domain.id.clone(), domain).is_some() {
            return Err(Error::Response(
                "Managed inventory repeats a domain identity",
            ));
        }
    }
    Ok(result)
}

fn device_map(devices: Vec<Device>) -> Result<BTreeMap<String, Device>, Error> {
    let mut result = BTreeMap::new();
    for device in devices {
        if result.insert(device.id.clone(), device).is_some() {
            return Err(Error::Response(
                "Managed inventory repeats an unenrolled device identity",
            ));
        }
    }
    Ok(result)
}
