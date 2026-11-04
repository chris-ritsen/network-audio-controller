use chrono::{DateTime, Utc};
use netaudio_core::dapi;
use netaudio_core::device_lookup::{EntryChange, LookupKind, RECOGNIZED_SERVICES};
use netaudio_core::managed_api::inventory::{Discovery, DiscoveryState as Presence};
use netaudio_core::managed_session::DeviceLookupEvent;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default)]
pub struct DiscoveryState {
    domains: BTreeMap<String, Domain>,
}

#[derive(Clone, Default)]
struct Domain {
    connection: Connection,
    entries: BTreeMap<(u32, String), Entry>,
    generation: u64,
    lookups: BTreeMap<u32, Registration>,
    rejected: u64,
    requested: BTreeSet<String>,
}

#[derive(Clone, Copy, Default, PartialEq)]
enum Connection {
    Connecting,
    #[default]
    Inactive,
    Lost,
    Observing,
}

#[derive(Clone, Copy, PartialEq)]
enum Registration {
    Pending,
    Refused(u16),
    Registered,
    Terminated,
}

#[derive(Clone)]
struct Entry {
    generation: u64,
    identity: Option<String>,
    observed_at: DateTime<Utc>,
    services: BTreeMap<String, bool>,
    withdrawn: bool,
}

pub struct Applied {
    pub changed: bool,
    pub terminal: bool,
    pub unknown: bool,
}

impl Domain {
    fn current(&self, key: &(u32, String), entry: &Entry) -> bool {
        self.connection == Connection::Observing
            && entry.generation == self.generation
            && self.lookups.get(&key.0) != Some(&Registration::Terminated)
    }
}

impl DiscoveryState {
    pub fn connecting(&mut self, domain: &str) -> u64 {
        let state = self.domains.entry(domain.to_owned()).or_default();
        state.generation += 1;
        state.connection = Connection::Connecting;
        state.lookups.clear();
        state.requested.clear();
        state.generation
    }

    pub fn observing(&mut self, domain: &str, generation: u64) -> bool {
        let Some(state) = self
            .domains
            .get_mut(domain)
            .filter(|state| state.generation == generation)
        else {
            return false;
        };
        state.connection = Connection::Observing;
        state.lookups = dapi::domain_lookups()
            .map(|(_, lookup)| (lookup, Registration::Pending))
            .collect();
        true
    }

    pub fn lost(&mut self, domain: &str, generation: u64) -> bool {
        match self
            .domains
            .get_mut(domain)
            .filter(|state| state.generation == generation)
        {
            Some(state) => {
                state.connection = Connection::Lost;
                true
            }
            None => false,
        }
    }

    pub fn reject(&mut self, domain: &str, generation: u64) {
        if let Some(state) = self
            .domains
            .get_mut(domain)
            .filter(|state| state.generation == generation)
        {
            state.rejected += 1;
        }
    }

    pub fn retain(&mut self, domains: &BTreeSet<String>) -> bool {
        let previous = self.domains.len();
        self.domains.retain(|domain, _| domains.contains(domain));
        previous != self.domains.len()
    }

    pub fn domains(&self) -> impl Iterator<Item = &String> {
        self.domains.keys()
    }

    pub fn apply(
        &mut self,
        domain: &str,
        generation: u64,
        event: &DeviceLookupEvent,
        received_at: DateTime<Utc>,
        known: &BTreeSet<String>,
    ) -> Applied {
        let mut applied = Applied {
            changed: false,
            terminal: false,
            unknown: false,
        };
        let Some(state) = self.domains.get_mut(domain).filter(|state| {
            state.generation == generation && state.connection == Connection::Observing
        }) else {
            return applied;
        };
        let message = &event.message;
        if message.kind == LookupKind::Registration && message.response {
            let registration = if message.status == 0 {
                Registration::Registered
            } else {
                Registration::Refused(message.status)
            };
            if !matches!(
                state.lookups.get(&event.lookup_id),
                Some(Registration::Terminated)
            ) && state.lookups.insert(event.lookup_id, registration) != Some(registration)
            {
                applied.changed = true;
            }
        }
        let Some(update) = &message.update else {
            return applied;
        };
        if let Some(observation) = &update.entry {
            let key = (event.lookup_id, observation.name.clone());
            let previous = state.entries.get(&key).cloned();
            let mut entry = state
                .entries
                .get(&key)
                .filter(|entry| entry.generation == generation)
                .cloned()
                .unwrap_or(Entry {
                    generation,
                    identity: None,
                    observed_at: received_at,
                    services: BTreeMap::new(),
                    withdrawn: false,
                });
            match observation.change() {
                EntryChange::Withdrawal => {
                    entry.withdrawn = true;
                    entry.services.clear();
                }
                EntryChange::Observation => {
                    entry.withdrawn = false;
                    for service in observation.services.iter().flatten() {
                        let Some(name) = service
                            .name
                            .as_deref()
                            .filter(|name| RECOGNIZED_SERVICES.contains(name))
                        else {
                            continue;
                        };
                        entry.services.insert(name.to_owned(), service.advertised());
                        if let Some(identity) = service
                            .monitoring_identity()
                            .and_then(|identity| identity.cmc_source_identifier)
                        {
                            entry.identity = Some(identity);
                        }
                    }
                }
                EntryChange::Unsupported => {
                    state.rejected += 1;
                    return applied;
                }
            }
            let modified = previous.as_ref().is_none_or(|previous| {
                previous.generation != entry.generation
                    || previous.identity != entry.identity
                    || previous.services != entry.services
                    || previous.withdrawn != entry.withdrawn
            });
            if modified {
                entry.observed_at = received_at;
                applied.unknown = entry.identity.as_ref().is_some_and(|identity| {
                    !entry.withdrawn
                        && entry.services.values().any(|advertised| *advertised)
                        && !known.contains(identity)
                        && !state.requested.contains(identity)
                });
                let identities: BTreeSet<_> = [
                    previous.and_then(|previous| previous.identity),
                    entry.identity.clone(),
                ]
                .into_iter()
                .flatten()
                .collect();
                let before: Vec<_> = identities
                    .iter()
                    .map(|identity| self.presence(domain, identity))
                    .collect();
                self.domains
                    .get_mut(domain)
                    .expect("domain state was selected above")
                    .entries
                    .insert(key, entry);
                applied.changed |= identities
                    .iter()
                    .zip(before)
                    .any(|(identity, before)| self.presence(domain, identity) != before);
            }
        }
        if update.terminal {
            self.terminate(domain, event.lookup_id, &mut applied);
        }
        applied
    }

    fn terminate(&mut self, domain: &str, lookup: u32, applied: &mut Applied) {
        if let Some(state) = self.domains.get_mut(domain) {
            state.lookups.insert(lookup, Registration::Terminated);
            applied.changed = true;
            applied.terminal = true;
        }
    }

    pub fn request_unknown(&mut self, domain: &str, known: &BTreeSet<String>) -> bool {
        let Some(state) = self.domains.get_mut(domain) else {
            return false;
        };
        let unknown: Vec<_> = state
            .entries
            .iter()
            .filter(|(key, entry)| {
                state.current(key, entry)
                    && !entry.withdrawn
                    && entry.services.values().any(|advertised| *advertised)
            })
            .filter_map(|(_, entry)| entry.identity.clone())
            .filter(|identity| !known.contains(identity) && !state.requested.contains(identity))
            .collect();
        let requested = !unknown.is_empty();
        state.requested.extend(unknown);
        requested
    }

    pub fn presence(&self, domain: &str, identity: &str) -> Option<Discovery> {
        let state = self.domains.get(domain)?;
        let entries: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.identity.as_deref() == Some(identity))
            .collect();
        let observed_at = entries.iter().map(|(_, entry)| entry.observed_at).max()?;
        let current: Vec<_> = entries
            .iter()
            .filter(|(key, entry)| state.current(key, entry))
            .map(|(_, entry)| *entry)
            .collect();
        let mut services = BTreeMap::new();
        for entry in &current {
            for (name, advertised) in &entry.services {
                *services.entry(name.clone()).or_insert(false) |= *advertised;
            }
        }
        let presence = if current.is_empty() {
            if state.connection == Connection::Observing {
                Presence::Historical
            } else {
                Presence::Unavailable
            }
        } else if services.values().any(|advertised| *advertised) {
            Presence::Advertised
        } else if current.iter().all(|entry| entry.withdrawn) {
            Presence::Withdrawn
        } else {
            Presence::ServicesWithdrawn
        };
        let observed_at = current
            .iter()
            .map(|entry| entry.observed_at)
            .max()
            .unwrap_or(observed_at);
        Some(Discovery {
            observed_at: observed_at.timestamp_micros() as f64 / 1_000_000.0,
            services,
            state: presence,
        })
    }

    pub fn status(&self) -> Value {
        json!(self
            .domains
            .iter()
            .map(|(domain, state)| {
                let advertised: BTreeSet<_> = state
                    .entries
                    .iter()
                    .filter(|(key, entry)| {
                        state.current(key, entry)
                            && entry.services.values().any(|advertised| *advertised)
                    })
                    .filter_map(|(_, entry)| entry.identity.as_deref())
                    .collect();
                let lookups: BTreeMap<_, _> = state
                    .lookups
                    .iter()
                    .map(|(lookup, registration)| {
                        let value = match registration {
                            Registration::Pending => json!("pending"),
                            Registration::Registered => json!("registered"),
                            Registration::Refused(status) => {
                                json!({"state": "refused", "status": status})
                            }
                            Registration::Terminated => json!("terminated"),
                        };
                        (format!("{lookup:08x}"), value)
                    })
                    .collect();
                (
                    domain.clone(),
                    json!({
                        "advertised_devices": advertised.len(),
                        "generation": state.generation,
                        "lookups": lookups,
                        "rejected_updates": state.rejected,
                        "state": match state.connection {
                            Connection::Connecting => "connecting",
                            Connection::Inactive => "inactive",
                            Connection::Lost => "unavailable",
                            Connection::Observing => "observing",
                        },
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>())
    }
}
