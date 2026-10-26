use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasher, RandomState};
use std::net::SocketAddrV4;

use anyhow::{Context, Result, bail};
use hickory_proto::rr::Name;
use netaudio_core::native_audio::clock::NANOSECONDS_PER_SECOND;

use super::records::{Data, Key, Kind, Record};

const RETIREMENT_GRACE_NANOSECONDS: u64 = NANOSECONDS_PER_SECOND;
const MAXIMUM_RECORDS_PER_KEY: usize = 32;
const INITIAL_QUERY_DELAY_SECONDS: [u64; 3] = [0, 1, 2];
const REFRESH_PARTS_PER_TEN_THOUSAND: [u64; 4] = [8000, 8500, 9000, 9500];
const MAXIMUM_REFRESH_JITTER: u64 = 200;
const PARTS_PER_WHOLE: u64 = 10_000;

#[derive(Clone, Debug)]
pub struct Evidence {
    pub record: Record,
    pub generation: u64,
    pub received_at: u64,
    pub expires_at: u64,
    refresh_at: [u64; REFRESH_PARTS_PER_TEN_THOUSAND.len()],
    next_refresh: usize,
    retiring: bool,
}

#[derive(Clone, Debug)]
pub struct ResolvedService {
    pub name: Name,
    pub target: Name,
    pub endpoint: SocketAddrV4,
    pub text: Vec<u8>,
    pub record_generations: [u64; 3],
    pub usable_through: u64,
}

struct Question {
    next_deadline: Option<u64>,
    next_attempt: usize,
    had_records: bool,
}

impl Question {
    fn deadline(&self) -> Option<u64> {
        if self.had_records {
            return None;
        }

        self.next_deadline
    }
}

pub struct Cache {
    services: BTreeSet<Name>,
    records: BTreeMap<Key, Vec<Evidence>>,
    questions: BTreeMap<Key, Question>,
    next_generation: u64,
    jitter: RandomState,
}

impl Cache {
    pub fn new(services: impl IntoIterator<Item = Name>, now: u64) -> Result<Self> {
        let services: BTreeSet<_> = services.into_iter().collect();

        if services.is_empty() || services.iter().any(Name::is_root) {
            bail!("discovery requires at least one exact service identity");
        }

        let mut cache = Self {
            services,
            records: BTreeMap::new(),
            questions: BTreeMap::new(),
            next_generation: 1,
            jitter: RandomState::new(),
        };
        cache.synchronize(now);
        Ok(cache)
    }

    fn interests(&self) -> BTreeSet<Key> {
        let mut keys = BTreeSet::new();

        for name in &self.services {
            for kind in [Kind::Service, Kind::Text] {
                keys.insert(Key {
                    name: name.clone(),
                    kind,
                });
            }
        }

        for (key, records) in &self.records {
            if key.kind == Kind::Service && self.services.contains(&key.name) {
                for evidence in records {
                    if let Data::Service {
                        target,
                        ..
                    } = &evidence.record.data
                        && !target.is_root()
                    {
                        keys.insert(Key {
                            name: target.clone(),
                            kind: Kind::Address,
                        });
                    }
                }
            }
        }

        keys
    }

    fn synchronize(&mut self, now: u64) {
        for records in self.records.values_mut() {
            records.retain(|record| record.expires_at > now);
        }

        self.records.retain(|_, records| !records.is_empty());
        let interests = self.interests();
        self.records.retain(|key, _| interests.contains(key));
        self.questions.retain(|key, _| interests.contains(key));

        for key in interests {
            let has_records = self.records.contains_key(&key);
            let question = self.questions.entry(key).or_insert(Question {
                next_deadline: Some(now),
                next_attempt: 0,
                had_records: false,
            });

            if question.had_records && !has_records {
                question.next_deadline = Some(now);
                question.next_attempt = 0;
            }

            question.had_records = has_records;
        }
    }

    pub fn expire(&mut self, now: u64) {
        self.synchronize(now);
    }

    pub fn evidence(&self, key: &Key) -> &[Evidence] {
        self.records.get(key).map(Vec::as_slice).unwrap_or_default()
    }

    fn insert_group(&mut self, key: Key, incoming: Vec<Record>, now: u64) -> Result<()> {
        let records = self.records.entry(key.clone()).or_default();
        let grace_deadline = now
            .checked_add(RETIREMENT_GRACE_NANOSECONDS)
            .context("discovery retirement deadline overflow")?;

        if incoming.iter().any(|record| record.cache_flush && record.lifetime_seconds != 0) {
            for evidence in records.iter_mut() {
                if now.saturating_sub(evidence.received_at) > RETIREMENT_GRACE_NANOSECONDS
                    && !incoming.iter().any(|record| record.data == evidence.record.data)
                {
                    evidence.expires_at = evidence.expires_at.min(grace_deadline);
                    evidence.retiring = true;
                }
            }
        }

        for record in incoming {
            let existing = records.iter_mut().find(|evidence| evidence.record.data == record.data);

            if record.lifetime_seconds == 0 {
                if let Some(evidence) = existing {
                    evidence.expires_at = evidence.expires_at.min(grace_deadline);
                    evidence.retiring = true;
                }

                continue;
            }

            let lifetime = u64::from(record.lifetime_seconds) * NANOSECONDS_PER_SECOND;
            let expires_at = now.checked_add(lifetime).context("discovery lifetime overflow")?;
            let refresh_at = REFRESH_PARTS_PER_TEN_THOUSAND.map(|fraction| {
                let random = self.jitter.hash_one((&key, now, fraction));
                let fraction = fraction + random % (MAXIMUM_REFRESH_JITTER + 1);
                now + (u128::from(lifetime) * u128::from(fraction) / u128::from(PARTS_PER_WHOLE))
                    as u64
            });
            let generation = match existing.as_ref() {
                Some(evidence) => evidence.generation,
                None => {
                    let generation = self.next_generation;
                    self.next_generation = generation
                        .checked_add(1)
                        .context("discovery generation space is exhausted")?;
                    generation
                }
            };
            let evidence = Evidence {
                record,
                generation,
                received_at: now,
                expires_at,
                refresh_at,
                next_refresh: 0,
                retiring: false,
            };

            match existing {
                Some(existing) => *existing = evidence,
                None => {
                    if records.len() >= MAXIMUM_RECORDS_PER_KEY {
                        bail!("selected discovery record set exceeds the bounded ambiguity budget");
                    }

                    records.push(evidence);
                }
            }
        }

        Ok(())
    }

    pub fn observe(&mut self, incoming: Vec<Record>, now: u64) -> Result<bool> {
        let mut changed = self.records.values().flatten().any(|record| record.expires_at <= now);
        self.synchronize(now);
        let mut groups = BTreeMap::<Key, Vec<Record>>::new();

        for record in incoming {
            groups.entry(record.key.clone()).or_default().push(record);
        }

        for kind in [Kind::Service, Kind::Text, Kind::Address] {
            let interests = self.interests();
            let keys: Vec<_> = groups
                .keys()
                .filter(|key| key.kind == kind && interests.contains(*key))
                .cloned()
                .collect();

            for key in keys {
                let records = groups.remove(&key).context("discovery update group disappeared")?;
                self.insert_group(key, records, now)?;
                changed = true;
            }
        }

        self.synchronize(now);
        Ok(changed)
    }

    pub fn questions_due(&self, now: u64) -> BTreeSet<Key> {
        let mut due: BTreeSet<_> = self
            .questions
            .iter()
            .filter(|(_, question)| question.deadline().is_some_and(|deadline| deadline <= now))
            .map(|(key, _)| key.clone())
            .collect();

        for (key, records) in &self.records {
            if records.iter().any(|evidence| {
                !evidence.retiring
                    && evidence
                        .refresh_at
                        .get(evidence.next_refresh)
                        .is_some_and(|deadline| *deadline <= now)
            }) {
                due.insert(key.clone());
            }
        }

        due
    }

    pub fn questions_sent(&mut self, keys: &BTreeSet<Key>, now: u64) {
        for key in keys {
            if let Some(question) = self.questions.get_mut(key)
                && question.deadline().is_some_and(|deadline| deadline <= now)
            {
                question.next_attempt += 1;
                question.next_deadline = INITIAL_QUERY_DELAY_SECONDS
                    .get(question.next_attempt)
                    .map(|seconds| now.saturating_add(seconds * NANOSECONDS_PER_SECOND));
            }

            if let Some(records) = self.records.get_mut(key) {
                for evidence in records {
                    while evidence
                        .refresh_at
                        .get(evidence.next_refresh)
                        .is_some_and(|deadline| *deadline <= now)
                    {
                        evidence.next_refresh += 1;
                    }
                }
            }
        }
    }

    pub fn next_deadline(&self) -> Option<u64> {
        self.questions
            .values()
            .filter_map(Question::deadline)
            .chain(self.records.values().flatten().flat_map(|evidence| {
                let refresh = if evidence.retiring {
                    None
                } else {
                    evidence.refresh_at.get(evidence.next_refresh).copied()
                };
                [Some(evidence.expires_at), refresh].into_iter().flatten()
            }))
            .min()
    }

    fn unique(&self, name: &Name, kind: Kind, now: u64) -> Result<&Evidence> {
        let key = Key {
            name: name.clone(),
            kind,
        };
        let records = self.evidence(&key);
        let mut current = records.iter().filter(|record| record.expires_at > now);
        let record = current.next().with_context(|| format!("waiting for {kind:?} at {name}"))?;

        if current.next().is_some() {
            bail!("discovery has conflicting {kind:?} records at {name}");
        }

        Ok(record)
    }

    pub fn resolve(&self, name: &Name, now: u64) -> Result<ResolvedService> {
        let service = self.unique(name, Kind::Service, now)?;
        let Data::Service {
            port,
            target,
            ..
        } = &service.record.data
        else {
            bail!("selected source has malformed service data");
        };

        if *port == 0 || target.is_root() {
            bail!("selected source has no service endpoint");
        }

        let text = self.unique(name, Kind::Text, now)?;
        let Data::Text(bytes) = &text.record.data else {
            bail!("selected source has no complete TXT record");
        };
        let address = self.unique(target, Kind::Address, now)?;
        let Data::Address(address_value) = address.record.data else {
            bail!("selected source has malformed address data");
        };

        if address_value.is_unspecified()
            || address_value.is_multicast()
            || address_value.is_broadcast()
        {
            bail!("selected source has no usable unicast address");
        }

        Ok(ResolvedService {
            name: name.clone(),
            target: target.clone(),
            endpoint: SocketAddrV4::new(address_value, *port),
            text: bytes.clone(),
            record_generations: [service.generation, text.generation, address.generation],
            usable_through: service.expires_at.min(text.expires_at).min(address.expires_at) - 1,
        })
    }
}
