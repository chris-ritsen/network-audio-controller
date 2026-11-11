use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::time::{delay_queue::Key, DelayQueue};

pub const LIFETIME: Duration = Duration::from_secs(15);
const LONGEST_LIFETIME: Duration = Duration::from_secs(300);
const SETTLING: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lease {
    lifetime: Duration,
    widened: Option<Instant>,
}

impl Default for Lease {
    fn default() -> Self {
        Self {
            lifetime: LIFETIME,
            widened: None,
        }
    }
}

impl Lease {
    pub fn lifetime(&mut self, now: Instant) -> Duration {
        if self
            .widened
            .is_some_and(|widened| now >= widened + SETTLING)
        {
            *self = Self::default();
        }

        self.lifetime
    }

    pub fn returned(&mut self, absence: Duration, now: Instant) -> bool {
        let lifetime = self.lifetime(now);

        if absence <= lifetime {
            if self.widened.is_some() {
                self.widened = Some(now);
            }

            return false;
        }

        if absence > LONGEST_LIFETIME / 2 {
            return false;
        }

        self.lifetime = absence * 2;
        self.widened = Some(now);
        true
    }

    fn settled(&mut self, now: Instant) -> bool {
        self.lifetime(now) == LIFETIME && self.widened.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct Presence {
    advertised: bool,
    changed: DateTime<Utc>,
    changes: u64,
    evidence: Option<Instant>,
    listening: bool,
    online: bool,
    silence: Lease,
}

impl Default for Presence {
    fn default() -> Self {
        Self {
            advertised: false,
            changed: Utc::now(),
            changes: 0,
            evidence: None,
            listening: false,
            online: false,
            silence: Lease::default(),
        }
    }
}

impl Presence {
    pub fn online(&self) -> bool {
        self.online
    }

    pub fn advertised(&mut self, advertised: bool, news: bool, now: Instant) -> bool {
        self.advertised = advertised;

        if !advertised {
            return self.settle(false);
        }

        if !news {
            return false;
        }

        self.evidence = Some(now);
        self.settle(true)
    }

    pub fn heard(&mut self, now: Instant, stream: bool) -> bool {
        if !self.online
            && self.listening
            && let Some(evidence) = self.evidence
        {
            self.silence
                .returned(now.saturating_duration_since(evidence), now);
        }

        self.evidence = Some(self.evidence.map_or(now, |evidence| evidence.max(now)));
        self.listening |= stream;
        self.advertised && self.settle(true)
    }

    pub fn deadline(&mut self, now: Instant) -> Option<Instant> {
        if !self.online || !self.listening {
            return None;
        }

        let lifetime = self.silence.lifetime(now);
        self.evidence.map(|evidence| evidence + lifetime)
    }

    pub fn expire(&mut self, now: Instant) -> bool {
        self.deadline(now).is_some_and(|deadline| now >= deadline) && self.settle(false)
    }

    pub fn withdraw(&mut self) -> bool {
        self.advertised = false;
        self.listening = false;
        self.settle(false)
    }

    pub fn presentation(&self) -> Value {
        json!({
            "changes": self.changes,
            "since": self.changed.to_rfc3339_opts(SecondsFormat::Millis, true),
            "state": if self.online { "online" } else { "offline" },
        })
    }

    fn settle(&mut self, online: bool) -> bool {
        if self.online == online {
            return false;
        }

        self.online = online;
        self.changed = Utc::now();
        self.changes += 1;
        true
    }
}

pub enum Returned {
    Held,
    Rediscovered,
    Widened(Duration),
}

#[derive(Default)]
pub struct Lapses {
    deadlines: DelayQueue<String>,
    departed: BTreeMap<String, Instant>,
    leases: BTreeMap<String, Lease>,
    pending: BTreeMap<String, (Key, Instant)>,
}

impl Lapses {
    pub fn is_empty(&self) -> bool {
        self.deadlines.is_empty()
    }

    pub fn lapsed(&mut self, name: String, now: Instant) {
        if self.pending.contains_key(&name) {
            return;
        }

        let lifetime = self.leases.entry(name.clone()).or_default().lifetime(now);
        let key = self.deadlines.insert_at(name.clone(), now + lifetime);
        self.pending.insert(name, (key, now));
        self.forget(now);
    }

    pub fn released(&mut self, name: &str) -> bool {
        let Some((key, _)) = self.pending.remove(name) else {
            return false;
        };
        self.deadlines.remove(&key);
        true
    }

    pub fn returned(&mut self, name: &str, now: Instant) -> Returned {
        if let Some((key, lapsed)) = self.pending.remove(name) {
            self.deadlines.remove(&key);
            self.leases
                .entry(name.to_owned())
                .or_default()
                .returned(now.saturating_duration_since(lapsed), now);
            return Returned::Held;
        }

        let Some(lapsed) = self.departed.remove(name) else {
            return Returned::Rediscovered;
        };
        let lease = self.leases.entry(name.to_owned()).or_default();

        if lease.returned(now.saturating_duration_since(lapsed), now) {
            Returned::Widened(lease.lifetime(now))
        } else {
            Returned::Rediscovered
        }
    }

    pub async fn withdrawn(&mut self) -> Option<String> {
        let expired = std::future::poll_fn(|context| self.deadlines.poll_expired(context)).await?;
        let name = expired.into_inner();

        if let Some((_, lapsed)) = self.pending.remove(&name) {
            self.departed.insert(name.clone(), lapsed);
        }

        Some(name)
    }

    fn forget(&mut self, now: Instant) {
        self.departed
            .retain(|_, lapsed| now.saturating_duration_since(*lapsed) <= LONGEST_LIFETIME);
        let pending = &self.pending;
        let departed = &self.departed;
        self.leases.retain(|name, lease| {
            pending.contains_key(name) || departed.contains_key(name) || !lease.settled(now)
        });
    }
}
