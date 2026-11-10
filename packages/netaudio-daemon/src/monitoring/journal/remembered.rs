use super::store::JournalDatabase;
use anyhow::{ensure, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SAVE_COALESCING: Duration = Duration::from_secs(60);

#[derive(Clone, Deserialize, Serialize)]
pub struct Device {
    pub seen_at: Option<String>,
    pub record: Arc<Value>,
}

pub type Devices = BTreeMap<String, Device>;

pub struct Inventory {
    devices: Devices,
    dirty: BTreeSet<String>,
    saved: Instant,
}

impl Inventory {
    pub fn load(database: &JournalDatabase) -> Result<Self> {
        let devices: Devices = serde_json::from_value(database.remembered()?)?;
        for (identity, device) in &devices {
            ensure!(!identity.is_empty(), "remembered device identity is empty");
            ensure!(
                device.record.is_object(),
                "remembered device must be an object"
            );
            if let Some(timestamp) = &device.seen_at {
                DateTime::parse_from_rfc3339(timestamp)?;
            }
        }
        Ok(Self {
            devices,
            dirty: BTreeSet::new(),
            saved: Instant::now(),
        })
    }

    pub fn snapshot(&self) -> Devices {
        self.devices.clone()
    }

    pub fn forget(&mut self, database: &mut JournalDatabase, identity: &str) -> Result<()> {
        database.forget(identity)?;
        self.devices.remove(identity);
        self.dirty.remove(identity);
        Ok(())
    }

    pub fn observe(&mut self, identity: &str, record: Arc<Value>, timestamp: &str) {
        let seen_at = if record["online"] == true {
            if record["last_seen"].is_null() {
                Some(timestamp.to_owned())
            } else {
                source_time(&record["last_seen"])
            }
        } else {
            self.devices
                .get(identity)
                .and_then(|device| device.seen_at.clone())
                .or_else(|| source_time(&record["last_seen"]))
        };
        let seen_at = seen_at.or_else(|| {
            self.devices
                .get(identity)
                .and_then(|device| device.seen_at.clone())
        });
        self.devices
            .insert(identity.to_owned(), Device { seen_at, record });
        self.dirty.insert(identity.to_owned());
    }

    pub fn save(&mut self, database: &mut JournalDatabase, identity: &str) -> Result<()> {
        if let Some(device) = self.devices.get(identity) {
            database.remember([(identity, device.record.as_ref(), device.seen_at.as_deref())])?;
            self.dirty.remove(identity);
        }
        Ok(())
    }

    pub fn flush(&mut self, database: &mut JournalDatabase) -> Result<()> {
        database.remember(self.dirty.iter().filter_map(|identity| {
            self.devices.get(identity).map(|device| {
                (
                    identity.as_str(),
                    device.record.as_ref(),
                    device.seen_at.as_deref(),
                )
            })
        }))?;
        self.dirty.clear();
        self.saved = Instant::now();
        Ok(())
    }

    pub fn flush_due(&mut self, database: &mut JournalDatabase) -> Result<()> {
        if !self.dirty.is_empty() && self.saved.elapsed() >= SAVE_COALESCING {
            self.flush(database)?;
        }
        Ok(())
    }
}

fn source_time(value: &Value) -> Option<String> {
    const MICROSECONDS_PER_SECOND: f64 = 1_000_000.0;
    match value {
        Value::String(timestamp) => DateTime::parse_from_rfc3339(timestamp)
            .ok()
            .map(|_| timestamp.clone()),
        Value::Number(number) => number
            .as_f64()
            .map(|seconds| (seconds * MICROSECONDS_PER_SECOND).round())
            .filter(|value| {
                value.is_finite() && *value >= i64::MIN as f64 && *value < i64::MAX as f64
            })
            .and_then(|value| DateTime::<Utc>::from_timestamp_micros(value as i64))
            .map(|time| time.to_rfc3339_opts(SecondsFormat::Micros, true)),
        _ => None,
    }
}
