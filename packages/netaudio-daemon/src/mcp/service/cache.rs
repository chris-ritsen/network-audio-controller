use crate::mcp::views::Records;
use crate::monitoring::journal::store::Overview;
use anyhow::{Result, anyhow};
use chrono::{Local, NaiveDate};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

#[derive(Default)]
pub(super) struct ViewCache {
    current: Mutex<Option<CachedView>>,
}

struct CachedView {
    records: BTreeMap<String, Weak<Value>>,
    managed: Value,
    journal: Option<Weak<Overview>>,
    date: NaiveDate,
    offset_seconds: i32,
    value: Value,
}

impl ViewCache {
    pub(super) fn read(
        &self,
        records: &Records,
        managed: &Value,
        journal: Option<&Arc<Overview>>,
        build: impl FnOnce() -> Value,
    ) -> Result<Value> {
        let now = Local::now();
        let mut current = self
            .current
            .lock()
            .map_err(|_| anyhow!("MCP view cache lock poisoned"))?;

        if let Some(cached) = &*current
            && cached.date == now.date_naive()
            && cached.offset_seconds == now.offset().local_minus_utc()
            && cached.managed == *managed
            && match (&cached.journal, journal) {
                (None, None) => true,
                (Some(previous), Some(journal)) => previous.as_ptr() == Arc::as_ptr(journal),
                _ => false,
            }
            && cached.records.len() == records.len()
            && cached.records.iter().zip(records).all(
                |((previous_key, previous), (key, record))| {
                    previous_key == key && previous.as_ptr() == Arc::as_ptr(record)
                },
            )
        {
            return Ok(cached.value.clone());
        }

        let value = build();
        *current = Some(CachedView {
            records: records
                .iter()
                .map(|(key, record)| (key.clone(), Arc::downgrade(record)))
                .collect(),
            managed: managed.clone(),
            journal: journal.map(Arc::downgrade),
            date: now.date_naive(),
            offset_seconds: now.offset().local_minus_utc(),
            value: value.clone(),
        });
        Ok(value)
    }
}
