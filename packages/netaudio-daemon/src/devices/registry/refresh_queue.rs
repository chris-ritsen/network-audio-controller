use super::Device;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

pub(super) struct ScheduledRefresh {
    pub key: String,
    pub device: Arc<Device>,
    pub generation: u64,
}

#[derive(Default)]
struct Pending {
    order: VecDeque<String>,
    devices: BTreeMap<String, ScheduledRefresh>,
}

#[derive(Default)]
pub(super) struct RefreshQueue {
    pending: Mutex<Pending>,
}

impl RefreshQueue {
    pub fn remove(&self, key: &str) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("inventory refresh queue lock poisoned"))?;
        pending.devices.remove(key);
        pending.order.retain(|name| name != key);
        Ok(())
    }

    pub fn schedule(&self, refresh: ScheduledRefresh) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("inventory refresh queue lock poisoned"))?;

        if !pending.devices.contains_key(&refresh.key) {
            pending.order.push_back(refresh.key.clone());
        }

        if pending
            .devices
            .get(&refresh.key)
            .is_none_or(|queued| queued.generation <= refresh.generation)
        {
            pending.devices.insert(refresh.key.clone(), refresh);
        }

        Ok(())
    }

    pub fn next(&self, active: &BTreeSet<String>) -> Result<Option<ScheduledRefresh>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("inventory refresh queue lock poisoned"))?;
        let Some(index) = pending.order.iter().position(|key| !active.contains(key)) else {
            return Ok(None);
        };
        let key = pending.order.remove(index).expect("queued device exists");
        Ok(pending.devices.remove(&key))
    }
}
