use super::{Device, DeviceRegistry, DeviceState};
use crate::devices::cached::include_remembered;
use crate::devices::inventory;
use crate::devices::operations::ControlError;
use crate::devices::record::DeviceRecord;
use crate::devices::transport::DeviceTransport;
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, oneshot};

const REDISCOVERY_DEADLINE: Duration = Duration::from_secs(5);

impl DeviceRegistry {
    pub async fn cached_records(&self) -> Result<BTreeMap<String, Arc<Value>>> {
        let mut records = self.model.records()?;
        include_remembered(&mut records, self.journal.remembered().await?);
        Ok(records)
    }

    pub async fn forget(
        self: &Arc<Self>,
        records: Vec<Arc<Value>>,
    ) -> Result<Vec<Value>, ControlError> {
        let permit = Arc::clone(&self.pending)
            .try_acquire_owned()
            .map_err(|_| ControlError::Unavailable("device operation queue is full".to_owned()))?;
        let registry = Arc::clone(self);
        let (reply, result) = oneshot::channel();
        self.tasks.spawn(async move {
            let _permit = permit;
            let result = registry.forget_records(records).await;
            let _ = reply.send(result);
        });
        result
            .await
            .map_err(|_| ControlError::Unavailable("device cache worker stopped".to_owned()))?
    }

    async fn forget_records(
        self: &Arc<Self>,
        records: Vec<Arc<Value>>,
    ) -> Result<Vec<Value>, ControlError> {
        let _serial = tokio::select! {
            _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
            guard = self.forgetting.lock() => guard,
        };
        let targets = records
            .into_iter()
            .map(|record| {
                let key = record["server_name"]
                    .as_str()
                    .context("cached device has no server name")?
                    .to_owned();
                Ok((key, record))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let entries = {
            let devices = self
                .devices
                .read()
                .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
            targets
                .keys()
                .filter_map(|key| {
                    devices
                        .get(key)
                        .map(|device| (key.clone(), Arc::clone(device)))
                })
                .collect::<Vec<_>>()
        };
        let mut guards = Vec::new();

        for (_, device) in &entries {
            guards.push(tokio::select! {
                _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
                guard = device.refresh.lock() => guard,
            });
        }

        let cached = self.cached_records().await?;
        let (forgotten, rediscovery, removed) = {
            let mut devices = self
                .devices
                .write()
                .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
            let mut states = entries
                .iter()
                .map(|(_, device)| {
                    device
                        .state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("device lock poisoned"))
                })
                .collect::<Result<Vec<_>>>()?;

            for (key, expected) in &targets {
                let current = if let Some(device) = devices.get(key) {
                    let Some(index) = entries
                        .iter()
                        .position(|(name, entry)| name == key && Arc::ptr_eq(entry, device))
                    else {
                        return Err(ControlError::Conflict("device was rediscovered before cache removal; obtain a fresh selection".to_owned()));
                    };
                    Arc::new(states[index].record.presentation()?)
                } else {
                    crate::state::resolve_record(&cached, key, None)?
                        .cloned()
                        .ok_or_else(|| {
                            ControlError::Conflict(
                                "cached device changed before removal".to_owned(),
                            )
                        })?
                };

                if cache_precondition(&current) != cache_precondition(expected) {
                    return Err(ControlError::Conflict(
                        "device changed before cache removal; obtain a fresh selection".to_owned(),
                    ));
                }
            }

            let mut rediscovery = Vec::new();

            for state in &mut states {
                if state.record.online && state.record.control_address().is_some() {
                    rediscovery.push(state.record.clone());
                }

                state.generation += 1;
                state.identity_generation += 1;
                state.record.retired = true;
                state.record.online = false;
                state.record.services.clear();
            }

            let mut removed = Vec::new();

            for key in targets.keys() {
                devices.remove(key);
                self.refresh_queue.remove(key)?;
                self.metering.invalidate(key)?;
                removed.push((key.clone(), self.model.remove_device(key)?));
            }

            let forgotten = targets
                .values()
                .map(|record| forgotten_record(record))
                .collect();
            (forgotten, rediscovery, removed)
        };

        for (key, sequence) in removed {
            self.journal
                .forget(key, sequence)
                .await
                .context("persist cache removal; inventory was already removed")?;
        }

        drop(guards);

        for record in rediscovery {
            let registry = Arc::clone(self);
            self.tasks.spawn(async move {
                let key = record.key.clone();
                if let Err(error) = registry.rediscover_forgotten(record).await {
                    tracing::info!(device = key, %error, "forgotten device did not complete rediscovery");
                }
            });
        }

        Ok(forgotten)
    }

    async fn rediscover_forgotten(&self, previous: DeviceRecord) -> Result<()> {
        let address = previous
            .control_address()
            .context("device has no routing service")?;
        let name = tokio::select! {
            _ = self.cancellation.cancelled() => return Ok(()),
            result = tokio::time::timeout(REDISCOVERY_DEADLINE, async {
                let mut transport = DeviceTransport::connect(address, self.local_address).await?.with_observer(self.wire_observer());
                inventory::device_name(&mut transport).await
            }) => result.context("forgotten device probe timed out")??,
        };
        let mut services = previous.services.into_values();
        let first = services.next().context("device has no discovery records")?;
        let mut record = DeviceRecord::discovered(&first)?;

        for service in services {
            record.attach(service)?;
        }

        record.name = name;
        let key = record.key.clone();
        let device = Arc::new(Device {
            state: Mutex::new(DeviceState {
                generation: 1,
                identity_generation: 1,
                field_observed_at: BTreeMap::new(),
                record,
                diagnostics: Default::default(),
            }),
            transport: AsyncMutex::new(None),
            refresh: AsyncMutex::new(()),
            inventory_changes: tokio::sync::watch::channel(()).0,
        });
        {
            let mut devices = self
                .devices
                .write()
                .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?;
            if devices.contains_key(&key) || self.cancellation.is_cancelled() {
                return Ok(());
            }

            self.publish(
                &device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
                    .record,
            )?;
            devices.insert(key.clone(), Arc::clone(&device));
        }

        self.schedule_refresh(key, device, 1)
    }
}

pub fn cache_precondition(record: &Value) -> Value {
    json!({
        "server_name": record["server_name"],
        "name": record["name"],
        "mac_address": record["mac_address"],
        "online": record["online"],
        "kind": record["kind"],
        "ddm_device_id": record["ddm_device_id"],
    })
}

fn forgotten_record(record: &Value) -> Value {
    json!({
        "ipv4": record["ipv4"],
        "kind": record["kind"],
        "name": record["name"],
        "online": record["online"],
        "server_name": record["server_name"],
    })
}
