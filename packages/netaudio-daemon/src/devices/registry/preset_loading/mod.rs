use super::{Device, DeviceRegistry};
use crate::devices::operations::ControlOperation;
use crate::devices::record::DeviceRecord;
use crate::presets::Error;
use crate::presets::loading::{LoadRequest, Target};
use anyhow::{Context, Result, ensure};
use netaudio_core::presets::planning::Action;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedMutexGuard, oneshot};

mod apply;
mod managed;
mod observation;
mod planning;
mod verification;

struct Selected {
    target: Target,
    provider: Provider,
}

enum Provider {
    Direct { device: Arc<Device>, identity: u64 },
    Managed(crate::managed::presets::Device),
}

struct PreparedAction {
    presentation: Action,
    operations: Vec<ControlOperation>,
}

struct PreparedDevice {
    generation: Value,
    actions: Vec<PreparedAction>,
}

impl Selected {
    fn direct(&self) -> Result<&Arc<Device>> {
        match &self.provider {
            Provider::Direct { device, .. } => Ok(device),
            Provider::Managed(_) => anyhow::bail!("preset target requires managed transport"),
        }
    }

    fn managed(&self) -> Option<&crate::managed::presets::Device> {
        match &self.provider {
            Provider::Managed(device) => Some(device),
            Provider::Direct { .. } => None,
        }
    }

    fn presentation(&self) -> Result<Value> {
        match &self.provider {
            Provider::Direct { .. } => self.record()?.0.presentation(),
            Provider::Managed(device) => device.record(),
        }
    }

    fn checkpoint(&self) -> Result<Value> {
        if let Some(device) = self.managed() {
            let record = device.record()?;
            return Ok(json!(
                self.target
                    .actions
                    .iter()
                    .map(|request| {
                        crate::presets::operations::operations(request, true).map(|operations| {
                            operations
                                .iter()
                                .map(|operation| operation.precondition(&record))
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            ));
        }
        Ok(json!(self.record()?.1))
    }

    fn record(&self) -> Result<(DeviceRecord, u64)> {
        let Provider::Direct { device, identity } = &self.provider else {
            anyhow::bail!("preset target requires managed transport");
        };
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        ensure!(
            state.identity_generation == *identity && state.record.online && !state.record.retired,
            "{}: device identity or availability changed during the preset operation",
            self.target.name
        );
        Ok((state.record.clone(), state.generation))
    }
}

impl DeviceRegistry {
    pub fn preset_records(&self) -> Result<BTreeMap<String, Arc<Value>>> {
        self.model.records()
    }

    async fn select_preset_targets(&self, targets: Vec<Target>) -> Result<Vec<Selected>, Error> {
        let records = self.model.records()?;
        let managed = if targets.iter().any(|target| {
            records
                .get(&target.identifier)
                .is_some_and(|record| crate::managed::control::required(record))
        }) {
            Some(
                self.managed
                    .get()
                    .context("managed provider unavailable")?
                    .preset_session()
                    .await?,
            )
        } else {
            None
        };
        let mut selected = BTreeMap::new();
        for target in targets {
            let record = self
                .model
                .device(&target.identifier)?
                .context("preset target disappeared")?;
            if !crate::presets::matches_record(&target.configuration, &target.identifier, &record) {
                return Err(Error::Conflict(format!(
                    "{}: selected target identity changed.",
                    target.name
                )));
            }
            let key = record["server_name"]
                .as_str()
                .context("preset target has no identity")?
                .to_owned();
            let provider = if crate::managed::control::required(&record) {
                Provider::Managed(
                    managed
                        .as_ref()
                        .context("managed preset session unavailable")?
                        .device(&record)?,
                )
            } else {
                let device = self
                    .controlled_device(&target.identifier)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let identity = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
                    .identity_generation;
                Provider::Direct { device, identity }
            };
            if selected
                .insert(key, Selected { target, provider })
                .is_some()
            {
                return Err("A target device may be selected only once.".into());
            }
        }
        Ok(selected.into_values().collect())
    }

    pub async fn plan_preset(&self, request: LoadRequest) -> Result<Value, Error> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let selected =
            tokio::time::timeout_at(deadline, self.select_preset_targets(request.targets))
                .await
                .map_err(|_| {
                    Error::Conflict(
                "Preset target ownership exceeded the planning deadline; no changes were sent."
                    .into(),
            )
                })??;
        let work = async {
            let _permit = self
                .pending
                .try_acquire()
                .context("device operation queue is full")?;
            let mut serial = Vec::new();
            for target in &selected {
                if let Provider::Direct { device, .. } = &target.provider {
                    serial.push(device.refresh.lock().await);
                }
            }
            let prepared = self
                .prepare_preset(&selected, request.confirm_destructive)
                .await?;
            Ok::<_, anyhow::Error>(presentation(
                &request.name,
                &request.digest,
                &selected,
                &prepared,
            ))
        };
        tokio::time::timeout_at(deadline, work)
            .await
            .map_err(|_| {
                Error::Conflict(
                    "Fresh preset planning exceeded its 60-second deadline; no changes were sent."
                        .into(),
                )
            })?
            .map_err(|error| Error::Conflict(format!("No changes were sent: {error:#}")))
    }

    pub async fn load_preset(
        self: &Arc<Self>,
        request: LoadRequest,
        serial: OwnedMutexGuard<()>,
    ) -> Result<Value, Error> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let selected =
            tokio::time::timeout_at(deadline, self.select_preset_targets(request.targets))
                .await
                .map_err(|_| {
                    Error::Conflict(
                "Preset target ownership exceeded the planning deadline; no changes were sent."
                    .into(),
            )
                })??;
        let permit = Arc::clone(&self.pending)
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let registry = Arc::clone(self);
        let (sender, receiver) = oneshot::channel();
        self.tasks.spawn(async move {
            let _permit = permit;
            let _serial = serial;
            let prepare = async {
                let mut guards = Vec::new();
                for target in &selected {
                    if let Provider::Direct { device, .. } = &target.provider {
                        guards.push(device.refresh.lock().await);
                    }
                }
                let prepared = registry
                    .prepare_preset(&selected, request.confirm_destructive)
                    .await?;
                Ok::<_, anyhow::Error>((guards, prepared))
            };
            let result = match tokio::time::timeout_at(deadline, prepare).await {
                Ok(Ok((_guards, prepared))) => {
                    let current = presentation(&request.name, &request.digest, &selected, &prepared);
                    if request.confirmed_plan.as_ref().is_some_and(|expected| crate::presets::loading::confirmation(&current) != *expected) {
                        Err(Error::Conflict("No changes were sent: the fresh preset plan changed after confirmation; review a new preview.".into()))
                    } else { registry
                        .apply_preset(
                            &request.name,
                            &request.digest,
                            &selected,
                            prepared,
                            request.store_current_configuration,
                        )
                        .await }
                }
                Ok(Err(error)) => Err(Error::Conflict(format!("No changes were sent: {error:#}"))),
                Err(_) => Err(Error::Conflict(
                    "Fresh preset planning exceeded its 60-second deadline; no changes were sent."
                        .into(),
                )),
            };
            if sender.send(result).is_err() {
                tracing::info!("preset operation completed after its caller disconnected");
            }
        });
        receiver.await.map_err(|error| {
            Error::Internal(anyhow::anyhow!("preset operation worker stopped: {error}"))
        })?
    }

    async fn prepare_preset(
        &self,
        selected: &[Selected],
        confirm_destructive: bool,
    ) -> Result<Vec<PreparedDevice>> {
        for target in selected {
            target.presentation()?;
            for action in &target.target.actions {
                crate::presets::operations::operations(action, confirm_destructive)?;
            }
        }
        let mut prepared = Vec::new();
        for target in selected {
            prepared.push(
                self.plan_preset_device(target, confirm_destructive)
                    .await
                    .with_context(|| format!("{}: fresh preset planning", target.target.name))?,
            );
        }
        for (target, prepared) in selected.iter().zip(&prepared) {
            ensure!(
                target.checkpoint()? == prepared.generation,
                "{}: inventory changed while planning other preset devices; review the selection again",
                target.target.name
            );
        }
        Ok(prepared)
    }
}

fn presentation(
    name: &str,
    digest: &str,
    selected: &[Selected],
    prepared: &[PreparedDevice],
) -> Value {
    json!({
        "name": name, "digest": digest,
        "devices": selected.iter().zip(prepared).map(|(target, prepared)| json!({
            "device_name": target.target.name, "server_name": target.target.identifier,
            "actions": prepared.actions.iter().map(|action| &action.presentation).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}
