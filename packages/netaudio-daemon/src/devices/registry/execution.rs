use super::read_inventory;
use super::{Device, DeviceRegistry};
use crate::devices::operations::{
    ControlCompletion, ControlError, ControlOperation, ControlReadback,
};
use crate::devices::transport::{CommandIntent, DeviceTransport, TransportError};
use crate::monitoring::requests::StatusQuery;
use anyhow::{bail, Context, Result};
use netaudio_core::responses::DeviceSettings;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Instant;

impl DeviceRegistry {
    pub async fn apply(
        self: &Arc<Self>,
        reference: &str,
        operation: ControlOperation,
    ) -> Result<ControlCompletion, ControlError> {
        self.apply_conditionally(reference, operation, None).await
    }

    pub fn bind_precondition(
        &self,
        reference: &str,
        operation: &ControlOperation,
        condition: &mut Value,
    ) -> Result<(), ControlError> {
        let Some(record) = self.model.device(reference)? else {
            return Ok(());
        };
        if crate::managed::control::required(&record) {
            let managed = self.managed.get().ok_or_else(|| {
                ControlError::Unavailable("Managed control is unavailable".into())
            })?;
            condition["managed_configuration"] = json!(managed.configuration_revision()?);
        } else if *condition == operation.precondition(&record) {
            let device = self.controlled_device(reference)?;
            let presented = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
                .record
                .presentation()?;
            *condition = operation.precondition(&presented);
        }
        Ok(())
    }

    pub(super) fn controlled_device(&self, reference: &str) -> Result<Arc<Device>, ControlError> {
        let record = self
            .model
            .device(reference)?
            .ok_or_else(|| ControlError::NotFound("device not found".to_owned()))?;
        if crate::managed::control::required(&record) {
            return Err(ControlError::Conflict(
                "Enrolled devices require managed control".into(),
            ));
        }
        let key = record
            .get("server_name")
            .and_then(Value::as_str)
            .context("device has no server name")?;
        let device = self
            .devices
            .read()
            .map_err(|_| anyhow::anyhow!("device registry lock poisoned"))?
            .get(key)
            .cloned()
            .context("device has no native control transport")?;
        Ok(device)
    }

    pub fn validate_operation(
        &self,
        reference: &str,
        operation: &ControlOperation,
    ) -> Result<(), ControlError> {
        if let Some(record) = self.model.device(reference)?
            && crate::managed::control::required(&record)
        {
            return crate::managed::control::validate(&record, operation);
        }
        self.planned_batch_count(reference, operation).map(|_| ())
    }

    pub fn planned_batch_count(
        &self,
        reference: &str,
        operation: &ControlOperation,
    ) -> Result<usize, ControlError> {
        if let Some(record) = self.model.device(reference)?
            && crate::managed::control::required(&record)
        {
            return crate::managed::control::command_count(operation);
        }
        let device = self.controlled_device(reference)?;
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        let plan = operation.plan(&state.record)?;

        if let ControlOperation::Subscriptions { changes } = operation {
            let protocol = state
                .record
                .routing_protocol()?
                .context("device has no ARC service metadata")?;
            let clears = changes
                .iter()
                .filter(|change| {
                    matches!(
                        change,
                        crate::devices::operations::SubscriptionChange::Clear { .. }
                    )
                })
                .count();
            return Ok(clears.div_ceil(protocol.subscription_batch_limit)
                + (changes.len() - clears).div_ceil(protocol.subscription_batch_limit));
        }

        Ok(plan.command_count())
    }

    pub async fn apply_conditionally(
        self: &Arc<Self>,
        reference: &str,
        operation: ControlOperation,
        precondition: Option<Value>,
    ) -> Result<ControlCompletion, ControlError> {
        if self.cancellation.is_cancelled() {
            return Err(ControlError::Unavailable(
                "device registry is stopping".to_owned(),
            ));
        }

        if let Some(record) = self.model.device(reference)?
            && crate::managed::control::required(&record)
        {
            let managed = self.managed.get().ok_or_else(|| {
                ControlError::Unavailable("Managed control is unavailable".into())
            })?;
            return managed
                .control(crate::managed::control::DeviceRequest {
                    caller: crate::managed::sessions::Caller::current(),
                    record: record.as_ref().clone(),
                    operation,
                    precondition,
                    configuration_revision: managed.configuration_revision()?,
                })
                .await;
        }

        let device = self.controlled_device(reference)?;
        let permit = Arc::clone(&self.pending)
            .try_acquire_owned()
            .map_err(|_| ControlError::Unavailable("device operation queue is full".to_owned()))?;
        let registry = Arc::clone(self);
        let (sender, receiver) = oneshot::channel();
        self.tasks.spawn(async move {
            let _permit = permit;
            let result = registry
                .execute_operation(device, operation, precondition)
                .await;

            if let Err(error) = &result {
                tracing::warn!(%error, "device operation failed");
            }

            if sender.send(result).is_err() {
                tracing::info!("device operation completed after its caller disconnected");
            }
        });
        receiver
            .await
            .map_err(|_| ControlError::Unavailable("device operation worker stopped".to_owned()))?
    }

    pub(super) async fn execute_operation(
        &self,
        device: Arc<Device>,
        operation: ControlOperation,
        precondition: Option<Value>,
    ) -> Result<ControlCompletion, ControlError> {
        let _serial = device.refresh.lock().await;
        if device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
            .record
            .retired
        {
            return Err(ControlError::Conflict(
                "device was removed from the cache".to_owned(),
            ));
        }

        let snapshot = || -> Result<Value> {
            device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?
                .record
                .presentation()
        };
        let mut audit =
            crate::devices::audit::Audit::begin(&self.journal, snapshot()?, &operation).await?;
        let mut result = self
            .execute_recorded_operation(&device, &operation, precondition, audit.as_mut())
            .await;

        if let Some(audit) = audit {
            if let Ok(completion) = &mut result
                && let Some(observation) = completion.observation.as_mut()
                && matches!(operation, ControlOperation::FlowAuthoring(_))
            {
                observation["operation_id"] = json!(audit.identifier());
            }

            audit
                .finish(&self.journal, &operation, snapshot()?, &result)
                .await
                .context("record final device operation result; do not replay the command")?;
        }

        result
    }

    pub(super) async fn execute_recorded_operation(
        &self,
        device: &Arc<Device>,
        operation: &ControlOperation,
        precondition: Option<Value>,
        audit: Option<&mut crate::devices::audit::Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        if self.cancellation.is_cancelled() {
            return Err(ControlError::Unavailable(
                "device registry is stopping".to_owned(),
            ));
        }

        let (generation, identity, address, protocol_identifier) = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if self
                .model
                .device(&state.record.key)?
                .as_ref()
                .is_some_and(|record| crate::managed::control::required(record))
            {
                return Err(ControlError::Conflict(
                    "Device became managed before the operation; request a new preview".into(),
                ));
            }

            let protocol = state
                .record
                .routing_protocol()?
                .context("device has no ARC service metadata")?;
            let address = state
                .record
                .control_address()
                .context("device has no routing service")?;
            (
                state.generation,
                state.identity_generation,
                address,
                protocol.protocol_id,
            )
        };
        let mut owned_transport = device.transport.lock().await;

        if owned_transport
            .as_ref()
            .is_none_or(|transport| transport.address() != address)
        {
            *owned_transport = Some(
                DeviceTransport::connect(address, self.local_address(address.address))
                    .await
                    .map_err(|error| ControlError::Transport {
                        error,
                        acknowledged_commands: 0,
                    })?
                    .with_observer(self.wire_observer()),
            );
        }

        let transport = owned_transport
            .as_mut()
            .context("device transport was not created")?;

        if let ControlOperation::Panel(operation) = operation {
            return self
                .control_panel(
                    device,
                    transport,
                    generation,
                    operation,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::Locking(requested) = operation {
            return self
                .configure_lock(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::Maintenance(requested) = operation {
            return self
                .maintain_device(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::SampleRate(requested) = operation {
            return self
                .configure_sample_rate(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if matches!(
            operation,
            ControlOperation::MulticastPrefix(_) | ControlOperation::InspectMulticastPrefix
        ) {
            let configuration = match operation {
                ControlOperation::MulticastPrefix(configuration) => Some(configuration),
                _ => None,
            };
            return self
                .configure_multicast_prefix(
                    device,
                    transport,
                    generation,
                    configuration,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::FlowAuthoring(requested) = operation {
            return self
                .author_flow(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::NetworkConfiguration(requested) = operation {
            return self
                .configure_network(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::StatusConfiguration(requested) = operation {
            return self
                .configure_status(
                    device,
                    transport,
                    generation,
                    requested,
                    precondition.as_ref(),
                    audit,
                )
                .await;
        }

        if let ControlOperation::InspectStatus(inspection) = operation {
            return self
                .inspect_status(device, transport, generation, *inspection)
                .await;
        }

        if matches!(operation, ControlOperation::RefreshExternalReceiver) {
            let fresh = self
                .refresh_external_receiver(device, transport, generation)
                .await?;
            return Ok(ControlCompletion {
                acknowledged_commands: 0,
                readback_completed: fresh.before.is_some(),
                readback_errors: fresh.errors,
                observation: fresh.before,
            });
        }

        if let ControlOperation::ExternalSubscription(subscription) = operation {
            return self
                .execute_external_subscription(
                    device,
                    transport,
                    subscription,
                    generation,
                    precondition,
                    audit,
                )
                .await;
        }

        let initial_clock = match &operation {
            ControlOperation::Clock(clock) => Some(
                self.read_clock(device, transport, generation, clock.profile, None)
                    .await
                    .map_err(|error| ControlError::Unavailable(error.to_string()))?,
            ),
            ControlOperation::RefreshClock { profile } => Some(
                self.read_clock(device, transport, generation, *profile, None)
                    .await
                    .map_err(|error| ControlError::Unavailable(error.to_string()))?,
            ),
            _ => None,
        };
        let audio_kind = match &operation {
            ControlOperation::AudioConfiguration(configuration) => Some(configuration.kind),
            ControlOperation::RefreshAudio(kind) => Some(*kind),
            _ => None,
        };
        let initial_audio = match audio_kind {
            Some(kind) => Some(
                self.read_audio(device, transport, identity, kind, None)
                    .await
                    .map_err(|error| {
                        tracing::warn!(%error, "initial audio configuration observation unavailable");
                        ControlError::ObservationUnavailable(format!("{} readback was unavailable", kind.label()))
                    })?,
            ),
            None => None,
        };
        let mut plan = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation {
                return Err(ControlError::Conflict(
                    "device identity changed before configuration request".to_owned(),
                ));
            }

            if let Some(expected) = &precondition
                && operation.precondition(&state.record.presentation()?) != *expected
            {
                return Err(ControlError::Conflict(
                    "the device configuration changed after confirmation; obtain a new preview before retrying".to_owned(),
                ));
            }

            operation.plan(&state.record)?
        };

        if let ControlReadback::Performance(performance) = &plan.readback {
            self.invalidate_performance(device, generation, performance.requested.keys().copied())?;
        }

        let mut execution = plan.execute(transport).await;

        if let Some(audit) = audit {
            audit
                .acknowledgement(&self.journal, &plan.acknowledgements, plan.command_count())
                .await
                .context("record device acknowledgements; do not replay the command")?;
        }

        if execution.is_ok() {
            for event in plan.pending_events {
                self.model.publish(event)?;
            }
        }

        let mut completion = ControlCompletion {
            acknowledged_commands: execution.as_ref().copied().unwrap_or_default(),
            readback_completed: false,
            readback_errors: Vec::new(),
            observation: None,
        };

        match plan.readback {
            ControlReadback::AudioStatus(kind) => {
                completion.readback_completed = true;
                completion.observation = Some(kind.status(&json!(initial_audio)));
                return Ok(completion);
            }
            ControlReadback::AudioConfiguration {
                operation,
                request_sent,
            } => {
                execution?;
                let observed = if request_sent {
                    match self
                        .read_audio(
                            device,
                            transport,
                            identity,
                            operation.kind,
                            Some(&operation),
                        )
                        .await
                    {
                        Ok(observed) => Some(observed),
                        Err(error) => {
                            completion.readback_errors.push(error.to_string());
                            None
                        }
                    }
                } else {
                    initial_audio
                };
                completion.readback_completed = observed.is_some();
                completion.observation =
                    Some(operation.completion(observed.as_ref(), request_sent)?);
                return Ok(completion);
            }
            ControlReadback::TransmitFlows(protocols) => {
                let inventory = tokio::select! {
                    _ = self.cancellation.cancelled() => return Err(ControlError::Unavailable("device registry is stopping".to_owned())),
                    inventory = crate::devices::flows::read_transmit(transport, &protocols) => inventory?,
                };
                let mut state = device
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

                if state.generation != generation {
                    return Err(ControlError::Conflict(
                        "device identity changed during flow inventory".to_owned(),
                    ));
                }

                for (field, value) in [
                    ("flow_protocol_id", json!(inventory.protocol)),
                    ("transmitter_flows", inventory.raw["flows"].clone()),
                    ("transmitter_flow_inventory", inventory.raw),
                ] {
                    state.record.properties.insert(field.to_owned(), value);
                    state
                        .field_observed_at
                        .insert(field.to_owned(), Instant::now());
                }

                self.publish(&state.record)?;
                let mut inventory = inventory.canonical;
                inventory["device"] = json!(state.record.key);
                completion.readback_completed = true;
                completion.observation = Some(inventory);
                return Ok(completion);
            }
            ControlReadback::Clock(clock) => {
                let observed = if clock.request_sent {
                    match self
                        .read_clock(device, transport, generation, clock.profile, Some(&clock))
                        .await
                    {
                        Ok(observed) => Some(observed),
                        Err(error) => {
                            completion.readback_errors.push(error.to_string());
                            None
                        }
                    }
                } else {
                    initial_clock
                };
                completion.readback_completed = observed.is_some();
                let mut result = clock.completion(observed.as_ref());

                if let Err(error) = execution {
                    result["request_sent"] = json!(false);
                    result["transport_error"] = json!(error.to_string());
                    result["effective_state_confirmed"] = json!(false);
                }

                completion.observation = Some(result);
                return Ok(completion);
            }
            ControlReadback::Performance(performance) => {
                let readback = if let Some(query) = performance.readback_command() {
                    match transport.execute(&query, CommandIntent::Read).await {
                        Ok(response) => {
                            if let Some(settings) =
                                netaudio_core::responses::parse_device_settings(&response)
                            {
                                self.accept_settings(device, generation, &settings)?;
                                completion.readback_completed = true;
                            }

                            Some(response)
                        }
                        Err(error) => {
                            completion.readback_errors.push(error.to_string());
                            None
                        }
                    }
                } else {
                    None
                };
                let mut result =
                    performance.completion(plan.acknowledgements.into_iter().next(), readback)?;

                if let Err(ControlError::Transport { error, .. }) = &execution {
                    result["transport_error"] = json!(error.to_string());
                    result["outcome_unknown"] =
                        json!(matches!(error, TransportError::WriteOutcomeUnknown(_)));
                }

                completion.observation = Some(result);
                return Ok(completion);
            }
            ControlReadback::None => return execution.map(|_| completion),
            ControlReadback::Latency { milliseconds } => {
                let settings = self.read_latency(device, transport, generation).await;
                let acknowledged = match &execution {
                    Ok(_) => Some(true),
                    Err(ControlError::Rejected { .. }) => Some(false),
                    Err(_) => None,
                };
                let observed = match settings {
                    Ok(settings) => {
                        completion.readback_completed = true;
                        Some(serde_json::to_value(settings).map_err(anyhow::Error::from)?)
                    }
                    Err(error) => {
                        completion.readback_errors.push(error.to_string());
                        None
                    }
                };
                let request: netaudio_core::latency_configuration::LatencyControl =
                    serde_json::from_value(json!({
                        "requested_milliseconds": milliseconds,
                        "acknowledged": acknowledged,
                        "settings": observed,
                    }))
                    .map_err(anyhow::Error::from)?;
                completion.observation = Some(
                    serde_json::to_value(
                        request
                            .resolve()
                            .map_err(|error| anyhow::anyhow!("{error}"))?,
                    )
                    .map_err(anyhow::Error::from)?,
                );
                return execution.map(|_| completion);
            }
            ControlReadback::Inventory => {}
        }

        let started = Instant::now();
        match read_inventory(transport, protocol_identifier).await {
            Ok(data) => {
                let report = self.accept_inventory(device, generation, started, data)?;
                completion.readback_completed = !report.superseded && report.errors.is_empty();
                completion.readback_errors = report.errors.into_values().collect();
            }
            Err(error) => {
                tracing::warn!(%error, "device operation readback failed");
                completion.readback_errors.push(error.to_string());
            }
        }

        if let Err(ControlError::Subscription(failure)) = &mut execution {
            failure.readback_completed = completion.readback_completed;
            failure
                .readback_errors
                .clone_from(&completion.readback_errors);
        }

        execution.map(|_| completion)
    }

    pub(super) async fn read_latency(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
    ) -> Result<DeviceSettings> {
        let response = transport
            .execute(
                &json!({"command": "query_latency_config"}),
                CommandIntent::Read,
            )
            .await?;
        let settings = netaudio_core::responses::parse_device_settings(&response)
            .ok_or(TransportError::MalformedResponse("latency readback"))?;
        self.accept_settings(device, generation, &settings)?;
        Ok(settings)
    }

    pub(super) async fn read_clock(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        profile: u16,
        expected: Option<&crate::devices::clock::ClockReadback>,
    ) -> Result<serde_json::Map<String, Value>> {
        let key = {
            let state = device
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

            if state.generation != generation || !state.record.online() {
                bail!("device identity changed before clock observation");
            }

            state.record.key.clone()
        };
        let observed = tokio::select! {
            _ = self.cancellation.cancelled() => bail!("device registry is stopping"),
            observed = self.status_requests.query_until(
                transport,
                &key,
                None,
                StatusQuery::ClockProfile(profile),
                |fields| expected.is_none_or(|expected| expected.matches(fields)),
            ) => observed?,
        };
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation {
            bail!("device identity changed during clock observation");
        }

        Ok(observed)
    }

    pub(super) fn accept_settings(
        &self,
        device: &Device,
        generation: u64,
        settings: &DeviceSettings,
    ) -> Result<()> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation {
            bail!("device identity changed during configuration readback");
        }

        let previous = state.record.properties.clone();
        state.record.apply_settings(settings)?;
        let received = Instant::now();
        let changed: Vec<_> = state
            .record
            .properties
            .iter()
            .filter(|(field, value)| previous.get(*field) != Some(*value))
            .map(|(field, _)| field.clone())
            .collect();

        for field in changed {
            state.field_observed_at.insert(field, received);
        }

        self.publish(&state.record)?;
        Ok(())
    }

    pub(super) fn invalidate_performance(
        &self,
        device: &Device,
        generation: u64,
        properties: impl Iterator<Item = u16>,
    ) -> Result<()> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;

        if state.generation != generation {
            bail!("device identity changed before configuration request");
        }

        let mut invalidated = false;

        if let Some(values) = state
            .record
            .properties
            .get_mut("performance_settings")
            .and_then(Value::as_object_mut)
        {
            for property in properties {
                invalidated |= values.remove(&property.to_string()).is_some();
            }
        }

        if invalidated {
            state
                .field_observed_at
                .insert("performance_settings".to_owned(), Instant::now());
            self.publish(&state.record)?;
        }

        Ok(())
    }
}
