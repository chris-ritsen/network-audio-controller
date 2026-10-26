use super::operations::{ControlCompletion, ControlError, ControlOperation};
use super::performance::PerformanceOperation;
use crate::monitoring::journal::model::{
    Derivation, EventKind, LifecyclePhase, OperationRequest, Severity,
};
use crate::monitoring::journal::service::Journal;
use anyhow::{Context, Result};
use rand::RngCore;
use serde_json::{Value, json};

pub(crate) mod outcome;

pub struct Audit {
    request: OperationRequest,
    rejected: bool,
    storage: bool,
}

impl Audit {
    pub async fn begin(
        journal: &Journal,
        snapshot: Value,
        operation: &ControlOperation,
    ) -> Result<Option<Self>> {
        Self::begin_child(journal, snapshot, operation, None).await
    }

    pub async fn begin_child(
        journal: &Journal,
        snapshot: Value,
        operation: &ControlOperation,
        parent_preset_run_id: Option<&str>,
    ) -> Result<Option<Self>> {
        let (name, requested, channel) = match operation {
            ControlOperation::Panel(super::panels::Operation::Apply(change)) => (
                "device_control".to_owned(),
                json!({"category": change.category, "requested": change.requested, "confirm_clear": change.confirm_clear}),
                None,
            ),
            ControlOperation::Panel(_) => return Ok(None),
            ControlOperation::Locking(operation) => (
                operation.name().to_owned(),
                json!({"is_locked": operation.locking}),
                None,
            ),
            ControlOperation::Maintenance(operation) => {
                (operation.name().to_owned(), operation.requested(), None)
            }
            ControlOperation::SampleRate(operation) if operation.preview => return Ok(None),
            ControlOperation::SampleRate(operation) => (
                "set_sample_rate".to_owned(),
                json!({"sample_rate": operation.target, "confirm_destructive": operation.confirm_destructive}),
                None,
            ),
            ControlOperation::MulticastPrefix(configuration) => (
                "set_aes67_multicast_prefix".to_owned(),
                json!({"prefix": configuration.prefix}),
                None,
            ),
            ControlOperation::FlowAuthoring(operation) => {
                (operation.name().to_owned(), operation.requested(), None)
            }
            ControlOperation::NetworkConfiguration(configuration) => (
                configuration.name().to_owned(),
                configuration.requested(),
                None,
            ),
            ControlOperation::StatusConfiguration(configuration) => (
                configuration.name().to_owned(),
                configuration.requested(),
                None,
            ),
            ControlOperation::ExternalSubscription(operation) => (
                "subscribe_external_rtp".to_owned(),
                operation.requested(),
                None,
            ),
            ControlOperation::InspectMulticastPrefix
            | ControlOperation::InspectStatus(_)
            | ControlOperation::RefreshExternalReceiver
            | ControlOperation::RefreshAudio(_)
            | ControlOperation::RefreshClock { .. }
            | ControlOperation::InspectTransmitFlows => return Ok(None),
            ControlOperation::AudioConfiguration(configuration) => (
                format!("set_{}", configuration.kind.name()),
                json!({configuration.kind.parameter(): configuration.requested}),
                None,
            ),
            ControlOperation::RenameDevice { name } => {
                ("set_device_name".to_owned(), json!({"name": name}), None)
            }
            ControlOperation::RenameChannel {
                direction,
                number,
                name,
            } => (
                "set_channel_name".to_owned(),
                json!({"direction": direction, "channel": number, "name": name}),
                Some(format!("{}:{number}", direction.command_value())),
            ),
            ControlOperation::SetLatency { milliseconds } => (
                "set_latency".to_owned(),
                json!({"milliseconds": milliseconds}),
                None,
            ),
            ControlOperation::Identify => ("identify".to_owned(), json!({}), None),
            ControlOperation::Clock(clock) => {
                ("clock_configuration".to_owned(), json!(clock.changes), None)
            }
            ControlOperation::Performance(performance) => {
                let mut requested = serde_json::to_value(performance)?;
                requested
                    .as_object_mut()
                    .context("performance request must be an object")?
                    .remove("operation");
                let name = if matches!(performance, PerformanceOperation::StoreCurrentConfiguration)
                {
                    performance.name().to_owned()
                } else {
                    format!("set_{}", performance.name())
                };
                (name, requested, None)
            }
            ControlOperation::Subscriptions { changes } => (
                "subscriptions".to_owned(),
                json!({"changes": changes}),
                (changes.len() == 1).then(|| format!("rx:{}", changes[0].receiver_channel())),
            ),
        };
        let mut identifier = [0_u8; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut identifier)
            .context("generate operation identity")?;
        let identifier = hex::encode(identifier);
        let request = OperationRequest {
            device_snapshot: snapshot,
            observe_device: false,
            kind: EventKind::ConfigurationOperation,
            severity: Severity::Info,
            derivation_status: Derivation::Derived,
            operation_id: identifier.clone(),
            correlation_id: identifier,
            parent_preset_run_id: parent_preset_run_id.map(str::to_owned),
            operation_name: name,
            lifecycle_phase: LifecyclePhase::Requested,
            requested_values: Some(requested.clone()),
            acknowledgement_result_code: None,
            transport: Some("direct".to_owned()),
            effective_values: None,
            final_operation_state: Some("requested".to_owned()),
            persistence_request_acknowledgement: None,
            persistence_confirmation: None,
            evidence: Some(serde_json::Map::from_iter([(
                "request".to_owned(),
                requested,
            )])),
            observation_source: "user_request".to_owned(),
            interface_identity: None,
            channel_identity: channel,
            flow_identity: None,
        };
        journal
            .operation(request.clone())
            .await
            .context("record operation before sending commands")?;
        Ok(Some(Self {
            request,
            rejected: false,
            storage: matches!(
                operation,
                ControlOperation::Performance(PerformanceOperation::StoreCurrentConfiguration)
            ),
        }))
    }

    pub fn identifier(&self) -> &str {
        &self.request.operation_id
    }

    pub async fn acknowledgement(
        &mut self,
        journal: &Journal,
        responses: &[Vec<u8>],
        command_count: usize,
    ) -> Result<()> {
        let receipts = responses
            .iter()
            .map(|response| {
                serde_json::to_value(netaudio_core::responses::command_receipt(response))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.request.evidence = Some(serde_json::Map::from_iter([
            ("command_count".to_owned(), json!(command_count)),
            (
                "acknowledged_commands".to_owned(),
                json!(
                    receipts
                        .iter()
                        .filter(|receipt| receipt["accepted"] == true)
                        .count()
                ),
            ),
            ("acknowledgements".to_owned(), json!(receipts)),
        ]));

        if let Some(receipt) = receipts.iter().rfind(|receipt| receipt["accepted"] == true) {
            self.request.acknowledgement_result_code = receipt["result_code"].as_i64();
            self.request.derivation_status = Derivation::Observed;
            self.request.observation_source = "operation_transport_acknowledgement".to_owned();

            let (phase, state) = if self.storage {
                self.request.persistence_request_acknowledgement = Some(receipt.clone());
                (
                    LifecyclePhase::PersistenceRequestAcknowledged,
                    "request_acknowledged",
                )
            } else {
                (LifecyclePhase::RequestAcknowledged, "request_acknowledged")
            };
            self.transition(journal, phase, state).await?;
        }

        if let Some(receipt) = receipts.iter().find(|receipt| receipt["accepted"] == false) {
            self.rejected = true;
            self.request.acknowledgement_result_code = receipt["result_code"].as_i64();
            self.request.derivation_status = Derivation::Observed;
            self.request.observation_source = "operation_transport_acknowledgement".to_owned();
            self.request.severity = Severity::Warning;
            self.transition(journal, LifecyclePhase::RequestRejected, "rejected")
                .await?;
        }

        Ok(())
    }

    pub async fn finish(
        mut self,
        journal: &Journal,
        operation: &ControlOperation,
        snapshot: Value,
        result: &Result<ControlCompletion, ControlError>,
    ) -> Result<()> {
        self.request.device_snapshot = snapshot;
        let (phase, state) = match result {
            Ok(completion) => {
                if matches!(operation, ControlOperation::Panel(_))
                    && let Some(observation) = &completion.observation
                    && matches!(
                        observation["plan"]["action"].as_str(),
                        Some("unsupported" | "unavailable")
                    )
                {
                    self.request.effective_values = Some(observation.clone());
                    self.request.observation_source = "panel_preflight".to_owned();
                    return self
                        .transition(journal, LifecyclePhase::RequestRejected, "rejected")
                        .await;
                }

                if matches!(operation, ControlOperation::SampleRate(_))
                    && let Some(observation) = &completion.observation
                    && matches!(
                        observation["state"].as_str(),
                        Some("unsupported" | "confirmation_required")
                    )
                {
                    self.request.effective_values = Some(observation.clone());
                    self.request.observation_source = "sample_rate_preflight".to_owned();
                    return self
                        .transition(journal, LifecyclePhase::RequestRejected, "rejected")
                        .await;
                }

                if let ControlOperation::FlowAuthoring(operation) = operation
                    && operation.is_planning()
                {
                    let observation = completion.observation.clone().unwrap_or(Value::Null);
                    self.request.effective_values = Some(observation.clone());
                    let supported = observation["plan"]["supported"] == true;
                    self.request.observation_source = "flow_planning".to_owned();
                    return self
                        .transition(
                            journal,
                            if supported {
                                LifecyclePhase::Requested
                            } else {
                                LifecyclePhase::RequestRejected
                            },
                            if supported { "planned" } else { "unsupported" },
                        )
                        .await;
                }

                let outcome =
                    outcome::observe(operation, &self.request.device_snapshot, completion);
                self.request.effective_values = outcome.effective;
                let evidence = self.request.evidence.get_or_insert_with(Default::default);
                evidence.insert("completion".to_owned(), serde_json::to_value(completion)?);

                if self.rejected {
                    return self
                        .transition(journal, LifecyclePhase::RequestRejected, "rejected")
                        .await;
                }

                if self.storage && self.request.persistence_request_acknowledgement.is_some() {
                    return self
                        .transition(
                            journal,
                            LifecyclePhase::PersistenceRequestAcknowledged,
                            "request_acknowledged",
                        )
                        .await;
                }

                self.request.observation_source = "fresh_parsed_readback".to_owned();
                match outcome.confirmed {
                    Some(true) => {
                        self.request.derivation_status = Derivation::Observed;
                        (LifecyclePhase::EffectiveStateConfirmed, "confirmed")
                    }
                    Some(false) => {
                        self.request.derivation_status = Derivation::Observed;
                        self.request.severity = Severity::Error;
                        (LifecyclePhase::Inconsistent, "inconsistent")
                    }
                    None => {
                        self.request.derivation_status = Derivation::Derived;
                        self.request.severity = Severity::Warning;
                        self.request.observation_source = "bounded_verification".to_owned();
                        (LifecyclePhase::PartialUnobservable, "unverified")
                    }
                }
            }
            Err(error) => {
                self.request.severity = Severity::Error;
                self.request.derivation_status = Derivation::Observed;
                self.request.observation_source = "operation_exception".to_owned();
                self.request
                    .evidence
                    .get_or_insert_with(Default::default)
                    .insert("failure".to_owned(), outcome::failure(error));

                if self.rejected {
                    (LifecyclePhase::RequestRejected, "rejected")
                } else {
                    (LifecyclePhase::TransportOrValidationFailure, "failed")
                }
            }
        };
        self.transition(journal, phase, state).await
    }

    async fn transition(
        &mut self,
        journal: &Journal,
        phase: LifecyclePhase,
        state: &str,
    ) -> Result<()> {
        self.request.lifecycle_phase = phase;
        self.request.final_operation_state = Some(state.to_owned());
        journal.operation(self.request.clone()).await?;
        Ok(())
    }
}
