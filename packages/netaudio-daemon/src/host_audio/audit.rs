use super::provider::Outcome;
use crate::monitoring::journal::{
    model::{Derivation, EventKind, LifecyclePhase, OperationRequest, Severity},
    service::Journal,
};
use anyhow::{Context, Result};
use netaudio_core::host_audio::{control::Command, protocol::Provider};
use rand::RngCore;
use serde_json::{Map, Value, json};

pub(super) struct Audit {
    request: OperationRequest,
}

impl Audit {
    pub async fn begin(
        journal: &Journal,
        host: &str,
        provider: Provider,
        command: &Command,
    ) -> Result<Self> {
        let provider = match provider {
            Provider::Jack => "jack",
            Provider::Pulse => "pulse",
        };
        let operation = match command {
            Command::JackConnection { connect: true, .. } => "connect_audio_ports",
            Command::JackConnection { connect: false, .. } => "disconnect_audio_ports",
            Command::PulseVolume { .. } => "set_audio_volume",
            Command::PulseMute { .. } => "set_audio_mute",
            Command::PulseDefault { .. } => "set_default_audio_device",
            Command::PulseMove { .. } => "move_audio_stream",
        };
        Self::begin_request(
            journal,
            host,
            provider,
            operation,
            serde_json::to_value(command)?,
        )
        .await
    }

    pub async fn begin_request(
        journal: &Journal,
        host: &str,
        provider: &str,
        operation: &str,
        command: Value,
    ) -> Result<Self> {
        let mut identity = [0; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut identity)
            .context("generate host audio operation identity")?;
        let identifier = hex::encode(identity);
        let request = OperationRequest {
            device_snapshot: json!({"device_identity":format!("host_audio:{host}:{provider}"),"server_name":format!("host_audio:{host}:{provider}"),"name":host}),
            observe_device: false,
            kind: EventKind::ConfigurationOperation,
            severity: Severity::Info,
            derivation_status: Derivation::Derived,
            operation_id: identifier.clone(),
            correlation_id: identifier,
            parent_preset_run_id: None,
            operation_name: operation.into(),
            lifecycle_phase: LifecyclePhase::Requested,
            requested_values: Some(command),
            acknowledgement_result_code: None,
            transport: Some(format!("host_audio_{provider}")),
            effective_values: None,
            final_operation_state: Some("requested".into()),
            persistence_request_acknowledgement: None,
            persistence_confirmation: None,
            evidence: None,
            observation_source: "user_request".into(),
            interface_identity: None,
            channel_identity: None,
            flow_identity: None,
        };
        journal
            .operation(request.clone())
            .await
            .context("record host audio operation before dispatch")?;
        Ok(Self { request })
    }

    pub fn identifier(&self) -> &str {
        &self.request.operation_id
    }

    pub async fn finish(mut self, journal: &Journal, outcome: &Outcome) -> Result<()> {
        let verified = outcome.body["state_verified"] == true;
        let refused = outcome.body["accepted"] == false
            || (outcome.status >= 400 && outcome.body["change_sent"] == false);
        self.request.lifecycle_phase = if verified {
            LifecyclePhase::EffectiveStateConfirmed
        } else if refused {
            LifecyclePhase::RequestRejected
        } else {
            LifecyclePhase::PartialUnobservable
        };
        self.request.final_operation_state = Some(
            if verified {
                "confirmed"
            } else if refused {
                "rejected"
            } else {
                "unverified"
            }
            .into(),
        );
        self.request.derivation_status = Derivation::Observed;
        self.request.observation_source = if verified {
            "host_audio_readback"
        } else {
            "host_audio_worker_result"
        }
        .into();
        self.request.effective_values = outcome.body.get("now").cloned();
        self.request.evidence = Some(Map::from_iter([(
            "completion".into(),
            outcome.body.clone(),
        )]));
        self.request.severity = if verified {
            Severity::Info
        } else {
            Severity::Warning
        };
        journal
            .operation(self.request)
            .await
            .context("record host audio operation completion")?;
        Ok(())
    }
}
