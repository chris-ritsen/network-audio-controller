use crate::monitoring::journal::model::{
    Derivation, EventKind, LifecyclePhase, OperationRequest, Severity,
};
use crate::monitoring::journal::service::Journal;
use anyhow::{Context, Result};
use rand::RngCore;
use serde_json::json;

pub struct PresetAudit {
    request: OperationRequest,
}

impl PresetAudit {
    pub async fn begin(
        journal: &Journal,
        name: &str,
        digest: &str,
        devices: &[String],
        store: bool,
    ) -> Result<Self> {
        let mut identity = [0_u8; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut identity)
            .context("generate preset operation identity")?;
        let identity = hex::encode(identity);
        let request = OperationRequest {
            device_snapshot: json!({"device_identity": format!("preset-run:{identity}"), "name": name, "server_name": ""}),
            observe_device: false,
            kind: EventKind::PresetRun,
            severity: Severity::Info,
            derivation_status: Derivation::Derived,
            operation_id: identity.clone(),
            correlation_id: identity,
            parent_preset_run_id: None,
            operation_name: "apply_preset".into(),
            lifecycle_phase: LifecyclePhase::Requested,
            requested_values: Some(
                json!({"digest": digest, "devices": devices, "store_current_configuration": store}),
            ),
            acknowledgement_result_code: None,
            transport: Some("preset".into()),
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
            .context("record preset operation before sending changes")?;
        Ok(Self { request })
    }

    pub fn identifier(&self) -> &str {
        &self.request.operation_id
    }

    pub async fn finish(
        mut self,
        journal: &Journal,
        report: &super::report::Report,
        interrupted: bool,
    ) -> Result<()> {
        let complete = report.failures == 0 && report.unverified == 0 && !interrupted;
        self.request.lifecycle_phase = if complete {
            LifecyclePhase::EffectiveStateConfirmed
        } else {
            LifecyclePhase::PartialUnobservable
        };
        self.request.final_operation_state = Some(
            if report.failures > 0 {
                "failed"
            } else if complete {
                "confirmed"
            } else {
                "partial"
            }
            .into(),
        );
        self.request.severity = if report.failures > 0 {
            Severity::Error
        } else if complete {
            Severity::Info
        } else {
            Severity::Warning
        };
        self.request.effective_values = Some(serde_json::to_value(report)?);
        self.request.observation_source = "preset_operation_results".into();
        journal
            .operation(self.request)
            .await
            .context("record final preset result; do not replay the preset")?;
        Ok(())
    }
}
