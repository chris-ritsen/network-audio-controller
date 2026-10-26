use crate::monitoring::journal::{
    model::{Derivation, EventKind, LifecyclePhase, OperationRequest, Severity},
    service::Journal,
};
use anyhow::{Context, Result};
use netaudio_core::shure::{Change, Device};
use rand::RngCore;
use serde_json::{Map, Value, json};

pub(super) struct Audit {
    request: OperationRequest,
}

impl Audit {
    pub async fn begin(journal: &Journal, device: &Device, change: &Change) -> Result<Self> {
        let mut bytes = [0; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut bytes)
            .context("Generate wireless operation identity")?;
        let identifier = hex::encode(bytes);
        let mut snapshot = device.presentation();
        snapshot["device_identity"] = json!(format!("shure:{}", device.mac));
        snapshot["server_name"] = json!(format!("shure:{}", device.mac));
        let request = OperationRequest {
            device_snapshot: snapshot,
            observe_device: false,
            kind: EventKind::ConfigurationOperation,
            severity: Severity::Info,
            derivation_status: Derivation::Derived,
            operation_id: identifier.clone(),
            correlation_id: identifier,
            parent_preset_run_id: None,
            operation_name: "set_wireless_value".to_owned(),
            lifecycle_phase: LifecyclePhase::Requested,
            requested_values: Some(serde_json::to_value(change)?),
            acknowledgement_result_code: None,
            transport: Some("shure_tcp".to_owned()),
            effective_values: None,
            final_operation_state: Some("requested".to_owned()),
            persistence_request_acknowledgement: None,
            persistence_confirmation: None,
            evidence: None,
            observation_source: "user_request".to_owned(),
            interface_identity: None,
            channel_identity: change.channel.map(|channel| channel.to_string()),
            flow_identity: None,
        };
        journal
            .operation(request.clone())
            .await
            .context("Record wireless operation before sending its command")?;
        Ok(Self { request })
    }

    pub fn identifier(&self) -> &str {
        &self.request.operation_id
    }

    pub async fn finish(mut self, journal: &Journal, result: &Value) -> Result<()> {
        let rejected = result["rejected"] == true;
        let verified = result["verified"] == true;
        self.request.lifecycle_phase = if rejected {
            LifecyclePhase::RequestRejected
        } else if verified && result["verification_scope"] == "command_report" {
            LifecyclePhase::RequestAcknowledged
        } else if verified {
            LifecyclePhase::EffectiveStateConfirmed
        } else {
            LifecyclePhase::PartialUnobservable
        };
        self.request.final_operation_state = Some(
            if rejected {
                "rejected"
            } else if verified {
                "confirmed"
            } else {
                "unverified"
            }
            .to_owned(),
        );
        self.request.derivation_status = Derivation::Observed;
        self.request.observation_source = if result["reported"].is_string() || rejected {
            "shure_control_report"
        } else {
            "shure_control_transport"
        }
        .to_owned();
        self.request.effective_values = result.get("reported").cloned();
        self.request.evidence = Some(Map::from_iter([("completion".to_owned(), result.clone())]));
        self.request.severity = if verified {
            Severity::Info
        } else {
            Severity::Warning
        };
        journal
            .operation(self.request)
            .await
            .context("Record wireless operation completion")?;
        Ok(())
    }
}
