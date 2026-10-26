use super::{Error, mutations::Request};
use crate::monitoring::journal::{
    model::{Derivation, EventKind, LifecyclePhase, OperationRequest, Severity},
    service::Journal,
};
use rand::RngCore;
use serde_json::{Value, json};

pub(super) struct Audit {
    request: OperationRequest,
}

impl Audit {
    pub async fn begin(journal: &Journal, operation: &Request) -> Result<Self, Error> {
        Self::begin_named(
            journal,
            &operation.server,
            operation.mutation.operation_name(),
            json!({"server": operation.server, "mutation": operation.mutation}),
            operation.correlation_id.clone(),
        )
        .await
    }

    pub async fn begin_named(
        journal: &Journal,
        server: &str,
        name: &str,
        requested: Value,
        correlation: Option<String>,
    ) -> Result<Self, Error> {
        Self::begin_snapshot(
            journal,
            json!({"device_identity": format!("managed-server:{server}"), "name": server, "server_name": ""}),
            name,
            requested,
            correlation,
        ).await
    }

    pub async fn begin_device(
        journal: &Journal,
        snapshot: Value,
        name: &str,
        requested: Value,
    ) -> Result<Self, Error> {
        Self::begin_snapshot(journal, snapshot, name, requested, None).await
    }

    async fn begin_snapshot(
        journal: &Journal,
        snapshot: Value,
        name: &str,
        requested: Value,
        correlation: Option<String>,
    ) -> Result<Self, Error> {
        let mut identifier = [0_u8; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut identifier)
            .map_err(|_| Error::Persistence("Managed operation identity could not be generated"))?;
        let identifier = hex::encode(identifier);
        let request = OperationRequest {
            device_snapshot: snapshot,
            observe_device: false,
            kind: EventKind::ConfigurationOperation,
            severity: Severity::Info,
            derivation_status: Derivation::Derived,
            operation_id: identifier.clone(),
            correlation_id: correlation.unwrap_or(identifier),
            parent_preset_run_id: None,
            operation_name: name.into(),
            lifecycle_phase: LifecyclePhase::Requested,
            requested_values: Some(requested),
            acknowledgement_result_code: None,
            transport: Some("ddm".into()),
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
        journal.operation(request.clone()).await.map_err(|_| {
            Error::Persistence("Managed operation could not be recorded; no mutation was sent")
        })?;
        Ok(Self { request })
    }

    pub fn identifier(&self) -> &str {
        &self.request.operation_id
    }

    pub async fn acknowledgement(
        &mut self,
        journal: &Journal,
        result: &Value,
    ) -> Result<(), Error> {
        self.request.lifecycle_phase = match result["accepted"].as_bool() {
            Some(true) => LifecyclePhase::RequestAcknowledged,
            Some(false) => LifecyclePhase::RequestRejected,
            None => LifecyclePhase::TransportOrValidationFailure,
        };
        self.request.acknowledgement_result_code = result["result_code"].as_i64();
        self.request.derivation_status = Derivation::Observed;
        self.request.observation_source = if result["control_transport"] == "controller" {
            controller_source(result)
        } else {
            "managed_api_response"
        }
        .into();
        self.request.final_operation_state = Some(
            match result["accepted"].as_bool() {
                Some(true) => "accepted",
                Some(false) => "rejected",
                None => "unconfirmed",
            }
            .into(),
        );
        self.request.effective_values = Some(result.clone());
        journal.operation(self.request.clone()).await.map_err(|_| Error::Persistence("Managed acknowledgement could not be recorded; check current state before retrying"))?;
        Ok(())
    }

    pub async fn finish(mut self, journal: &Journal, result: &Value) -> Result<(), Error> {
        self.request.lifecycle_phase = match result["state"].as_str() {
            Some("confirmed") => LifecyclePhase::EffectiveStateConfirmed,
            Some("inconsistent" | "contradicted") => LifecyclePhase::Inconsistent,
            Some("rejected") | Some("refused") => LifecyclePhase::RequestRejected,
            _ => LifecyclePhase::PartialUnobservable,
        };
        self.request.severity = if matches!(
            result["state"].as_str(),
            Some("confirmed" | "configured" | "executed")
        ) {
            Severity::Info
        } else {
            Severity::Warning
        };
        self.request.derivation_status = Derivation::Observed;
        self.request.observation_source = if result["readback_transport"] == "managed_api"
            && result["readback_fresh"] == true
        {
            "managed_inventory_readback"
        } else if result["control_transport"] == "controller" && result["readback_fresh"] == true {
            "managed_controller_readback"
        } else if result["request_attempted"] != true {
            "managed_operation_preflight"
        } else if result["control_transport"] == "controller" {
            controller_source(result)
        } else {
            "managed_inventory_readback"
        }
        .into();
        self.request.final_operation_state = result["state"].as_str().map(str::to_owned);
        self.request.effective_values = Some(result.clone());
        journal.operation(self.request).await.map_err(|_| Error::Persistence("Managed operation result could not be recorded; check current state before retrying"))?;
        Ok(())
    }
}

fn controller_source(result: &Value) -> &'static str {
    if result["execution_report_fresh"] == true {
        "managed_controller_execution_report"
    } else if result["accepted"].is_boolean() {
        "managed_controller_response"
    } else {
        "managed_controller_transport"
    }
}
