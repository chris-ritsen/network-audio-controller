use super::{
    Error,
    audit::Audit,
    control::DeviceRequest,
    device_session::{DeviceSession, identities},
    registry::{Registry, Source},
};
use crate::devices::{
    maintenance,
    operations::{ControlCompletion, ControlError, ControlOperation},
};
use netaudio_core::{
    capabilities::{self, AvailabilityRequest},
    managed_session::ManagedOperation,
};
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) enum Action {
    Identify,
    Reboot,
}

impl Action {
    pub fn from_operation(operation: &ControlOperation) -> Option<Self> {
        match operation {
            ControlOperation::Identify => Some(Self::Identify),
            ControlOperation::Maintenance(maintenance::Operation::Reboot) => Some(Self::Reboot),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Identify => "identify",
            Self::Reboot => "reboot",
        }
    }

    pub fn validate(self, record: &Value) -> Result<(), ControlError> {
        if record["ddm_fresh"] != true || record["ddm_connection_state"] != "READY" {
            return Err(ControlError::ObservationUnavailable(
                "The selected managed device is not currently ready".into(),
            ));
        }
        match self {
            Self::Identify => {
                let availability = capabilities::availability(AvailabilityRequest {
                    operation: capabilities::Operation::Identify,
                    supported: record["identify_supported"].as_bool(),
                    supported_source: None,
                    readable: false,
                    read_only: None,
                    read_only_source: None,
                    locked: record["is_locked"].as_bool(),
                    audio: None,
                    redundancy: None,
                    transport_available: true,
                    has_adapter: false,
                    managed: true,
                    permission: record["managed_operation_permissions"]["identify"].as_bool(),
                });
                if !availability.write_permitted {
                    return Err(ControlError::Conflict(format!(
                        "Managed identification is unavailable: {}",
                        availability.reasons.join(", ")
                    )));
                }
            }
            Self::Reboot if record["ddm_capabilities"]["can_reset"] != true => {
                return Err(ControlError::Conflict(
                    "The managed server does not currently permit resetting this device".into(),
                ));
            }
            Self::Reboot => {}
        }
        Ok(())
    }

    fn operation(self, device_id: String, host_mac: [u8; 6]) -> ManagedOperation {
        match self {
            Self::Identify => ManagedOperation::Identify {
                device_id,
                host_mac,
            },
            Self::Reboot => ManagedOperation::Reboot {
                device_id,
                host_mac,
            },
        }
    }
}

impl Registry {
    pub(super) async fn controller_control(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        action: Action,
    ) -> Result<ControlCompletion, ControlError> {
        let mut audit = Audit::begin_device(
            &self.journal,
            request.record.clone(),
            action.name(),
            json!({"device_id": request.record["ddm_device_id"], "domain_id": request.record["ddm_domain_id"], "command": action.name()}),
        ).await.map_err(control_error)?;
        let mut value = json!({
            "operation_id": audit.identifier(), "operation": action.name(),
            "transport": "ddm", "control_transport": "controller",
            "request_attempted": false, "request_sent": false, "accepted": false,
            "state_verified": false, "verified": false, "effective_state_confirmed": false,
            "readback_fresh": false, "request_acknowledged": false, "change_sent": false,
            "state": "refused",
        });
        let preflight = async {
            let fresh = self.fresh_target(request, source, revision).await?;
            action.validate(&fresh.record)?;
            identities(&fresh)
        }
        .await;
        let (device_id, domain) = match preflight {
            Ok(target) => target,
            Err(error) => {
                value["error"] = json!(error.to_string());
                audit
                    .finish(&self.journal, &value)
                    .await
                    .map_err(control_error)?;
                return Err(ControlError::Managed {
                    status: 409,
                    result: value,
                });
            }
        };
        value["request_attempted"] = json!(true);
        let response = async {
            let mut session = DeviceSession::connect(source, device_id, domain).await?;
            session
                .exchange(action.operation(session.device_id.clone(), session.host_mac))
                .await
        }
        .await;
        let mut status = match response {
            Ok(exchange) => {
                value["request_sent"] = json!(exchange.request_sent);
                value["accepted"] = json!(true);
                value["state"] = json!("accepted");
                200
            }
            Err(failure) => {
                value["request_sent"] = json!(failure.request_sent);
                value["accepted"] = Value::Null;
                value["state"] = json!("unconfirmed");
                value["error"] = json!(failure.to_string());
                502
            }
        };
        if value["accepted"] == true
            && matches!(action, Action::Identify)
            && let Err(error) = self
                .model
                .publish(crate::devices::operations::identify_event(
                    request.record["server_name"]
                        .as_str()
                        .expect("validated target name"),
                ))
        {
            value["event_error"] = json!(error.to_string());
            status = 500;
        }
        value["change_sent"] = value["request_sent"].clone();
        value["request_acknowledged"] = value["accepted"].clone();
        if let Err(error) = audit.acknowledgement(&self.journal, &value).await {
            value["audit_error"] = json!(error.to_string());
            status = 500;
        }
        if let Err(error) = audit.finish(&self.journal, &value).await {
            value["audit_error"] = json!(error.to_string());
            status = 500;
        }
        if status != 200 {
            return Err(ControlError::Managed {
                status,
                result: value,
            });
        }
        Ok(ControlCompletion {
            acknowledged_commands: 1,
            readback_completed: false,
            readback_errors: Vec::new(),
            observation: Some(value),
        })
    }
}

fn control_error(error: Error) -> ControlError {
    ControlError::Unavailable(error.to_string())
}
