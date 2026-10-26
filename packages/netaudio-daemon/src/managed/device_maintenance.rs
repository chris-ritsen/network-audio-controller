use super::{
    audit::Audit,
    control::{DeviceRequest, FreshTarget},
    device_control::Action,
    device_session::{DeviceSession, identities},
    registry::{Registry, Source},
};
use crate::{
    devices::{
        maintenance::Operation,
        operations::{ControlCompletion, ControlError, ControlOperation},
    },
    monitoring::requests::StatusQuery,
};
use netaudio_core::managed_session::ManagedOperation;
use serde_json::{Value, json};
use std::sync::Arc;

pub(super) fn operation(operation: &ControlOperation) -> Option<&Operation> {
    match operation {
        ControlOperation::Maintenance(operation @ Operation::ClearConfiguration { .. }) => {
            Some(operation)
        }
        _ => None,
    }
}

pub(super) fn validate(record: &Value, operation: &Operation) -> Result<(), ControlError> {
    if record["ddm_fresh"] != true || record["ddm_connection_state"] != "READY" {
        return Err(ControlError::ObservationUnavailable(
            "The selected managed device is not currently ready".into(),
        ));
    }
    match record["is_locked"].as_bool() {
        Some(false) => {}
        Some(true) => return Err(ControlError::Conflict("device is locked".into())),
        None => {
            return Err(ControlError::ObservationUnavailable(
                "device lock state is unknown; no clear command was sent".into(),
            ));
        }
    }
    if matches!(
        operation,
        Operation::ClearConfiguration { reboot: true, .. }
    ) {
        Action::Reboot.validate(record)?;
    }
    Ok(())
}

impl Registry {
    pub(super) async fn controller_clear(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        operation: &Operation,
    ) -> Result<ControlCompletion, ControlError> {
        let mut audit = Audit::begin_device(
            &self.journal,
            request.record.clone(),
            operation.name(),
            operation.requested(),
        )
        .await
        .map_err(|error| ControlError::Unavailable(error.to_string()))?;
        let mut value = json!({
            "operation_id": audit.identifier(), "operation": operation.name(),
            "device_reference": request.record["server_name"], "requested": operation.requested(),
            "mode": operation.requested()["mode"],
            "transport": "ddm", "control_transport": "controller",
            "request_attempted": false, "request_sent": false, "change_sent": false,
            "accepted": null, "request_acknowledged": null, "state": "refused",
            "execution_confirmed": null, "execution_report_fresh": false, "cleared": false,
            "state_verified": false, "verified": false, "effective_state_confirmed": null,
            "readback_fresh": false, "rebooting": false,
        });
        let mut status = self
            .execute_controller_clear(request, source, revision, operation, &mut value, &mut audit)
            .await;
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
            acknowledged_commands: usize::from(value["accepted"] == true)
                + usize::from(value["reboot"]["accepted"] == true),
            readback_completed: false,
            readback_errors: Vec::new(),
            observation: Some(value),
        })
    }

    async fn execute_controller_clear(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        operation: &Operation,
        value: &mut Value,
        audit: &mut Audit,
    ) -> u16 {
        let (fresh, mut session) = match self
            .prepare_controller_clear(request, source, revision, operation)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                value["error"] = json!(error.to_string());
                return match error {
                    ControlError::ObservationUnavailable(_) => 504,
                    ControlError::Unavailable(_) => 503,
                    _ => 409,
                };
            }
        };
        value["request_attempted"] = json!(true);
        value["state"] = json!("unconfirmed");
        let mut status = match session
            .command(json!({"command": operation.command_name()}))
            .await
        {
            Ok(exchange) => {
                value["request_sent"] = json!(exchange.request_sent);
                value["accepted"] = json!(true);
                let report = exchange
                    .packet
                    .as_deref()
                    .and_then(netaudio_core::responses::parse_clear_configuration_status);
                match report {
                    Some(report) => {
                        let confirmed = report.executed_action == Some(operation.command_name());
                        value["execution_confirmed"] = json!(confirmed);
                        value["execution_report_fresh"] = json!(true);
                        value["cleared"] = json!(confirmed);
                        value["executed_mode"] = json!(report.executed_mode);
                        value["execution_report"] = json!(report);
                        value["observation"] = json!({
                            "transport": "controller", "peer": exchange.peer.to_string(),
                            "observed_at": exchange.received_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                            "fresh": true,
                        });
                        if confirmed {
                            value["state"] = json!("executed");
                            value["takes_effect"] =
                                json!("at the next reboot on devices that defer clears");
                            200
                        } else {
                            value["state"] = json!("contradicted");
                            value["error"] = json!(
                                "Device report does not confirm the requested clear mode; no follow-up reboot was sent"
                            );
                            409
                        }
                    }
                    None => {
                        value["error"] = json!(
                            "Clear response has no valid execution report; no follow-up reboot was sent"
                        );
                        502
                    }
                }
            }
            Err(error) => {
                value["request_sent"] = json!(error.request_sent);
                value["error"] = json!(error.to_string());
                502
            }
        };
        value["change_sent"] = value["request_sent"].clone();
        value["request_acknowledged"] = value["accepted"].clone();
        if let Err(error) = audit.acknowledgement(&self.journal, value).await {
            value["audit_error"] = json!(error.to_string());
            status = 500;
        }
        let refreshed = async {
            if !self.refresh_server(&fresh.target.server_profile).await? {
                return Err(super::Error::Conflict(
                    "Current managed inventory is unavailable after clear",
                ));
            }
            let inventory = self.command_inventory(&fresh.target.server_profile, source)?;
            if !inventory.fresh {
                return Err(super::Error::Conflict(
                    "Current managed inventory is not complete and fresh after clear",
                ));
            }
            Ok(())
        }
        .await;
        match refreshed {
            Ok(()) => value["inventory_refreshed"] = json!(true),
            Err(error) => {
                value["inventory_refreshed"] = json!(false);
                value["inventory_error"] = json!(error.to_string());
                if status == 200 {
                    value["error"] = value["inventory_error"].clone();
                    status = 504;
                }
            }
        }
        if status == 200
            && matches!(
                operation,
                Operation::ClearConfiguration { reboot: true, .. }
            )
        {
            let mut reboot = json!({
                "operation_id": format!("{}:reboot", audit.identifier()),
                "request_attempted": false, "request_sent": false, "accepted": null,
                "state_verified": false,
            });
            let result = async {
                let current = self.status_record(&fresh)?;
                Action::Reboot.validate(&current)?;
                let inventory = self
                    .command_inventory(&fresh.target.server_profile, source)
                    .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
                let target = inventory
                    .device(&fresh.target.device.id)
                    .map_err(|error| ControlError::Conflict(error.to_string()))?;
                let target_identity =
                    super::device_session::target_identity(target, &inventory.devices)?;
                if identities(&fresh)? != target_identity {
                    return Err(ControlError::Conflict(
                        "Managed Controller identity changed after clear; no reboot was sent"
                            .into(),
                    ));
                }
                reboot["request_attempted"] = json!(true);
                match session
                    .exchange(ManagedOperation::Reboot {
                        device_id: session.device_id.clone(),
                        host_mac: session.host_mac,
                    })
                    .await
                {
                    Ok(exchange) => {
                        reboot["request_sent"] = json!(exchange.request_sent);
                        reboot["accepted"] = json!(true);
                        Ok(())
                    }
                    Err(error) => {
                        reboot["request_sent"] = json!(error.request_sent);
                        Err(ControlError::ObservationUnavailable(error.to_string()))
                    }
                }
            }
            .await;
            match result {
                Ok(()) => {
                    value["rebooting"] = json!(true);
                    value["reboot_verified"] = json!(false);
                    value["takes_effect"] = json!("now rebooting");
                }
                Err(error) => {
                    reboot["error"] = json!(error.to_string());
                    value["error"] = json!(format!(
                        "Clear execution was confirmed, but the follow-up reboot was not acknowledged: {error}"
                    ));
                    status = 502;
                }
            }
            value["reboot"] = reboot;
            if let Err(error) = audit.acknowledgement(&self.journal, value).await {
                value["audit_error"] = json!(error.to_string());
                status = 500;
            }
        }
        if status != 200 && value["execution_confirmed"] == true {
            value["state"] = json!("partial");
        }
        status
    }

    async fn prepare_controller_clear(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        operation: &Operation,
    ) -> Result<(FreshTarget, DeviceSession), ControlError> {
        let fresh = self.fresh_target(request, source, revision).await?;
        let mut session = match DeviceSession::open(source, &fresh).await {
            Ok(session) => session,
            Err(error) => {
                self.invalidate_controller_status(
                    source,
                    &fresh.target,
                    StatusQuery::Lock.kind(),
                    &error.to_string(),
                )?;
                return Err(error);
            }
        };
        self.controller_status(source, &fresh, &mut session, StatusQuery::Lock)
            .await?;
        let record = self.status_record(&fresh)?;
        validate(&record, operation)?;
        let mut current = request.operation.precondition(&record);
        current["managed_configuration"] = json!(revision);
        if request
            .precondition
            .as_ref()
            .is_some_and(|expected| *expected != current)
        {
            return Err(ControlError::Conflict(
                "Device changed after confirmation; request a new preview".into(),
            ));
        }
        self.invalidate_controller_device(
            source,
            &fresh.target,
            "Configuration clear requires fresh status observations",
        )?;
        Ok((fresh, session))
    }
}
