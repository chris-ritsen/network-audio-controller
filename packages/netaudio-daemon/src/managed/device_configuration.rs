use super::{
    audit::Audit,
    control::{DeviceRequest, FreshTarget},
    device_session::DeviceSession,
    registry::{Registry, Source},
};
use crate::{
    devices::operations::{ControlCompletion, ControlError},
    monitoring::requests::StatusQuery,
};
use serde_json::{Value, json};

mod channel;
mod latency;
mod request;
pub(super) use request::Configuration;
use std::sync::Arc;

struct Prepared {
    fresh: FreshTarget,
    session: DeviceSession,
    command: Option<Value>,
    before: Arc<Value>,
}

impl Registry {
    pub(super) async fn controller_configure(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        requested: &Configuration<'_>,
    ) -> Result<ControlCompletion, ControlError> {
        let mut audit = Audit::begin_device(
            &self.journal,
            request.record.clone(),
            requested.name(),
            json!({"device_id": request.record["ddm_device_id"], "domain_id": request.record["ddm_domain_id"], "configuration": requested.requested()}),
        )
        .await
        .map_err(|error| ControlError::Unavailable(error.to_string()))?;
        let mut value = json!({
            "operation_id": audit.identifier(), "operation": requested.name(),
            "device_reference": request.record["server_name"], "requested": requested.requested(),
            "transport": "ddm", "control_transport": "controller",
            "request_attempted": false, "request_sent": false, "accepted": null,
            "request_acknowledged": null, "state_verified": false,
            "configuration_confirmed": null, "effective_state_confirmed": null,
            "readback_fresh": false, "state": "refused",
        });
        let mut prepared = match self
            .prepare_controller_configuration(request, source, revision, requested)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                value["error"] = json!(error.to_string());
                let mut status = match error {
                    ControlError::ObservationUnavailable(_) => 504,
                    ControlError::Unavailable(_) => 503,
                    ControlError::Invalid(_) => 400,
                    ControlError::NotFound(_) => 404,
                    _ => 409,
                };
                if let Err(error) = audit.finish(&self.journal, &value).await {
                    value["audit_error"] = json!(error.to_string());
                    status = 500;
                }
                return Err(ControlError::Managed {
                    status,
                    result: value,
                });
            }
        };
        let mut transport_failed = false;
        let mut audit_failed = false;
        let after = if let Some(command) = prepared.command.take() {
            value["request_attempted"] = json!(true);
            match prepared.session.command(command).await {
                Ok(exchange) => {
                    value["request_sent"] = json!(exchange.request_sent);
                    if let Err(error) = requested.acknowledge(&exchange, &mut value) {
                        value["error"] = json!(error.to_string());
                        transport_failed = true;
                    }
                }
                Err(error) => {
                    value["request_sent"] = json!(error.request_sent);
                    value["error"] = json!(error.to_string());
                    transport_failed = true;
                }
            }
            value["request_acknowledged"] = value["accepted"].clone();
            if let Err(error) = audit.acknowledgement(&self.journal, &value).await {
                value["audit_error"] = json!(error.to_string());
                audit_failed = true;
            }
            let readback = async {
                if requested.inventory_readback() {
                    self.refresh_configuration_inventory(source, &prepared.fresh)
                        .await?;
                } else if transport_failed {
                    prepared.session = DeviceSession::open(source, &prepared.fresh).await?;
                }
                for query in requested.readback_queries(&prepared.before) {
                    self.controller_status(source, &prepared.fresh, &mut prepared.session, query)
                        .await?;
                }
                let record = self.status_record(&prepared.fresh)?;
                requested.validate_readback(&prepared.before, &record)?;
                requested.observe(&record, &mut value)?;
                Ok::<_, ControlError>(record)
            }
            .await;
            match readback {
                Ok(record) => Some(record),
                Err(error) => {
                    value["readback_error"] = json!(error.to_string());
                    for query in requested.readback_queries(&prepared.before) {
                        if let Err(error) = self.invalidate_controller_status(
                            source,
                            &prepared.fresh.target,
                            query.kind(),
                            &error.to_string(),
                        ) {
                            value["publication_error"] = json!(error.to_string());
                        }
                    }
                    None
                }
            }
        } else {
            requested.observe(&prepared.before, &mut value)?;
            Some(prepared.before.clone())
        };
        let completion = requested.completion(
            &prepared.before,
            after.as_deref(),
            value["request_sent"] == true,
            &value,
        )?;
        for (field, observed) in completion
            .as_object()
            .expect("configuration result is an object")
        {
            if !matches!(field.as_str(), "request_sent" | "request_acknowledged") {
                value[field] = observed.clone();
            }
        }
        let configured = value["configuration_confirmed"].as_bool();
        let effective = value["effective_state_confirmed"] == true;
        let confirmed = requested.confirmation(&value);
        value["readback_fresh"] = json!(after.is_some());
        value["state_verified"] = json!(confirmed == Some(true));
        value["state"] = json!(match (configured, effective) {
            (Some(true), true) => "confirmed",
            (Some(true), false) => "configured",
            (Some(false), _) => "contradicted",
            (None, _) => "unconfirmed",
        });
        if value["accepted"] == false {
            value["state"] = json!("rejected");
            value["state_verified"] = json!(false);
        }
        if let Some(record) = after
            && let Some(observation) = requested.observation(&record, &prepared.before)
        {
            value["observation"] = observation;
        }
        let mut status = if transport_failed {
            502
        } else if value["accepted"] == false {
            409
        } else {
            match confirmed {
                Some(true) => 200,
                Some(false) => 409,
                None => 504,
            }
        };
        if status != 200 && value.get("error").is_none() {
            value["error"] = json!(match confirmed {
                Some(false) => "Fresh readback does not confirm the requested operation result",
                _ => "Fresh configuration readback is unavailable; do not replay the command",
            });
        }
        if let Err(error) = audit.finish(&self.journal, &value).await {
            value["audit_error"] = json!(error.to_string());
            audit_failed = true;
        }
        if audit_failed {
            status = 500;
        }
        if status != 200 {
            return Err(ControlError::Managed {
                status,
                result: value,
            });
        }
        Ok(ControlCompletion {
            acknowledged_commands: usize::from(value["accepted"] == true),
            readback_completed: value["readback_fresh"] == true,
            readback_errors: Vec::new(),
            observation: Some(value),
        })
    }

    async fn refresh_configuration_inventory(
        &self,
        source: &Arc<Source>,
        fresh: &FreshTarget,
    ) -> Result<(), ControlError> {
        let server = &fresh.target.server_profile;
        if !self
            .refresh_server(server)
            .await
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?
        {
            return Err(ControlError::ObservationUnavailable(
                "Managed configuration readback is unavailable".into(),
            ));
        }
        let inventory = self
            .command_inventory(server, source)
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        if !inventory.fresh {
            return Err(ControlError::ObservationUnavailable(
                "Managed configuration readback is not complete and fresh".into(),
            ));
        }
        let target = inventory
            .device(&fresh.target.device.id)
            .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
        if super::device_session::identities(fresh)?
            != super::device_session::target_identity(target, &inventory.devices)?
        {
            return Err(ControlError::ObservationUnavailable(
                "Managed device identity changed during configuration readback".into(),
            ));
        }
        Ok(())
    }

    async fn prepare_controller_configuration(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        requested: &Configuration<'_>,
    ) -> Result<Prepared, ControlError> {
        let fresh = self.fresh_target(request, source, revision).await?;
        let mut session = match DeviceSession::open(source, &fresh).await {
            Ok(session) => session,
            Err(error) => {
                for query in requested.affected_queries() {
                    self.invalidate_controller_status(
                        source,
                        &fresh.target,
                        query.kind(),
                        &error.to_string(),
                    )?;
                }
                return Err(error);
            }
        };
        for query in requested.preflight_queries() {
            self.controller_status(source, &fresh, &mut session, query)
                .await?;
        }
        if requested.needs_switch_status(self.status_record(&fresh)?.as_ref())? {
            self.controller_status(
                source,
                &fresh,
                &mut session,
                StatusQuery::SwitchConfiguration,
            )
            .await?;
        }
        let before = self.status_record(&fresh)?;
        let mut current = request.operation.precondition(&before);
        current["managed_configuration"] = json!(revision);
        if request
            .precondition
            .as_ref()
            .is_some_and(|expected| *expected != current)
        {
            return Err(ControlError::Conflict(
                "Device settings changed after confirmation; request a new preview".into(),
            ));
        }
        let command = requested.command(&before)?;
        requested.prepare(&mut session).await?;
        if requested.inventory_readback() {
            let current = self.status_record(&fresh)?;
            if request.operation.precondition(&before) != request.operation.precondition(&current) {
                return Err(ControlError::Conflict(
                    "Device changed during channel frontend inspection; no write was sent".into(),
                ));
            }
            requested.command(&current)?;
        }
        if command.is_some() {
            for query in requested.readback_queries(&before) {
                self.invalidate_controller_status(
                    source,
                    &fresh.target,
                    query.kind(),
                    "Configuration command is awaiting readback",
                )?;
            }
        }
        Ok(Prepared {
            fresh,
            session,
            command,
            before,
        })
    }
}
