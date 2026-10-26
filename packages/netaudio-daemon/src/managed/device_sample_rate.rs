use super::{
    audit::Audit,
    control::{DeviceRequest, FreshTarget},
    device_session::DeviceSession,
    registry::{Registry, Source},
};
use crate::{
    devices::{
        operations::{ControlCompletion, ControlError},
        sample_rate::{self, Operation},
    },
    monitoring::{requests::StatusQuery, status::StatusKind},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

struct Prepared {
    session: DeviceSession,
    record: Arc<Value>,
    status: Value,
    preflight: Value,
}

impl Registry {
    pub(super) async fn controller_sample_rate(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        operation: &Operation,
    ) -> Result<ControlCompletion, ControlError> {
        let mut audit = Audit::begin_device(
            &self.journal,
            request.record.clone(),
            "set_sample_rate",
            json!({"sample_rate": operation.target, "confirm_destructive": operation.confirm_destructive, "preview": operation.preview}),
        ).await.map_err(|error| ControlError::Unavailable(error.to_string()))?;
        let mut value = json!({
            "operation_id": audit.identifier(), "operation": "set_sample_rate",
            "device_reference": request.record["server_name"],
            "requested": {"sample_rate": operation.target},
            "transport": "ddm", "control_transport": "controller",
            "request_attempted": false, "request_sent": false, "change_sent": false,
            "accepted": null, "request_acknowledged": null,
            "state": "refused", "state_verified": false, "success": false,
            "configuration_confirmed": null, "effective_state_confirmed": null,
            "topology_verified": false, "topology_preservation_confirmed": null,
            "verification_scope": null, "readback_fresh": false, "changed": false,
        });
        let mut status = match self
            .run_sample_rate(request, source, revision, operation, &mut audit, &mut value)
            .await
        {
            Ok(status) => status,
            Err(error) => {
                value["error"] = json!(error.to_string());
                error_status(&error)
            }
        };
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
            acknowledged_commands: usize::from(value["accepted"] == true),
            readback_completed: value["readback_fresh"] == true,
            readback_errors: Vec::new(),
            observation: Some(value),
        })
    }

    async fn run_sample_rate(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        operation: &Operation,
        audit: &mut Audit,
        value: &mut Value,
    ) -> Result<u16, ControlError> {
        let fresh = self.fresh_target(request, source, revision).await?;
        let prepared = tokio::time::timeout(
            Duration::from_secs(30),
            self.prepare_sample_rate(source, &fresh, operation),
        )
        .await
        .unwrap_or_else(|_| {
            Err(ControlError::ObservationUnavailable(
                "Managed sample-rate preflight deadline expired; no change was sent".into(),
            ))
        });
        let mut prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.invalidate_sample_rate(source, &fresh, &error.to_string())?;
                return Err(error);
            }
        };
        value["preflight"] = prepared.preflight.clone();
        let mut current = request.operation.precondition(&prepared.record);
        current["managed_configuration"] = json!(revision);
        if request
            .precondition
            .as_ref()
            .is_some_and(|expected| *expected != current)
        {
            return Err(ControlError::Conflict(
                "Sample-rate or topology facts changed after confirmation; request a new preview"
                    .into(),
            ));
        }
        if operation.preview {
            value["state"] = json!("preview");
            return Ok(200);
        }
        if prepared.status["current_value"] == operation.target {
            merge(
                value,
                sample_rate::success(&prepared.preflight, &prepared.status, None, false),
            );
            value["verification_scope"] = json!("current_sample_rate");
            value["configuration_confirmed"] = json!(true);
            value["effective_state_confirmed"] = json!(true);
            value["readback_fresh"] = json!(true);
            value["observation"] = prepared.record["managed_status"]["sample_rate"].clone();
            return Ok(200);
        }
        if let Some((state, reason)) = operation.refusal(&prepared.preflight) {
            value["state"] = json!(state);
            value["error"] = json!(reason);
            return Ok(409);
        }
        operation.writable(self.status_record(&fresh)?.as_ref())?;
        self.invalidate_controller_status(
            source,
            &fresh.target,
            StatusKind::Topology,
            "Sample-rate command may change the observed topology",
        )?;
        self.invalidate_controller_status(
            source,
            &fresh.target,
            StatusKind::SampleRate,
            "Sample-rate command requires a new status observation",
        )?;
        value["request_attempted"] = json!(true);
        let transport_failed = match prepared
            .session
            .command(json!({"command": "set_sample_rate", "sample_rate": operation.target}))
            .await
        {
            Ok(exchange) => {
                value["request_sent"] = json!(exchange.request_sent);
                value["accepted"] = json!(true);
                false
            }
            Err(error) => {
                value["request_sent"] = json!(error.request_sent);
                value["error"] = json!(error.to_string());
                true
            }
        };
        value["change_sent"] = value["request_sent"].clone();
        value["request_acknowledged"] = value["accepted"].clone();
        value["state"] = json!("unverified");
        let audit_failed = if let Err(error) = audit.acknowledgement(&self.journal, value).await {
            value["audit_error"] = json!(error.to_string());
            true
        } else {
            false
        };
        let observed = tokio::time::timeout(Duration::from_secs(30), async {
            if transport_failed {
                prepared.session = DeviceSession::open(source, &fresh).await?;
            }
            self.controller_status(
                source,
                &fresh,
                &mut prepared.session,
                StatusQuery::SampleRate,
            )
            .await?;
            let record = self.status_record(&fresh)?;
            let status = sample_rate::status(&record)?;
            value["observed_sample_rate_hertz"] = status["current_value"].clone();
            value["configuration_confirmed"] = json!(status["current_value"] == operation.target);
            value["observation"] = record["managed_status"]["sample_rate"].clone();
            let rate = status["current_value"]
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| {
                    ControlError::ObservationUnavailable("Observed sample rate is invalid".into())
                })?;
            let topology = self
                .controller_topology(source, &fresh, &mut prepared.session, rate)
                .await?;
            value["resulting_topology"] = topology.clone();
            value["readback_fresh"] = json!(true);
            value["topology_observation"] =
                self.status_record(&fresh)?["managed_status"]["topology"].clone();
            value["verification_scope"] = json!(if prepared.preflight["capacity_known"] == true {
                "expected_topology"
            } else {
                "observed_topology_consistency"
            });
            if let Err(error) = operation.verify(&prepared.preflight, &topology) {
                if prepared.preflight["capacity_known"] == true {
                    value["topology_preservation_confirmed"] = json!(false);
                }
                return Err(error);
            }
            if prepared.preflight["capacity_known"] == true {
                value["topology_preservation_confirmed"] = json!(true);
            }
            merge(
                value,
                sample_rate::success(&prepared.preflight, &status, Some(&topology), true),
            );
            value["effective_state_confirmed"] = json!(true);
            value["topology_verified"] = json!(true);
            Ok::<_, ControlError>(())
        })
        .await
        .unwrap_or_else(|_| {
            Err(ControlError::ObservationUnavailable(
                "Managed sample-rate readback deadline expired".into(),
            ))
        });
        let readback_status = match observed {
            Ok(()) => 200,
            Err(error) => {
                value["readback_error"] = json!(error.to_string());
                value["state"] = json!(if value["readback_fresh"] == true {
                    "contradicted"
                } else {
                    "unverified"
                });
                if value["readback_fresh"] == true {
                    value["effective_state_confirmed"] = json!(false);
                } else {
                    self.invalidate_controller_status(
                        source,
                        &fresh.target,
                        StatusKind::Topology,
                        &error.to_string(),
                    )?;
                }
                error_status(&error)
            }
        };
        if audit_failed {
            return Ok(500);
        }
        if transport_failed {
            return Ok(502);
        }
        Ok(readback_status)
    }

    async fn prepare_sample_rate(
        &self,
        source: &Arc<Source>,
        fresh: &FreshTarget,
        operation: &Operation,
    ) -> Result<Prepared, ControlError> {
        let mut session = DeviceSession::open(source, fresh).await?;
        self.controller_status(source, fresh, &mut session, StatusQuery::SampleRate)
            .await?;
        let record = self.status_record(fresh)?;
        let status = operation.validate_status(&record)?;
        let topology = if status["current_value"] == operation.target {
            None
        } else {
            self.controller_status(source, fresh, &mut session, StatusQuery::Lock)
                .await?;
            operation.writable(self.status_record(fresh)?.as_ref())?;
            let rate = status["current_value"]
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| {
                    ControlError::ObservationUnavailable("Observed sample rate is invalid".into())
                })?;
            let topology = self
                .controller_topology(source, fresh, &mut session, rate)
                .await?;
            self.controller_status(source, fresh, &mut session, StatusQuery::Lock)
                .await?;
            Some(topology)
        };
        let record = self.status_record(fresh)?;
        if topology.is_some() {
            operation.writable(&record)?;
        }
        let preflight = operation.preflight(&record, topology)?;
        Ok(Prepared {
            session,
            record,
            status,
            preflight,
        })
    }

    fn invalidate_sample_rate(
        &self,
        source: &Arc<Source>,
        fresh: &FreshTarget,
        error: &str,
    ) -> Result<(), ControlError> {
        for kind in [StatusKind::SampleRate, StatusKind::Topology] {
            self.invalidate_controller_status(source, &fresh.target, kind, error)?;
        }
        Ok(())
    }
}

fn merge(target: &mut Value, fields: Value) {
    target
        .as_object_mut()
        .expect("sample-rate receipt is an object")
        .extend(
            fields
                .as_object()
                .expect("sample-rate result is an object")
                .clone(),
        );
}

fn error_status(error: &ControlError) -> u16 {
    match error {
        ControlError::Invalid(_) => 400,
        ControlError::ObservationUnavailable(_) => 504,
        ControlError::Unavailable(_) => 503,
        ControlError::Internal(_) => 500,
        _ => 409,
    }
}
