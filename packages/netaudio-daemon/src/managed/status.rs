use super::{
    control::{DeviceRequest, FreshTarget},
    device_session::DeviceSession,
    inventory::freshness,
    registry::{Registry, Source},
};
use crate::{
    devices::{
        capabilities,
        operations::{ControlCompletion, ControlError},
        registry::Inspection,
    },
    monitoring::requests::StatusQuery,
};
use serde_json::{Value, json};
use std::sync::Arc;

pub(crate) mod observations;
use observations::Received;
mod query;
pub(super) use query::Query;

impl Registry {
    pub(super) async fn controller_inspect(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
        inspection: Inspection,
    ) -> Result<ControlCompletion, ControlError> {
        let fresh = self.fresh_target(request, source, revision).await?;
        let mut session = match DeviceSession::open(source, &fresh).await {
            Ok(session) => session,
            Err(error) => {
                let queries: Vec<Query> = match inspection {
                    Inspection::MulticastPrefix => vec![
                        StatusQuery::Lock.into(),
                        StatusQuery::Aes67.into(),
                        Query::MulticastPrefix,
                    ],
                    Inspection::Latency => vec![StatusQuery::Lock.into(), Query::Latency],
                    Inspection::Lock => vec![StatusQuery::Lock.into()],
                    Inspection::Status(query) => vec![StatusQuery::Lock.into(), query.into()],
                    Inspection::Network | Inspection::Redundancy => {
                        vec![
                            StatusQuery::Interface.into(),
                            StatusQuery::SwitchConfiguration.into(),
                        ]
                    }
                    Inspection::NetworkConfiguration => vec![
                        StatusQuery::Lock.into(),
                        StatusQuery::Platform.into(),
                        StatusQuery::Interface.into(),
                        StatusQuery::SwitchConfiguration.into(),
                    ],
                };
                for query in queries {
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
        let mut readback_errors = Vec::new();
        match inspection {
            Inspection::MulticastPrefix => {
                for query in [
                    StatusQuery::Lock.into(),
                    StatusQuery::Aes67.into(),
                    Query::MulticastPrefix,
                ] {
                    self.controller_status(source, &fresh, &mut session, query)
                        .await?;
                }
            }
            Inspection::Latency => {
                self.controller_status(source, &fresh, &mut session, StatusQuery::Lock)
                    .await?;
                self.controller_status(source, &fresh, &mut session, Query::Latency)
                    .await?;
            }
            Inspection::Lock => {
                self.controller_status(source, &fresh, &mut session, StatusQuery::Lock)
                    .await?;
            }
            Inspection::Status(query) => {
                self.controller_status(source, &fresh, &mut session, StatusQuery::Lock)
                    .await?;
                if !matches!(query, StatusQuery::Lock) {
                    self.controller_status(source, &fresh, &mut session, query)
                        .await?;
                }
            }
            Inspection::Network | Inspection::NetworkConfiguration | Inspection::Redundancy => {
                if matches!(inspection, Inspection::NetworkConfiguration) {
                    self.controller_status(source, &fresh, &mut session, StatusQuery::Lock)
                        .await?;
                    self.controller_status(source, &fresh, &mut session, StatusQuery::Platform)
                        .await?;
                }
                self.controller_status(source, &fresh, &mut session, StatusQuery::Interface)
                    .await?;
                let record = self.status_record(&fresh)?;
                if capabilities::operation(&record, "redundancy")?.supported != Some(false) {
                    match self
                        .controller_status(
                            source,
                            &fresh,
                            &mut session,
                            StatusQuery::SwitchConfiguration,
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(ControlError::ObservationUnavailable(error))
                            if !matches!(inspection, Inspection::NetworkConfiguration) =>
                        {
                            readback_errors.push(error)
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        let record = self.status_record(&fresh)?;
        let observation = match inspection {
            Inspection::Status(_) | Inspection::Latency => record.as_ref().clone(),
            Inspection::MulticastPrefix => json!({"prefix": record["aes67_multicast_prefix"]}),
            Inspection::Lock => json!({
                "device": record["server_name"],
                "is_locked": record["is_locked"],
                "lock_state_code": record["lock_reset_status"]["lock_state_code"],
                "status_code": record["lock_reset_status"]["status_code"],
                "observed_at": record["lock_status_observed_at"],
                "observation_source": "observed_after_0x1008",
                "operation_availability": record["operation_availability"]["locking"],
                "transport": "ddm", "control_transport": "controller",
            }),
            Inspection::Network | Inspection::NetworkConfiguration => {
                capabilities::network(&record)?
            }
            Inspection::Redundancy => json!({
                "device": record["server_name"], "redundancy": capabilities::redundancy(&record)?,
            }),
        };
        Ok(ControlCompletion {
            acknowledged_commands: 0,
            readback_completed: readback_errors.is_empty(),
            readback_errors,
            observation: Some(observation),
        })
    }

    pub(super) async fn controller_status(
        &self,
        source: &Arc<Source>,
        fresh: &FreshTarget,
        session: &mut DeviceSession,
        query: impl Into<Query>,
    ) -> Result<(), ControlError> {
        let query = query.into();
        let result = async {
            let command = query.command(&session.device_id)?;
            let exchange = session
                .command(command)
                .await
                .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
            let received =
                Received::decode(exchange, query.kind(), freshness(&source.configuration))
                    .map_err(|error| ControlError::ObservationUnavailable(error.to_string()))?;
            self.accept_controller_status(source, &fresh.target, received)
        }
        .await;
        if let Err(error) = &result {
            self.invalidate_controller_status(
                source,
                &fresh.target,
                query.kind(),
                &error.to_string(),
            )?;
        }
        result
    }

    pub(super) fn status_record(&self, fresh: &FreshTarget) -> Result<Arc<Value>, ControlError> {
        let record = self
            .model
            .device(
                fresh.record["server_name"].as_str().ok_or_else(|| {
                    ControlError::Conflict("Managed device has no identity".into())
                })?,
            )?
            .ok_or_else(|| {
                ControlError::Conflict("Managed device disappeared during status inspection".into())
            })?;
        if ["ddm_server_profile", "ddm_domain_id", "ddm_device_id"]
            .iter()
            .any(|field| record[field] != fresh.record[field])
            || record["ddm_fresh"] != true
            || record["ddm_connection_state"] != "READY"
        {
            return Err(ControlError::Conflict(
                "Managed device ownership changed during status inspection".into(),
            ));
        }
        Ok(record)
    }
}
