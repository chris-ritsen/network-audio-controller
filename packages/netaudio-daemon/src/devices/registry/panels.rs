use super::{Device, DeviceRegistry};
use crate::devices::audit::Audit;
use crate::devices::operations::{ControlCompletion, ControlError, ControlOperation};
use crate::devices::panels::{self, Operation};
use crate::devices::record::DeviceRecord;
use crate::devices::transport::{CommandIntent, DeviceTransport};
use crate::monitoring::requests::StatusQuery;
use anyhow::{Context, Result, ensure};
use netaudio_core::device_controls::{PanelRequest, PanelStatus, profile};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::time::Duration;

const QUERY_DEADLINE: Duration = Duration::from_secs(1);
const READBACK_DEADLINE: Duration = Duration::from_secs(2);

fn completion(observation: Value, errors: Vec<String>) -> ControlCompletion {
    ControlCompletion {
        acknowledged_commands: 0,
        readback_completed: observation.get("plan").is_none()
            || observation["plan"]["action"] == "unchanged"
            || observation["correlated_status"] == true,
        readback_errors: errors,
        observation: Some(observation),
    }
}

impl DeviceRegistry {
    fn panel_record(&self, device: &Device, generation: u64) -> Result<DeviceRecord, ControlError> {
        let state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        if state.generation != generation || !state.record.online || state.record.retired {
            return Err(ControlError::Conflict(
                "device identity or inventory changed during panel operation".into(),
            ));
        }
        Ok(state.record.clone())
    }

    fn panel_unavailable(&self, device: &Device, category: &str) -> Result<()> {
        let mut state = device
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("device lock poisoned"))?;
        panels::unavailable(&mut state.record, category);
        self.publish(&state.record)
    }

    pub(super) async fn query_panel(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        category: &str,
    ) -> Result<PanelStatus> {
        let record = self.panel_record(device, generation)?;
        let profile = profile(&panels::record_facts(&record));
        let query = profile
            .queries
            .iter()
            .find(|query| query.category == category)
            .context("no query is established for that device and setting")?;
        let status = self
            .exchange_panel(
                device,
                transport,
                generation,
                category,
                &query.request,
                None,
            )
            .await?;
        let mut result = status.clone();
        for request in status.followup_requests {
            result = self
                .exchange_panel(device, transport, generation, category, &request, None)
                .await?;
            ensure!(
                result.followup_requests.is_empty(),
                "panel follow-up did not provide a complete status"
            );
        }
        Ok(result)
    }

    async fn exchange_panel(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        category: &str,
        request: &PanelRequest,
        expected: Option<&Value>,
    ) -> Result<PanelStatus> {
        let before = self.panel_record(device, generation)?;
        let profile = profile(&panels::record_facts(&before));
        ensure!(
            profile.read_unavailable_reason.is_none(),
            "{}",
            profile.read_unavailable_reason.unwrap_or_default()
        );
        ensure!(
            panels::family_name(profile.family) == Some(request.family()),
            "panel family changed before query"
        );
        let mut observation = self.panel_requests.subscribe(&before.key)?;
        let command = json!({"command": "panel_control", "request": request, "requester": 0, "sequence": observation.sequence});
        transport.execute(&command, CommandIntent::Read).await?;
        let duration = if expected.is_some() {
            READBACK_DEADLINE
        } else {
            QUERY_DEADLINE
        };
        let result = tokio::select! {
            _ = self.cancellation.cancelled() => Err(anyhow::anyhow!("device registry is stopping")),
            result = observation.wait(category, duration, expected) => result,
        };
        let after = self.panel_record(device, generation)?;
        ensure!(
            before.properties.get("platform_versions_record")
                == after.properties.get("platform_versions_record"),
            "panel identity changed during query"
        );
        result
    }

    pub(super) async fn capture_panels(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
    ) -> Result<Value> {
        let record = self.panel_record(device, generation)?;
        let identity = record.properties.get("platform_versions_record").cloned();
        let profile = profile(&panels::record_facts(&record));
        ensure!(
            profile.unrecognized_panels.is_empty(),
            "device advertises unrecognized control panels"
        );
        if profile.family.is_none() {
            ensure!(
                panels::record_facts(&record).plugins.is_empty(),
                "device panel identity is ambiguous"
            );
            return Ok(Value::Null);
        }
        ensure!(
            profile.read_unavailable_reason.is_none(),
            "{}",
            profile.read_unavailable_reason.unwrap_or_default()
        );
        let mut fresh_values = Map::new();
        for query in profile.queries {
            let status = match self
                .query_panel(device, transport, generation, query.category)
                .await
            {
                Ok(status) => status,
                Err(error) => {
                    self.panel_unavailable(device, query.category)?;
                    return Err(error);
                }
            };
            for observation in status.observations {
                let observation = serde_json::to_value(observation)?;
                if observation["category"] == query.category {
                    fresh_values.insert(query.category.to_owned(), observation["value"].clone());
                }
            }
        }
        let settings = netaudio_core::configuration::resolve(
            netaudio_core::configuration::ConfigurationRequest::CapturePanel { fresh_values },
        )
        .map_err(anyhow::Error::msg)?;
        let record = self.panel_record(device, generation)?;
        ensure!(
            record.properties.get("platform_versions_record") == identity.as_ref(),
            "panel identity changed during preset capture"
        );
        let state = panels::snapshot(
            &record.properties,
            record.online,
            !record.address.is_unspecified(),
        )?;
        Ok(
            json!({"settings": settings, "identity": state["identity"], "observed_extensions": state}),
        )
    }

    pub(super) async fn control_panel(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        operation: &Operation,
        precondition: Option<&Value>,
        mut audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        if operation
            .change()
            .is_some_and(|change| change.category == "analog_level")
        {
            return self
                .control_analog_panel(
                    device,
                    transport,
                    generation,
                    operation,
                    precondition,
                    audit,
                )
                .await;
        }
        self.read_status(device, transport, generation, StatusQuery::Platform)
            .await?;
        let initial = self.panel_record(device, generation)?;
        let profile = profile(&panels::record_facts(&initial));
        if matches!(operation, Operation::Inspect) {
            let mut errors = Vec::new();
            if let Err(error) = self
                .read_status(device, transport, generation, StatusQuery::Lock)
                .await
            {
                self.accept_status(
                    device,
                    generation,
                    Map::from_iter([("is_locked".into(), Value::Null)]),
                )?;
                errors.push(error.to_string());
            }
            if profile.family.is_some() {
                if let Some(reason) = profile.read_unavailable_reason {
                    return Err(ControlError::Conflict(reason.to_owned()));
                }
                for query in profile.queries {
                    if let Err(error) = self
                        .query_panel(device, transport, generation, query.category)
                        .await
                    {
                        self.panel_unavailable(device, query.category)?;
                        errors.push(error.to_string());
                    }
                }
            }
            let record = self.panel_record(device, generation)?;
            if analog_access(&record, false)?.is_none()
                && let Err(error) = self
                    .read_status(device, transport, generation, StatusQuery::Codec)
                    .await
            {
                errors.push(error.to_string());
                self.accept_status(
                    device,
                    generation,
                    Map::from_iter([("codec_observed_at".into(), Value::Null)]),
                )?;
            }
            let record = self.panel_record(device, generation)?;
            let mut snapshot = panels::snapshot(
                &record.properties,
                record.online,
                !record.address.is_unspecified(),
            )?;
            let presented = record.presentation()?;
            snapshot["analog"] = json!({
                "status": presented["codec_status"], "levels": presented["gain_levels"],
                "direction": presented["gain_device_type"], "choices": presented["gain_level_choices"],
                "observed_at_unix": presented["codec_observed_at"],
                "read_unavailable_reason": analog_access(&record, false)?,
                "write_unavailable_reason": analog_access(&record, true)?,
            });
            return Ok(completion(snapshot, errors));
        }
        let change = operation.change().context("panel change is missing")?;
        if let Some(reason) = profile.read_unavailable_reason {
            return Err(ControlError::Conflict(reason.to_owned()));
        }
        let mut errors = Vec::new();
        let mut fresh_values = BTreeMap::new();
        if let Some(query) = profile
            .queries
            .iter()
            .find(|query| query.category == change.category)
        {
            for category in &query.prerequisites {
                match self
                    .query_panel(device, transport, generation, category)
                    .await
                {
                    Ok(status) => {
                        for observation in status.observations {
                            let observation =
                                serde_json::to_value(observation).map_err(anyhow::Error::from)?;
                            if observation["category"] == *category {
                                fresh_values
                                    .insert((*category).to_owned(), observation["value"].clone());
                            }
                        }
                    }
                    Err(error) => {
                        self.panel_unavailable(device, category)?;
                        errors.push(error.to_string());
                    }
                }
            }
        }
        if let Err(error) = self
            .read_status(device, transport, generation, StatusQuery::Lock)
            .await
        {
            self.accept_status(
                device,
                generation,
                Map::from_iter([("is_locked".into(), Value::Null)]),
            )?;
            errors.push(error.to_string());
        }
        let record = self.panel_record(device, generation)?;
        let presented = record.presentation()?;
        let mut condition = ControlOperation::Panel(operation.clone()).precondition(&presented);
        condition["panel"]["values"] = json!(fresh_values);
        let plan = netaudio_core::device_controls::planning::plan(
            netaudio_core::device_controls::planning::PanelPlanRequest {
                profile: panels::record_facts(&record),
                category: change.category.clone(),
                requested: change.requested.clone(),
                fresh_values,
                confirm_clear: change.confirm_clear,
            },
        );
        if matches!(operation, Operation::Plan(_)) {
            return Ok(completion(
                json!({"plan": plan, "precondition": condition}),
                errors,
            ));
        }
        let mut result = json!({
            "plan": plan, "request_attempted": false, "request_sent": false, "request_acknowledged": null,
            "transport_reply": false, "correlated_status": false, "effective_state_confirmed": matches!(plan.action, netaudio_core::device_controls::planning::PanelPlanAction::Unchanged),
            "persistence": "unknown", "media_readiness": "unknown", "sent_operations": [],
        });
        if !matches!(
            plan.action,
            netaudio_core::device_controls::planning::PanelPlanAction::Change
        ) {
            result["reason"] = json!(plan.reason);
            return Ok(completion(result, errors));
        }
        if precondition.is_some_and(|expected| condition != *expected) {
            return Err(ControlError::Conflict(
                "panel state changed after confirmation; obtain a new preview before retrying"
                    .into(),
            ));
        }
        let mut receipts = Vec::new();
        let mut sent_operations = Vec::new();
        for request in &plan.requests {
            let current = match self.panel_record(device, generation) {
                Ok(current) => current,
                Err(error) => {
                    result["reason"] = json!(error.to_string());
                    return Ok(completion(result, errors));
                }
            };
            let current_profile =
                netaudio_core::device_controls::profile(&panels::record_facts(&current));
            if current_profile.family != profile.family
                || current.properties.get("platform_versions_record")
                    != initial.properties.get("platform_versions_record")
                || current_profile.write_unavailable_reason.is_some()
            {
                result["reason"] = json!(
                    current_profile
                        .write_unavailable_reason
                        .unwrap_or("panel identity changed before command")
                );
                return Ok(completion(result, errors));
            }
            let receipt = self.panel_requests.subscribe(&current.key)?;
            let command = json!({"command": "panel_control", "request": request, "requester": 0, "sequence": receipt.sequence});
            result["request_attempted"] = json!(true);
            if let Err(error) = transport.send_command(&command).await {
                result["reason"] = json!(error.to_string());
                return Ok(completion(result, errors));
            }
            result["request_sent"] = json!(true);
            sent_operations.push(
                serde_json::to_value(request).map_err(anyhow::Error::from)?["operation"].clone(),
            );
            result["sent_operations"] = json!(sent_operations);
            receipts.push(receipt);
            if let Some(audit) = audit.as_deref_mut() {
                audit
                    .acknowledgement(&self.journal, &[], sent_operations.len())
                    .await
                    .context("record panel command transmission; do not replay")?;
            }
        }
        let query = profile
            .queries
            .iter()
            .find(|query| query.category == change.category)
            .context("panel readback query is unavailable")?;
        match self
            .exchange_panel(
                device,
                transport,
                generation,
                &change.category,
                &query.request,
                plan.expected.as_ref(),
            )
            .await
        {
            Ok(status) => {
                result["correlated_status"] = json!(true);
                for observation in status.observations {
                    let observation =
                        serde_json::to_value(observation).map_err(anyhow::Error::from)?;
                    if observation["category"] == change.category {
                        result["status"] = observation["value"].clone();
                        result["effective_state_confirmed"] =
                            json!(plan.expected.as_ref().is_some_and(|expected| {
                                netaudio_core::device_controls::planning::matches(
                                    &observation["value"],
                                    expected,
                                )
                            }));
                    }
                }
            }
            Err(error) => {
                self.panel_unavailable(device, &change.category)?;
                result["reason"] = json!(error.to_string());
                errors.push(error.to_string());
            }
        }
        result["transport_reply"] = json!(receipts.iter().any(|receipt| receipt.received()));
        Ok(completion(result, errors))
    }

    async fn control_analog_panel(
        &self,
        device: &Device,
        transport: &mut DeviceTransport,
        generation: u64,
        operation: &Operation,
        precondition: Option<&Value>,
        audit: Option<&mut Audit>,
    ) -> Result<ControlCompletion, ControlError> {
        let change = operation.change().context("analog change is missing")?;
        let configuration = crate::devices::analog::Configuration::new(
            &change.requested["channel"],
            &change.requested["level"],
            &change.requested["direction"],
        )?;
        let mut errors = Vec::new();
        let before = self.panel_record(device, generation)?;
        let reason = analog_access(&before, false)?;
        let mut plan = json!({"category": "analog_level", "requested": configuration.requested(), "action": "unavailable", "reason": reason});
        if reason.is_none() {
            match self
                .read_status(device, transport, generation, StatusQuery::Codec)
                .await
            {
                Ok(()) => {
                    let record = self.panel_record(device, generation)?;
                    let adapter = record
                        .properties
                        .get("gain_adapter")
                        .unwrap_or(&Value::Null);
                    let planned = serde_json::to_value(configuration.plan(adapter)?)
                        .map_err(anyhow::Error::from)?;
                    plan.as_object_mut().unwrap().extend(
                        planned
                            .as_object()
                            .context("analog plan is not an object")?
                            .clone(),
                    );
                    plan["status"] = adapter.clone();
                    if plan["action"] == "change" {
                        if let Err(error) = self
                            .read_status(device, transport, generation, StatusQuery::Lock)
                            .await
                        {
                            self.accept_status(
                                device,
                                generation,
                                Map::from_iter([("is_locked".into(), Value::Null)]),
                            )?;
                            errors.push(error.to_string());
                        }
                        let reason = analog_access(&self.panel_record(device, generation)?, true)?;
                        if reason.is_some() {
                            plan["action"] = json!("unavailable");
                            plan["reason"] = json!(reason);
                        }
                    }
                }
                Err(error) => {
                    plan["reason"] = json!("Fresh analog status is unavailable.");
                    errors.push(error.to_string());
                }
            }
        }
        if matches!(operation, Operation::Plan(_)) {
            let record = self.panel_record(device, generation)?.presentation()?;
            let mut condition = ControlOperation::Panel(operation.clone()).precondition(&record);
            condition["panel"]["analog"] = plan["status"].clone();
            return Ok(completion(
                json!({"plan": plan, "precondition": condition}),
                errors,
            ));
        }
        let mut result = json!({
            "plan": plan, "reason": plan["reason"], "request_sent": false, "request_acknowledged": null,
            "transport_reply": false, "correlated_status": false, "effective_state_confirmed": plan["action"] == "unchanged",
            "persistence": "unknown", "media_readiness": "unknown", "status": plan["status"],
        });
        if plan["action"] != "change" {
            return Ok(completion(result, errors));
        }
        let record = self.panel_record(device, generation)?.presentation()?;
        if precondition.is_some_and(|expected| {
            ControlOperation::Panel(operation.clone()).precondition(&record) != *expected
        }) {
            return Err(ControlError::Conflict(
                "analog state changed after confirmation; obtain a new preview before retrying"
                    .into(),
            ));
        }
        let configuration =
            crate::devices::status_configuration::Configuration::Analog(configuration);
        let precondition =
            ControlOperation::StatusConfiguration(configuration.clone()).precondition(&record);
        let mut completed = self
            .configure_status(
                device,
                transport,
                generation,
                &configuration,
                Some(&precondition),
                audit,
            )
            .await?;
        if let Some(observation) = completed.observation {
            result.as_object_mut().unwrap().extend(
                observation
                    .as_object()
                    .context("analog result is not an object")?
                    .clone(),
            );
        }
        completed.observation = Some(result);
        completed.readback_errors.extend(errors);
        Ok(completed)
    }
}

fn analog_access(record: &DeviceRecord, write: bool) -> Result<Option<&'static str>> {
    let properties = &record.properties;
    let access: netaudio_core::responses::AnalogAccess = serde_json::from_value(json!({
        "managed": properties.get("requires_managed_control") == Some(&json!(true)),
        "managed_context_available": false,
        "address_available": !record.address.is_unspecified(),
        "online": record.online,
        "supported": properties.get("generic_codec_control_supported"),
        "locked": properties.get("is_locked"),
        "write": write,
    }))?;
    Ok(access.denial())
}
