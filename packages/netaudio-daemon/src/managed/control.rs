use super::{
    Error,
    audit::Audit,
    registry::{Registry, Request, Source},
};
use crate::devices::operations::{
    ControlCompletion, ControlError, ControlOperation, SubscriptionChange,
};
use crate::devices::panels::Operation;
use netaudio_core::managed_api::control::{Control, Subscription};
use netaudio_core::managed_api::inventory::Observation;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const READBACK_INTERVAL: Duration = Duration::from_millis(500);
const READBACK_LIMIT: Duration = Duration::from_secs(10);

pub struct DeviceRequest {
    pub caller: super::sessions::Caller,
    pub record: Value,
    pub operation: ControlOperation,
    pub precondition: Option<Value>,
    pub configuration_revision: String,
}

pub(super) struct FreshTarget {
    pub inventory: super::mutations::Inventory,
    pub target: Observation,
    pub record: Arc<Value>,
}

pub fn required(record: &Value) -> bool {
    record["requires_managed_control"] == true
        || record["management_state"]
            .as_str()
            .is_some_and(|state| state.eq_ignore_ascii_case("managed"))
        || record["ddm_enrolment_state"].as_str().is_some_and(|state| {
            ["enrolled", "managed"]
                .iter()
                .any(|expected| state.eq_ignore_ascii_case(expected))
        })
}

pub fn outcome(result: &Result<ControlCompletion, ControlError>) -> Option<&Value> {
    match result {
        Ok(completion) => completion
            .observation
            .as_ref()
            .filter(|value| value["transport"] == "ddm"),
        Err(ControlError::Managed { result, .. }) => Some(result),
        _ => None,
    }
}

pub fn evidence(outcome: &Value) -> Value {
    Value::Object(
        [
            "operation_id",
            "transport",
            "control_transport",
            "request_attempted",
            "request_sent",
            "accepted",
            "state_verified",
            "readback_fresh",
            "state",
            "error",
            "readback_error",
            "audit_error",
        ]
        .into_iter()
        .filter_map(|field| {
            outcome
                .get(field)
                .map(|value| (field.to_owned(), value.clone()))
        })
        .collect(),
    )
}

fn settles_routes(operation: &ControlOperation) -> bool {
    match operation {
        ControlOperation::SampleRate(operation) => !operation.preview,
        ControlOperation::AudioConfiguration(_)
        | ControlOperation::FlowAuthoring(_)
        | ControlOperation::Maintenance(_)
        | ControlOperation::RenameChannel { .. }
        | ControlOperation::RenameDevice { .. }
        | ControlOperation::SetLatency { .. }
        | ControlOperation::StatusConfiguration(_)
        | ControlOperation::Subscriptions { .. } => true,
        _ => false,
    }
}

pub fn validate(record: &Value, operation: &ControlOperation) -> Result<(), ControlError> {
    if matches!(
        operation,
        ControlOperation::InspectStatus(_)
            | ControlOperation::RefreshClock { .. }
            | ControlOperation::InspectTransmitFlows
            | ControlOperation::RefreshAudio(_)
            | ControlOperation::InspectMulticastPrefix
    ) {
        return Ok(());
    }
    if let ControlOperation::Panel(operation) = operation {
        return match super::panels::configuration(operation)? {
            Some(configuration) if matches!(operation, Operation::Apply(_)) => {
                validate(record, &super::panels::status_operation(configuration))
            }
            _ => Ok(()),
        };
    }
    if let ControlOperation::SampleRate(operation) = operation {
        operation.validate_status(record)?;
        if record["sample_rate_hz"] != operation.target {
            operation.writable(record)?;
        }
        return Ok(());
    }
    if let Some(action) = super::device_control::Action::from_operation(operation) {
        return action.validate(record);
    }
    if let Some(operation) = super::device_maintenance::operation(operation) {
        return super::device_maintenance::validate(record, operation);
    }
    if let Some(configuration) =
        super::device_configuration::Configuration::from_operation(operation)
    {
        return configuration.command(record).map(|_| ());
    }
    plan(record, operation).map(|_| ())
}

pub fn plan(record: &Value, operation: &ControlOperation) -> Result<Control, ControlError> {
    let mut command = encode(operation)?;
    if let Control::Subscriptions { subscriptions } = &mut command {
        for subscription in subscriptions {
            let number = subscription.receiver_channel;
            let channel = record["channels"]["receivers"]
                .get(number.to_string())
                .ok_or_else(|| ControlError::NotFound(format!("rx channel {number} not found")))?;
            let same = subscription.transmitter_device == "."
                || ["name", "server_name"].iter().any(|field| {
                    record[field].as_str().is_some_and(|name| {
                        name.eq_ignore_ascii_case(&subscription.transmitter_device)
                    })
                });
            if same {
                if channel["managed_can_subscribe_self"] == false
                    && channel["managed_can_subscribe_self_fresh"] == true
                {
                    return Err(ControlError::Conflict(format!(
                        "receiver channel {number} does not support self-connections"
                    )));
                }
                subscription.transmitter_device = text(record, "name")?.into();
            }
        }
    }
    Ok(command)
}

pub fn command_count(operation: &ControlOperation) -> Result<usize, ControlError> {
    if matches!(
        operation,
        ControlOperation::RefreshClock { .. }
            | ControlOperation::InspectTransmitFlows
            | ControlOperation::InspectStatus(_)
            | ControlOperation::InspectMulticastPrefix
            | ControlOperation::RefreshAudio(_)
    ) {
        return Ok(0);
    }
    if let ControlOperation::Panel(operation) = operation {
        super::panels::configuration(operation)?;
        return Ok(usize::from(matches!(operation, Operation::Apply(_))));
    }
    if let ControlOperation::SampleRate(operation) = operation {
        return Ok(usize::from(!operation.preview));
    }
    if super::device_control::Action::from_operation(operation).is_some() {
        return Ok(1);
    }
    if let Some(operation) = super::device_maintenance::operation(operation) {
        return Ok(1 + usize::from(operation.requested()["reboot"] == true));
    }
    if super::device_configuration::Configuration::from_operation(operation).is_some() {
        return Ok(1);
    }
    encode(operation).map(|_| 1)
}

fn encode(operation: &ControlOperation) -> Result<Control, ControlError> {
    match operation {
        ControlOperation::RenameDevice { name } => {
            if !name.trim().is_empty() {
                netaudio_core::protocol::validate_dante_name(name)
                    .map_err(|error| ControlError::Invalid(error.to_string()))?;
            }
            Ok(Control::Name {
                name: if name.trim().is_empty() {
                    String::new()
                } else {
                    name.clone()
                },
            })
        }
        ControlOperation::Clock(clock) if clock.changes.len() == 1 => {
            let enabled = clock.changes["preferred_leader"].as_bool().ok_or_else(|| {
                ControlError::Conflict(
                    "Managed clock control currently supports preferred leader only".into(),
                )
            })?;
            Ok(Control::PreferredLeader { enabled })
        }
        ControlOperation::Subscriptions { changes } => {
            if changes.is_empty() {
                return Err(ControlError::Invalid(
                    "Select at least one receiver channel".into(),
                ));
            }
            let mut numbers = BTreeSet::new();
            let mut subscriptions = Vec::new();
            for change in changes {
                let number = change.receiver_channel();
                if number == 0 || !numbers.insert(number) {
                    return Err(ControlError::Invalid(
                        "Receiver channels must be positive and unique".into(),
                    ));
                }
                let (device, channel) = match change {
                    SubscriptionChange::Clear { .. } => (String::new(), String::new()),
                    SubscriptionChange::Set {
                        transmitter_device,
                        transmitter_channel,
                        ..
                    } => {
                        if transmitter_device.is_empty() || transmitter_channel.is_empty() {
                            return Err(ControlError::Invalid(
                                "A subscription requires a source device and channel".into(),
                            ));
                        }
                        (transmitter_device.clone(), transmitter_channel.clone())
                    }
                };
                subscriptions.push(Subscription {
                    receiver_channel: number,
                    transmitter_device: device,
                    transmitter_channel: channel,
                });
            }
            Ok(Control::Subscriptions { subscriptions })
        }
        _ => Err(ControlError::Unavailable(
            "This operation is not integrated with native managed control yet".into(),
        )),
    }
}

impl Registry {
    pub fn configuration_revision(&self) -> Result<String, ControlError> {
        self.state_revision().map_err(control_error)
    }

    pub async fn control(&self, request: DeviceRequest) -> Result<ControlCompletion, ControlError> {
        let (response, received) = tokio::sync::oneshot::channel();
        self.requests
            .try_send(Request::Control { request, response })
            .map_err(|_| {
                ControlError::Unavailable("Managed operation queue is unavailable".into())
            })?;
        received.await.map_err(|_| ControlError::Unavailable("Managed worker stopped before the operation result was available; do not replay the command".into()))?
    }

    pub(super) async fn control_owned(
        &self,
        request: DeviceRequest,
    ) -> Result<ControlCompletion, ControlError> {
        let configuration = self.configuration_change.lock().await;
        let server = request.record["ddm_server_profile"].as_str().map(str::to_owned);
        let settles = settles_routes(&request.operation);
        let result = self.control_locked(request, Some(configuration)).await;
        let sent = |outcome: &Value| outcome["request_sent"] == true || outcome["change_sent"] == true;
        let changed = match &result {
            Ok(completion) => {
                completion.acknowledged_commands > 0
                    || completion.observation.as_ref().is_some_and(sent)
            }
            Err(ControlError::Managed { result, .. }) => sent(result),
            Err(_) => false,
        };
        if settles
            && changed
            && let Some(server) = server
            && let Err(error) = self.schedule_settle_readbacks(&server)
        {
            tracing::warn!(%error, server, "managed settle readback could not be scheduled");
        }
        result
    }

    pub(super) async fn control_locked(
        &self,
        request: DeviceRequest,
        configuration: Option<tokio::sync::MutexGuard<'_, ()>>,
    ) -> Result<ControlCompletion, ControlError> {
        self.load_configuration().await.map_err(control_error)?;
        let revision = self.configuration_revision()?;
        if request.configuration_revision != revision
            || request
                .precondition
                .as_ref()
                .is_some_and(|expected| expected["managed_configuration"] != revision)
        {
            return Err(ControlError::Conflict(
                "Managed configuration changed before execution; request a new preview".into(),
            ));
        }
        let server = text(&request.record, "ddm_server_profile")?;
        let identifier = text(&request.record, "ddm_device_id")?;
        let domain = text(&request.record, "ddm_domain_id")?;
        let source = self
            .source(server)
            .and_then(|source| source.acting_as(server, &request.caller))
            .map_err(control_error)?;
        let client = source.client.as_ref().ok_or_else(|| {
            ControlError::Unavailable("Log in to the selected DDM server first".into())
        })?;
        if let ControlOperation::RefreshClock { profile } = request.operation {
            let completion = self
                .controller_inspect(
                    &request,
                    &source,
                    &revision,
                    crate::devices::registry::Inspection::Status(
                        crate::monitoring::requests::StatusQuery::ClockProfile(profile),
                    ),
                )
                .await?;
            let record = completion.observation.ok_or_else(|| {
                ControlError::ObservationUnavailable(
                    "Managed clock observation is unavailable".into(),
                )
            })?;
            let status = record["clock_status"].as_object().ok_or_else(|| {
                ControlError::ObservationUnavailable("Managed clock status is unavailable".into())
            })?;
            return Ok(ControlCompletion {
                acknowledged_commands: 0,
                readback_completed: true,
                readback_errors: Vec::new(),
                observation: Some(
                    json!({"transport":"ddm","control_transport":"controller","request_sent":false,"effective_state_confirmed":true,"status":status}),
                ),
            });
        }
        if matches!(request.operation, ControlOperation::InspectTransmitFlows) {
            return self
                .controller_transmit_flows(&request, &source, &revision)
                .await;
        }
        if let ControlOperation::RefreshAudio(kind) = request.operation {
            return self
                .controller_inspect(
                    &request,
                    &source,
                    &revision,
                    crate::devices::registry::Inspection::Status(kind.query()),
                )
                .await;
        }
        if matches!(request.operation, ControlOperation::InspectMulticastPrefix) {
            return self
                .controller_inspect(
                    &request,
                    &source,
                    &revision,
                    crate::devices::registry::Inspection::MulticastPrefix,
                )
                .await;
        }
        if let ControlOperation::InspectStatus(inspection) = request.operation {
            return self
                .controller_inspect(&request, &source, &revision, inspection)
                .await;
        }
        if let Some(action) = super::device_control::Action::from_operation(&request.operation) {
            return self
                .controller_control(&request, &source, &revision, action)
                .await;
        }
        if let ControlOperation::Panel(operation) = &request.operation {
            return self
                .controller_panel(&request, &source, &revision, operation)
                .await;
        }
        if let ControlOperation::SampleRate(operation) = &request.operation {
            return self
                .controller_sample_rate(&request, &source, &revision, operation)
                .await;
        }
        if let Some(operation) = super::device_maintenance::operation(&request.operation) {
            return self
                .controller_clear(&request, &source, &revision, operation)
                .await;
        }
        if let Some(configuration) =
            super::device_configuration::Configuration::from_operation(&request.operation)
        {
            return self
                .controller_configure(&request, &source, &revision, &configuration)
                .await;
        }
        let command = encode(&request.operation)?;
        let mut audit = Audit::begin_device(
            &self.journal,
            request.record.clone(),
            command.operation_name(),
            json!({"device_id": identifier, "domain_id": domain, "command": command}),
        )
        .await
        .map_err(control_error)?;
        let preflight = async {
            let fresh = self.fresh_target(&request, &source, &revision).await?;
            let mut record = fresh.target.record();
            record["server_name"] = request.record["server_name"].clone();
            plan(&record, &request.operation)
        }
        .await;
        let command = match preflight {
            Ok(command) => command,
            Err(error) => {
                let value = json!({"operation_id": audit.identifier(), "request_attempted": false, "request_sent": false, "accepted": false, "state_verified": false, "state": "refused", "error": error.to_string()});
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
        let response = client
            .execute(
                &command.query(),
                command.variables(identifier),
                Some(command.operation_name()),
            )
            .await;
        let mut value = json!({"operation_id": audit.identifier(), "transport": "ddm", "request_attempted": true, "request_sent": null, "accepted": null, "state_verified": false, "readback_fresh": false, "state": "unconfirmed"});
        match response {
            Ok(response) => {
                value["request_sent"] = json!(true);
                match command.acknowledgement(&response) {
                    Ok(accepted) => {
                        value["accepted"] = json!(accepted);
                        if !accepted {
                            value["error"] = json!("DDM rejected the device command");
                        }
                    }
                    Err(error) => value["error"] = json!(error),
                }
                if !response.errors.is_empty() {
                    value["graphql_errors"] = json!(response.errors);
                }
            }
            Err(error) => {
                value["request_sent"] = json!(match error {
                    Error::Invalid(_) | Error::Credential(_) | Error::Busy => Some(false),
                    Error::Authentication
                    | Error::Status(_)
                    | Error::Response(_)
                    | Error::TooLarge => Some(true),
                    _ => None,
                });
                value["error"] = json!(error.to_string());
            }
        }
        let acknowledgement_error = audit.acknowledgement(&self.journal, &value).await.err();
        let acknowledged = value["accepted"] == true;
        if acknowledged {
            for event in pending_events(&request.record, &request.operation) {
                if let Err(error) = self.model.publish(event) {
                    tracing::warn!(%error, "managed subscription progress was not published");
                }
            }
        }
        drop(configuration);
        let readback = settle(
            || self.command_readback(server, &source, identifier, domain),
            |observed| {
                !acknowledged
                    || observed.as_ref().is_ok_and(|target| {
                        matches(&command, target) && !establishing(&command, target)
                    })
            },
            READBACK_INTERVAL,
            READBACK_LIMIT,
        )
        .await;
        match readback {
            Ok(observed) => {
                value["readback_fresh"] = json!(true);
                value["state_verified"] = json!(matches(&command, &observed));
                value["device"] = observed.record();
                if matches!(command, Control::PreferredLeader { .. }) {
                    value["status"] = json!({"preferred_leader": observed.device.clock_preferences.as_ref().and_then(|preferences| preferences.leader)});
                }
            }
            Err(error) => value["readback_error"] = json!(error.to_string()),
        }
        let accepted = value["accepted"].as_bool();
        let verified = value["state_verified"] == true;
        if let Control::PreferredLeader { enabled } = command {
            value["requested"] = json!({"preferred_leader": enabled});
            value["persistence"] = json!("unknown");
        }
        value["effective_state_confirmed"] = json!(verified);
        value["request_acknowledged"] = value["accepted"].clone();
        value["state"] = json!(match (accepted, verified) {
            (Some(false), true) => "inconsistent",
            (_, true) => "confirmed",
            (Some(true), false) => "accepted",
            (Some(false), false) => "rejected",
            (None, false) => "unconfirmed",
        });
        let mut status = match accepted {
            Some(true) => 200,
            Some(false) => 409,
            None => 502,
        };
        if let Some(error) = acknowledgement_error {
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
            readback_completed: value["readback_fresh"] == true,
            readback_errors: value["readback_error"]
                .as_str()
                .map(str::to_owned)
                .into_iter()
                .collect(),
            observation: Some(value),
        })
    }

    async fn command_readback(
        &self,
        server: &str,
        source: &Arc<Source>,
        identifier: &str,
        domain: &str,
    ) -> Result<Observation, Error> {
        if !self.refresh_queued(server).await? {
            return Err(Error::Response("Managed command readback is unavailable"));
        }
        let inventory = self.command_inventory(server, source)?;
        let target = inventory.device(identifier)?;
        if target.domain_id.as_deref() != Some(domain) {
            return Err(Error::Conflict(
                "Managed device changed domains during the command",
            ));
        }
        Ok(target.clone())
    }

    pub(super) async fn fresh_target(
        &self,
        request: &DeviceRequest,
        source: &Arc<Source>,
        revision: &str,
    ) -> Result<FreshTarget, ControlError> {
        let server = text(&request.record, "ddm_server_profile")?;
        if !self.refresh_queued(server).await.map_err(control_error)? {
            return Err(ControlError::ObservationUnavailable(
                "Current managed inventory is unavailable".into(),
            ));
        }
        let inventory = self
            .command_inventory(server, source)
            .map_err(control_error)?;
        if !inventory.fresh {
            return Err(ControlError::ObservationUnavailable(
                "Current managed inventory is not complete and fresh".into(),
            ));
        }
        let target = inventory
            .device(text(&request.record, "ddm_device_id")?)
            .map_err(control_error)?
            .clone();
        if target.domain_id.as_deref() != Some(text(&request.record, "ddm_domain_id")?) {
            return Err(ControlError::Conflict(
                "Managed device changed domains before the operation".into(),
            ));
        }
        let record = self
            .model
            .device(text(&request.record, "server_name")?)?
            .ok_or_else(|| {
                ControlError::Conflict(
                    "Managed device identity changed before the operation".into(),
                )
            })?;
        if !required(&record)
            || ["ddm_device_id", "ddm_domain_id", "ddm_server_profile"]
                .iter()
                .any(|field| record[field] != request.record[field])
        {
            return Err(ControlError::Conflict(
                "Managed device ownership changed before the operation".into(),
            ));
        }
        let mut current = request.operation.precondition(&record);
        current["managed_configuration"] = json!(revision);
        if request
            .precondition
            .as_ref()
            .is_some_and(|expected| current != *expected)
        {
            return Err(ControlError::Conflict(
                "Device changed after confirmation; request a new preview".into(),
            ));
        }
        Ok(FreshTarget {
            inventory,
            target,
            record,
        })
    }
}

async fn settle<T, R, F>(
    mut read: R,
    settled: impl Fn(&T) -> bool,
    interval: Duration,
    limit: Duration,
) -> T
where
    R: FnMut() -> F,
    F: Future<Output = T>,
{
    let deadline = Instant::now() + limit;
    loop {
        let result = read().await;
        if settled(&result) || Instant::now() + interval > deadline {
            return result;
        }
        tokio::time::sleep(interval).await;
    }
}

fn pending_events(record: &Value, operation: &ControlOperation) -> Vec<Value> {
    let ControlOperation::Subscriptions { changes } = operation else {
        return Vec::new();
    };
    changes
        .iter()
        .map(|change| {
            let (action, transmitter_device, transmitter_channel) = match change {
                SubscriptionChange::Set {
                    transmitter_device,
                    transmitter_channel,
                    ..
                } => (
                    "add",
                    transmitter_device.as_str(),
                    transmitter_channel.as_str(),
                ),
                SubscriptionChange::Clear { .. } => ("remove", "", ""),
            };
            json!({
                "event": "subscription_pending",
                "action": action,
                "server_name": record["server_name"],
                "rx_device": record["server_name"],
                "rx_channel": change.receiver_channel(),
                "tx_channel": transmitter_channel,
                "tx_device": transmitter_device,
            })
        })
        .collect()
}

fn establishing(command: &Control, target: &Observation) -> bool {
    let Control::Subscriptions { subscriptions } = command else {
        return false;
    };
    subscriptions
        .iter()
        .filter(|subscription| !subscription.transmitter_channel.is_empty())
        .any(|subscription| {
            target
                .device
                .rx_channels
                .iter()
                .flatten()
                .find(|channel| channel.index == subscription.receiver_channel as i64)
                .and_then(|channel| channel.status.as_deref())
                .is_some_and(|status| {
                    netaudio_core::subscription_status::classification_for_identifier(
                        &status.trim().to_ascii_uppercase(),
                    )
                    .severity
                        == "progress"
                })
        })
}

fn matches(command: &Control, target: &Observation) -> bool {
    match command {
        Control::Name { name } => {
            !name.is_empty()
                && netaudio_core::managed_api::inventory::device_name(&target.device) == name
        }
        Control::PreferredLeader { enabled } => {
            target
                .device
                .clock_preferences
                .as_ref()
                .and_then(|preferences| preferences.leader)
                == Some(*enabled)
        }
        Control::Subscriptions { subscriptions } => subscriptions.iter().all(|subscription| {
            let Some(channel) = target.device.rx_channels.as_ref().and_then(|channels| {
                channels
                    .iter()
                    .find(|channel| channel.index == subscription.receiver_channel as i64)
            }) else {
                return false;
            };
            let Some(device) = channel.subscribed_device.as_deref() else {
                return false;
            };
            let source_matches = device == subscription.transmitter_device
                || device == "."
                    && subscription.transmitter_device
                        == netaudio_core::managed_api::inventory::device_name(&target.device);
            source_matches
                && channel.subscribed_channel.as_deref()
                    == Some(subscription.transmitter_channel.as_str())
        }),
    }
}

fn text<'a>(record: &'a Value, field: &str) -> Result<&'a str, ControlError> {
    record[field]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ControlError::Conflict(format!("Managed target has no {field}")))
}

fn control_error(error: Error) -> ControlError {
    match error {
        Error::Forbidden(message) => ControlError::Managed {
            status: 403,
            result: json!({"error": message}),
        },
        error => ControlError::Unavailable(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receiver(source: &str, channel: &str) -> Observation {
        Observation {
            device: serde_json::from_value(json!({
                "id": "receiver", "name": "receiver", "domainId": "domain",
                "rxChannels": [{"id": "rx-1", "index": 1, "subscribedDevice": source, "subscribedChannel": channel}],
            }))
            .expect("receiver fixture"),
            domain_id: Some("domain".into()),
            domain_name: None,
            observed_at: None,
            fresh: true,
            server_profile: "server".into(),
            context_name: None,
            operation_permissions: None,
            discovery: None,
        }
    }

    #[test]
    fn a_self_connection_names_the_device_ddm_knows() {
        let record = json!({"name": "receiver", "server_name": "receiver.local", "channels": {"receivers": {"1": {}}}});
        for transmitter_device in [".", "receiver", "RECEIVER.local"] {
            let operation = ControlOperation::Subscriptions {
                changes: vec![SubscriptionChange::Set {
                    receiver_channel: 1,
                    transmitter_device: transmitter_device.into(),
                    transmitter_channel: "01".into(),
                }],
            };
            let Control::Subscriptions { subscriptions } =
                plan(&record, &operation).expect("planned self-connection")
            else {
                panic!("subscription command expected");
            };
            assert_eq!(subscriptions[0].transmitter_device, "receiver");
        }
    }

    #[test]
    fn a_self_connection_ddm_reports_as_a_dot_matches_the_device_name() {
        let command = Control::Subscriptions {
            subscriptions: vec![Subscription {
                receiver_channel: 1,
                transmitter_device: "receiver".into(),
                transmitter_channel: "01".into(),
            }],
        };
        assert!(matches(&command, &receiver(".", "01")));
        assert!(matches(&command, &receiver("receiver", "01")));
        assert!(!matches(&command, &receiver(".", "02")));
    }
}
