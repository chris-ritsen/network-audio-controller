use super::confirmation::{PreviewTokens, Redemption, preview_digest};
use super::service::CallError;
use super::views::{self, Records};
use crate::authorization::Grant;
use crate::devices::operations::{ControlError, ControlOperation, SubscriptionChange};
use crate::devices::registry::DeviceRegistry;
use crate::state::ReadModel;
use futures_util::{StreamExt, stream};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

mod diagnostics;
mod external_subscription;
mod flow_authoring;
mod forgetting;
mod host_audio;
mod locking;
mod maintenance;
mod managed;
mod metering;
mod panels;
mod preset_load;
mod presets;
mod sample_rate;
mod settings;
mod wireless;

const WRITE_CONCURRENCY: usize = 16;
const NEXT_STEP: &str = "Nothing has changed. Tell the user what this will do and wait for their answer; only after they agree, call again with the same arguments, confirmed=true and this preview's preview_token.";
const MISSING_CONFIRMATION: &str = "Not changed: confirmed=true needs the preview_token from a preview of this exact change, shown to the user first. Tell the user what the preview says will change and ask; if they agree, call again without confirmed to get a preview_token, then confirm with it.";
const UNKNOWN_CONFIRMATION: &str = "Not changed: that preview_token is unknown, already used or expired. Tell the user what this preview says will change and ask again; if they agree, confirm with the new preview_token.";
const CHANGED_CONFIRMATION: &str = "Not changed: the arguments or the situation are different from the preview that preview_token came from. Tell the user what this new preview says will change and ask again; if they agree, confirm with the new preview_token.";

struct PreparedDevice {
    reference: String,
    operation: ControlOperation,
    precondition: Value,
}

struct Route {
    receiver: String,
    receiver_label: String,
    number: u16,
    source: Option<(String, String)>,
    before: String,
}

impl Route {
    fn requested(&self) -> String {
        self.source.as_ref().map_or_else(
            || "nothing".to_owned(),
            |(device, channel)| format!("{device}:{channel}"),
        )
    }

    fn change(&self) -> Value {
        json!({"receiver": self.receiver_label, "from": self.before, "to": self.requested()})
    }

    fn after(&self, records: &Records) -> Result<Value, CallError> {
        let record = views::resolve(records, &self.receiver)?;
        let route = record.and_then(|record| current_route(record, self.number));
        let now = source_text(route);
        let mut view = json!({
            "receiver": self.receiver_label,
            "requested": self.requested(),
            "now": now,
            "verified": record.is_some() && now == self.requested()
                && !matches!(route.and_then(|route| route["status"]["state"].as_str()), Some("in_progress" | "pending" | "unknown")),
        });

        if let Some(route) = route {
            view["status"] = route["status"]["label"].clone();

            if matches!(
                route["status"]["severity"].as_str(),
                Some("warning" | "error")
            ) {
                view["problem"] = route["status"]["detail"].clone();
            }
        }

        Ok(view)
    }
}

enum PreparedKind {
    HostAudio(host_audio::PreparedHostAudio),
    Wireless(wireless::PreparedWireless),
    Managed(managed::PreparedManaged),
    Panel {
        reference: String,
    },
    SavePreset(presets::PreparedSave),
    ApplyPreset(preset_load::PreparedLoad),
    Forget(forgetting::PreparedForget),
    Locking {
        reference: String,
    },
    Maintenance {
        reference: String,
    },
    SampleRate {
        reference: String,
    },
    Flow {
        reference: String,
    },
    ExternalSubscription {
        reference: String,
    },
    Diagnostics(diagnostics::PreparedDiagnostics),
    Metering(metering::PreparedMetering),
    Routes(Vec<Route>),
    Rename {
        reference: String,
        before: String,
        after: String,
    },
    Setting(settings::PreparedSetting),
}

struct Prepared {
    preview: Value,
    devices: Vec<PreparedDevice>,
    kind: PreparedKind,
}

pub struct Writes {
    host_audio: Option<Arc<crate::host_audio::manager::Manager>>,
    signal: Option<Arc<crate::host_audio::signal::Service>>,
    shure: Arc<crate::shure::Registry>,
    managed: Arc<crate::managed::registry::Registry>,
    registry: Arc<DeviceRegistry>,
    model: Arc<ReadModel>,
    tokens: PreviewTokens,
    presets: Arc<crate::presets::Service>,
    metering: Arc<crate::monitoring::metering_streams::MeteringStreams>,
}

impl Writes {
    pub fn new(
        registry: Arc<DeviceRegistry>,
        model: Arc<ReadModel>,
        metering: Arc<crate::monitoring::metering_streams::MeteringStreams>,
        presets: Arc<crate::presets::Service>,
        managed: Arc<crate::managed::registry::Registry>,
        shure: Arc<crate::shure::Registry>,
        host_audio: Option<Arc<crate::host_audio::manager::Manager>>,
    ) -> Self {
        Self {
            host_audio,
            signal: None,
            shure,
            managed,
            registry,
            model,
            tokens: PreviewTokens::default(),
            presets,
            metering,
        }
    }

    pub fn with_signal(mut self, signal: Option<Arc<crate::host_audio::signal::Service>>) -> Self {
        self.signal = signal;
        self
    }

    pub fn supports(operation: &str) -> bool {
        host_audio::supports(operation)
            || managed::supports(operation)
            || operation == "set_wireless_value"
            || flow_authoring::supports(operation)
            || locking::supports(operation)
            || maintenance::supports(operation)
            || operation == "set_sample_rate"
            || operation == "forget_device"
            || operation == "save_preset"
            || operation == "apply_preset"
            || operation == "apply_device_control"
            || settings::supports(operation)
            || diagnostics::supports(operation)
            || metering::supports(operation)
            || matches!(
                operation,
                "subscribe"
                    | "unsubscribe"
                    | "set_subscriptions"
                    | "rename_device"
                    | "subscribe_external_flow"
            )
    }

    pub async fn call(
        &self,
        operation: &str,
        arguments: &Value,
        grant: &Grant,
    ) -> Result<(Value, bool), CallError> {
        let request: Map<_, _> = arguments
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(key, _)| !["confirmed", "preview_token"].contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let request = Value::Object(request);

        self.refresh_managed_targets(operation, &request).await?;

        if maintenance::supports(operation) || locking::supports(operation) {
            let reference = required_text(&request, "device")?;
            let record = self.model.device(reference)?;
            if operation == "clear_configuration"
                || record
                    .as_deref()
                    .is_none_or(|record| !crate::managed::control::required(record))
            {
                self.registry
                    .apply(
                        reference,
                        ControlOperation::InspectStatus(crate::devices::registry::Inspection::Lock),
                    )
                    .await
                    .map_err(|error| CallError::invalid(error.to_string()))?;
            }
        }

        if operation == "rename_channel" {
            let reference = required_text(&request, "device")?;
            if self
                .model
                .device(reference)?
                .as_deref()
                .is_some_and(crate::managed::control::required)
            {
                self.registry
                    .apply(
                        reference,
                        ControlOperation::InspectStatus(crate::devices::registry::Inspection::Lock),
                    )
                    .await
                    .map_err(|error| CallError::invalid(error.to_string()))?;
            }
        }

        if operation == "set_latency" {
            self.registry
                .apply(
                    required_text(&request, "device")?,
                    ControlOperation::InspectStatus(crate::devices::registry::Inspection::Latency),
                )
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        if operation == "set_aes67_multicast_prefix" {
            self.registry
                .apply(
                    required_text(&request, "device")?,
                    ControlOperation::InspectMulticastPrefix,
                )
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        if matches!(operation, "set_interface" | "set_redundancy") {
            self.registry
                .apply(
                    required_text(&request, "device")?,
                    ControlOperation::InspectStatus(
                        crate::devices::registry::Inspection::NetworkConfiguration,
                    ),
                )
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        if matches!(operation, "set_aes67" | "set_gain") {
            let query = if operation == "set_gain" {
                crate::monitoring::requests::StatusQuery::Codec
            } else {
                crate::monitoring::requests::StatusQuery::Aes67
            };
            self.registry
                .apply(
                    required_text(&request, "device")?,
                    ControlOperation::InspectStatus(crate::devices::registry::Inspection::Status(
                        query,
                    )),
                )
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        if operation == "subscribe_external_flow" {
            let reference = required_text(&request, "rx_device")?;
            self.registry
                .apply(reference, ControlOperation::RefreshExternalReceiver)
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        if let Some(kind) = settings::audio_kind(operation) {
            let reference = required_text(&request, "device")?;
            self.registry
                .apply(reference, ControlOperation::RefreshAudio(kind))
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        if settings::requires_clock(operation) {
            let reference = required_text(&request, "device")?;
            let records = self.model.records()?;
            let record = require_record(&records, &request["device"])?;
            if crate::managed::control::required(record) {
                let server = required_text(record, "ddm_server_profile")?.to_owned();
                let inventory = self
                    .managed
                    .selection_inventory(Some(server))
                    .await
                    .map_err(|error| CallError::invalid(error.to_string()))?;
                if !inventory.devices.iter().any(|current| {
                    ["ddm_device_id", "ddm_domain_id", "ddm_server_profile"]
                        .iter()
                        .all(|field| current[field] == record[field])
                }) {
                    return Err(CallError::invalid(
                        "Managed device ownership changed before the clock preview",
                    ));
                }
            } else {
                self.registry
                    .apply(
                        reference,
                        ControlOperation::RefreshClock {
                            profile: netaudio_core::commands::default_clock_control_profile(),
                        },
                    )
                    .await
                    .map_err(|error| CallError::invalid(error.to_string()))?;
            }
        }

        let records = if operation == "forget_device" {
            self.registry.cached_records().await?
        } else {
            self.model.records()?
        };
        let mut prepared = if host_audio::supports(operation) {
            host_audio::prepare(operation, &request, &self.host_audio).await?
        } else if operation == "set_wireless_value" {
            wireless::prepare(&request, &self.shure)?
        } else if managed::supports(operation) {
            managed::prepare(operation, &request, &self.managed).await?
        } else if operation == "apply_device_control" {
            panels::prepare(&request, &self.registry, &self.model).await?
        } else if operation == "save_preset" {
            presets::prepare(&request, &records, &self.presets).await?
        } else if operation == "apply_preset" {
            preset_load::prepare(&request, &records, &self.presets, &self.registry).await?
        } else if operation == "forget_device" {
            forgetting::prepare(&request, &records)?
        } else if operation == "set_sample_rate" {
            sample_rate::prepare(&request, &self.registry, &self.model).await?
        } else if maintenance::supports(operation) {
            maintenance::prepare(operation, &request, &records)?
        } else if locking::supports(operation) {
            locking::prepare(operation, &request, &records)?
        } else if flow_authoring::supports(operation) {
            flow_authoring::prepare(operation, &request, &self.registry, &self.model).await?
        } else if operation == "subscribe_external_flow" {
            external_subscription::prepare(&request, &records, &self.model)?
        } else {
            prepare(operation, &request, &records)?
        };

        for device in &mut prepared.devices {
            self.registry
                .validate_operation(&device.reference, &device.operation)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            self.registry
                .bind_managed_precondition(&device.reference, &mut device.precondition)
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        let mut conditions: BTreeMap<_, _> = prepared
            .devices
            .iter()
            .map(|device| (device.reference.clone(), device.precondition.clone()))
            .collect();

        if let PreparedKind::Forget(forgetting) = &prepared.kind {
            conditions.extend(forgetting.conditions());
        }

        if let PreparedKind::Diagnostics(diagnostics) = &prepared.kind {
            conditions.insert(
                diagnostics.reference.clone(),
                diagnostics.precondition.clone(),
            );
        }

        if let PreparedKind::Metering(metering) = &prepared.kind {
            if metering.start {
                self.registry
                    .metering_target(&metering.reference)
                    .map_err(|error| CallError::invalid(error.to_string()))?;
            }

            conditions.insert(metering.reference.clone(), metering.precondition.clone());
        }

        if let PreparedKind::SavePreset(preset) = &prepared.kind {
            conditions.extend(preset.conditions());
        }
        if let PreparedKind::ApplyPreset(preset) = &prepared.kind {
            conditions.extend(preset.conditions());
        }

        if let PreparedKind::Managed(managed) = &prepared.kind {
            conditions.insert("managed".into(), managed.conditions());
        }
        if let PreparedKind::Wireless(wireless) = &prepared.kind {
            conditions.insert("wireless".into(), wireless.precondition.clone());
        }
        if let PreparedKind::HostAudio(host_audio) = &prepared.kind {
            conditions.insert("host_audio".into(), host_audio.conditions());
        }

        let bound_request = json!({"request": request, "conditions": conditions});
        if let (Some(signal), PreparedKind::Routes(routes)) = (&self.signal, &prepared.kind) {
            let sources = routes
                .iter()
                .flat_map(|route| [route.before.clone(), route.requested()])
                .filter(|source| source != "nothing")
                .collect::<BTreeSet<_>>();
            match signal.channels_carry(&sources).await {
                Ok(context) if !context.is_empty() => {
                    prepared.preview["source_context"] = json!(context)
                }
                Ok(_) => {}
                Err(error) => prepared.preview["source_context_error"] = json!(error.to_string()),
            }
        }
        let digest = preview_digest(operation, &bound_request, &prepared.preview);

        if arguments["confirmed"] != true {
            prepared.preview["preview_token"] = json!(self.tokens.issue(&grant.client_id, digest)?);
            return Ok((prepared.preview, false));
        }

        let redemption = self.tokens.redeem(
            arguments["preview_token"].as_str(),
            &grant.client_id,
            digest,
        )?;

        if !matches!(redemption, Redemption::Accepted { .. }) {
            if let Some(fields) = prepared.preview.as_object_mut() {
                fields.remove("next");
            }

            prepared.preview["confirmation"] = json!(match redemption {
                Redemption::Missing => MISSING_CONFIRMATION,
                Redemption::Unknown => UNKNOWN_CONFIRMATION,
                Redemption::Changed { .. } => CHANGED_CONFIRMATION,
                Redemption::Accepted { .. } => unreachable!(),
            });

            if redemption != Redemption::Missing {
                prepared.preview["preview_token"] =
                    json!(self.tokens.issue(&grant.client_id, digest)?);
            }

            return Ok((prepared.preview, false));
        }

        tracing::info!(
            client = grant.client_name,
            operation,
            "MCP confirmed device operation"
        );

        if let PreparedKind::Forget(forgetting) = prepared.kind {
            return forgetting.apply(&self.registry).await;
        }

        if let PreparedKind::Diagnostics(diagnostics) = prepared.kind {
            return diagnostics.apply(&self.registry);
        }

        if let PreparedKind::Metering(metering) = prepared.kind {
            return metering.apply(&self.metering, &grant.client_id);
        }

        if let PreparedKind::SavePreset(preset) = prepared.kind {
            return preset.apply(&self.presets, &self.registry).await;
        }
        if let PreparedKind::ApplyPreset(preset) = prepared.kind {
            return preset.apply(&self.presets, &self.registry).await;
        }

        if let PreparedKind::Managed(managed) = prepared.kind {
            return managed.apply(&self.managed).await;
        }
        if let PreparedKind::Wireless(wireless) = prepared.kind {
            return wireless.apply(&self.shure).await;
        }
        if let PreparedKind::HostAudio(host_audio) = prepared.kind {
            return host_audio.apply(&self.host_audio).await;
        }

        let mut executions = stream::iter(prepared.devices.into_iter().map(|device| {
            let registry = Arc::clone(&self.registry);
            async move {
                let result = registry
                    .apply_conditionally(
                        &device.reference,
                        device.operation,
                        Some(device.precondition),
                    )
                    .await;
                (device.reference, result)
            }
        }))
        .buffer_unordered(WRITE_CONCURRENCY);
        let mut outcomes = BTreeMap::new();

        while let Some((reference, result)) = executions.next().await {
            outcomes.insert(reference, result);
        }

        let after = self.model.records()?;
        let failed = outcomes.values().any(Result::is_err);
        let mut result = json!({"changed": !failed, "operation": operation});

        match prepared.kind {
            PreparedKind::Panel { reference } => return panels::outcome(&reference, &outcomes),
            PreparedKind::Managed(_)
            | PreparedKind::HostAudio(_)
            | PreparedKind::Wireless(_)
            | PreparedKind::Forget(_)
            | PreparedKind::SavePreset(_)
            | PreparedKind::ApplyPreset(_) => unreachable!(),
            PreparedKind::Locking { reference } => {
                return locking::outcome(&reference, &outcomes);
            }
            PreparedKind::Maintenance { reference } => {
                return maintenance::outcome(&reference, &outcomes);
            }
            PreparedKind::Flow { reference } => {
                return flow_authoring::outcome(&reference, &outcomes, &after);
            }
            PreparedKind::SampleRate { reference } => {
                return sample_rate::outcome(&reference, &outcomes, &after);
            }
            PreparedKind::ExternalSubscription { reference } => {
                return external_subscription::outcome(&reference, &outcomes);
            }
            PreparedKind::Diagnostics(_) => unreachable!(),
            PreparedKind::Metering(_) => unreachable!(),
            PreparedKind::Setting(setting) => return setting.outcome(&outcomes, &after),
            PreparedKind::Routes(routes) => {
                let mut views = Vec::new();
                let mut failures = 0;

                for route in routes {
                    let mut view = route.after(&after)?;

                    if let Some(Err(error)) = outcomes.get(&route.receiver) {
                        if let ControlError::Subscription(failure) = error {
                            let outcome = failure.route(route.number);
                            failures += usize::from(outcome["ok"] != true);

                            if !failure.readback_completed {
                                view["verified"] = json!(false);
                            }

                            view.as_object_mut()
                                .expect("route view is an object")
                                .extend(
                                    outcome
                                        .as_object()
                                        .expect("route outcome is an object")
                                        .clone(),
                                );
                        } else {
                            view["error"] = json!(error.to_string());
                            view["verified"] = json!(false);
                            failures += 1;
                        }
                    } else if let Some(Ok(completion)) = outcomes.get(&route.receiver)
                        && !completion.readback_completed
                    {
                        view["verified"] = json!(false);
                        view["readback_errors"] = json!(completion.readback_errors);
                    }

                    if let Some(managed) = outcomes
                        .get(&route.receiver)
                        .and_then(crate::managed::control::outcome)
                    {
                        view["verified"] =
                            json!(view["verified"] == true && managed["state_verified"] == true);
                        view["managed"] = crate::managed::control::evidence(managed);
                    }
                    views.push(view);
                }

                if operation == "set_subscriptions" {
                    result["applied"] = json!(views.len() - failures);
                    result["failed"] = json!(failures);
                    result["changed"] = json!(views.len() > failures);
                    result["routes"] = json!(views);
                } else if let Some(view) = views
                    .into_iter()
                    .next()
                    .and_then(|view| view.as_object().cloned())
                    && let Some(fields) = result.as_object_mut()
                {
                    fields.extend(view);
                }
            }
            PreparedKind::Rename {
                reference,
                before,
                after: requested,
            } => {
                let now =
                    views::resolve(&after, &reference)?.and_then(|record| record["name"].as_str());
                let managed = outcomes
                    .get(&reference)
                    .and_then(crate::managed::control::outcome);
                let verified = now == Some(requested.as_str())
                    && managed.is_none_or(|managed| managed["state_verified"] == true);
                result["device"] = json!(before);
                result["change"] = json!({"name": {"from": before, "to": requested, "now": now}});
                result["verified"] = json!(verified);
                if let Some(managed) = managed {
                    result["managed"] = crate::managed::control::evidence(managed);
                }

                if let Some(Err(error)) = outcomes.get(&reference) {
                    result["error"] = json!(error.to_string());
                    result["changed"] = json!(verified);
                }
            }
        }

        if failed {
            result["readback"] = json!(
                "Read the observed values before retrying; a failed or unanswered command may have partially applied."
            );
        }

        Ok((views::compact(result), failed))
    }

    async fn refresh_managed_targets(
        &self,
        operation: &str,
        request: &Value,
    ) -> Result<(), CallError> {
        if !matches!(
            operation,
            "rename_device"
                | "subscribe"
                | "unsubscribe"
                | "set_subscriptions"
                | "identify"
                | "reboot"
        ) {
            return Ok(());
        }
        let records = self.model.records()?;
        let selectors = if operation == "set_subscriptions" {
            request["routes"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|route| &route["rx_device"])
                .collect::<Vec<_>>()
        } else {
            vec![
                &request[if matches!(operation, "rename_device" | "identify" | "reboot") {
                    "device"
                } else {
                    "rx_device"
                }],
            ]
        };
        let mut servers = BTreeSet::new();
        for selector in selectors {
            if let Some(record) = selector
                .as_str()
                .map(|selector| views::resolve(&records, selector))
                .transpose()?
                .flatten()
                && crate::managed::control::required(record)
            {
                servers.insert(required_text(record, "ddm_server_profile")?.to_owned());
            }
        }
        for server in servers {
            self.managed
                .selection_inventory(Some(server))
                .await
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }
        Ok(())
    }
}

fn prepare(operation: &str, request: &Value, records: &Records) -> Result<Prepared, CallError> {
    if metering::supports(operation) {
        return metering::prepare(operation, request, records);
    }

    if diagnostics::supports(operation) {
        return diagnostics::prepare(operation, request, records);
    }

    if settings::supports(operation) {
        return settings::prepare(operation, request, records);
    }

    let mut warnings = Vec::new();
    let mut preview = json!({"changed": false, "confirmation_required": true, "operation": operation, "next": NEXT_STEP});

    if operation == "rename_device" {
        let record = require_record(records, &request["device"])?;
        let reference = required_text(record, "server_name")?.to_owned();
        let before = required_text(record, "name")?.to_owned();
        let after = required_text(request, "name")?.to_owned();

        if !after.trim().is_empty() {
            netaudio_core::protocol::validate_dante_name(&after)
                .map_err(|error| CallError::invalid(error.to_string()))?;
        }

        let downstream: Vec<_> = records
            .values()
            .flat_map(|device| {
                views::subscriptions(device)
                    .filter(|route| route["tx_device"] == before)
                    .map(|route| {
                        format!(
                            "{}:{} <- {}:{}",
                            views::text(&device["name"]),
                            views::text(&route["rx_channel"]),
                            before,
                            views::text(&route["tx_channel"])
                        )
                    })
            })
            .collect();

        if !downstream.is_empty() {
            warnings.push(format!("{} route(s) subscribe to {before} by name: {}. They keep running on their current flows, but fail the next time they are re-established (either device restarts, or the route is re-made) unless they are re-pointed to {after}", downstream.len(), downstream.join("; ")));
        }

        if record["online"] == false {
            warnings.push(format!("{before} is offline; the change will fail"));
        }

        if record["is_locked"] == true {
            warnings.push(format!(
                "{before} is locked; unlock it first or the device will refuse the change"
            ));
        }

        preview["device"] = json!(before);
        preview["change"] = json!({"name": {"from": before, "to": after}});
        if !warnings.is_empty() {
            preview["warnings"] = json!(warnings);
        }
        let control = ControlOperation::RenameDevice {
            name: after.clone(),
        };
        let precondition = control.precondition(record);
        return Ok(Prepared {
            preview,
            devices: vec![PreparedDevice {
                reference: reference.clone(),
                operation: control,
                precondition,
            }],
            kind: PreparedKind::Rename {
                reference,
                before,
                after,
            },
        });
    }

    let entries = if operation == "set_subscriptions" {
        request["routes"]
            .as_array()
            .ok_or_else(|| CallError::invalid("routes must be a list"))?
            .iter()
            .collect::<Vec<_>>()
    } else {
        vec![request]
    };
    let mut routes = Vec::new();
    let mut grouped = BTreeMap::<String, Vec<SubscriptionChange>>::new();
    let mut targets = BTreeSet::new();

    for entry in entries {
        let record = require_record(records, &entry["rx_device"])?;
        let reference = required_text(record, "server_name")?.to_owned();
        let number = channel_number(record, &entry["rx_channel"], "receivers")?;

        if !targets.insert((reference.clone(), number)) {
            return Err(CallError::invalid(format!(
                "receive channel {number} on {reference} occurs more than once"
            )));
        }

        let source = match (entry["tx_device"].as_str(), entry.get("tx_channel")) {
            (None, None) => None,
            (Some(source), Some(channel)) if !source.is_empty() => {
                let transmitter = views::resolve(records, source)?;
                let channel = match transmitter {
                    Some(record) => {
                        let number = channel_number(record, channel, "transmitters")?;
                        views::channel_label(&record["channels"]["transmitters"][number.to_string()])
                            .as_str().ok_or_else(|| CallError::invalid("transmit channel has no name"))?.to_owned()
                    }
                    None => channel.as_str().filter(|channel| !channel.is_empty()).ok_or_else(|| {
                        CallError::invalid("name the transmit channel; its device is not on the network, so numbers cannot be resolved")
                    })?.to_owned(),
                };
                let source = transmitter
                    .and_then(|record| record["name"].as_str())
                    .unwrap_or(source)
                    .to_owned();
                netaudio_core::protocol::validate_dante_name(&source)
                    .map_err(|error| CallError::invalid(error.to_string()))?;
                Some((source, channel))
            }
            _ => {
                return Err(CallError::invalid(
                    "tx_device and tx_channel must both be supplied, or both omitted",
                ));
            }
        };
        let channel = views::channel_label(&record["channels"]["receivers"][number.to_string()]);
        let suffix = channel
            .as_str()
            .filter(|label| !label.is_empty())
            .map_or_else(String::new, |label| format!(" ({label})"));
        let route = Route {
            receiver: reference.clone(),
            receiver_label: format!(
                "{} receive channel {number}{suffix}",
                views::text(&record["name"])
            ),
            number,
            source,
            before: source_text(current_route(record, number)),
        };
        let requested = route.requested();

        if requested == route.before {
            warnings.push(format!(
                "{} is already {}; nothing would change",
                route.receiver_label,
                if requested == "nothing" {
                    "unrouted".to_owned()
                } else {
                    format!("fed by {requested}")
                }
            ));
        }

        if let Some((source, _)) = &route.source
            && views::resolve(records, source)?.is_none_or(|record| record["online"] == false)
        {
            warnings.push(format!(
                "{source} is not on the network now; the route stays unresolved until it appears"
            ));
        }

        grouped
            .entry(reference)
            .or_default()
            .push(match &route.source {
                Some((device, channel)) => SubscriptionChange::Set {
                    receiver_channel: number,
                    transmitter_device: device.clone(),
                    transmitter_channel: channel.clone(),
                },
                None => SubscriptionChange::Clear {
                    receiver_channel: number,
                },
            });
        routes.push(route);
    }

    if operation == "set_subscriptions" {
        preview["changes"] = routes.iter().map(Route::change).collect();
    } else if let Some(route) = routes.first() {
        preview["change"] = route.change();
    }

    if !warnings.is_empty() {
        preview["warnings"] = json!(warnings);
    }

    preview["effect"] = json!(route_effect(&routes));
    let mut devices = Vec::new();

    for (reference, changes) in grouped {
        let record = require_record(records, &json!(reference))?;
        let operation = ControlOperation::Subscriptions { changes };
        let precondition = operation.precondition(record);
        devices.push(PreparedDevice {
            reference,
            operation,
            precondition,
        });
    }

    Ok(Prepared {
        preview,
        devices,
        kind: PreparedKind::Routes(routes),
    })
}

fn required_text<'a>(record: &'a Value, field: &str) -> Result<&'a str, CallError> {
    record[field]
        .as_str()
        .ok_or_else(|| CallError::invalid(format!("{field} must be a string")))
}

fn require_record<'a>(records: &'a Records, selector: &Value) -> Result<&'a Value, CallError> {
    let selector = selector
        .as_str()
        .ok_or_else(|| CallError::invalid("device must be a string"))?;
    views::resolve(records, selector)?
        .ok_or_else(|| CallError::invalid(format!("device {selector} not found")))
}

fn channel_number(record: &Value, selector: &Value, direction: &str) -> Result<u16, CallError> {
    let channels = &record["channels"][direction];

    if let Some(label) = selector.as_str() {
        for case_sensitive in [true, false] {
            let matches: Vec<_> = views::channel_names(channels)
                .into_iter()
                .filter(|(_, name)| {
                    name.as_str().is_some_and(|name| {
                        if case_sensitive {
                            name == label
                        } else {
                            caseless::default_caseless_match_str(name, label)
                        }
                    })
                })
                .collect();

            if matches.len() > 1 {
                return Err(CallError::invalid(format!(
                    "channel label {label:?} is ambiguous; use its number"
                )));
            }

            if let Some((number, _)) = matches.first() {
                return number
                    .parse::<u16>()
                    .map_err(|error| CallError::invalid(error.to_string()));
            }
        }
    }

    let number = selector
        .as_u64()
        .and_then(|number| u16::try_from(number).ok())
        .or_else(|| {
            selector
                .as_str()
                .and_then(|label| label.parse::<u16>().ok())
        });

    if let Some(number) = number.filter(|number| *number != 0)
        && channels.get(number.to_string()).is_some()
    {
        return Ok(number);
    }

    if let Some(label) = selector.as_str() {
        let matches: Vec<_> = channels
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, channel)| {
                channel["name"]
                    .as_str()
                    .is_some_and(|name| caseless::default_caseless_match_str(name, label))
            })
            .collect();

        if matches.len() == 1 {
            return matches[0]
                .0
                .parse::<u16>()
                .map_err(|error| CallError::invalid(error.to_string()));
        }
    }

    Err(CallError::invalid(format!(
        "{} has no {direction} channel {}",
        views::text(&record["name"]),
        views::text(selector)
    )))
}

fn current_route(record: &Value, number: u16) -> Option<&Value> {
    views::subscriptions(record)
        .find(|route| route["rx_channel_number"].as_u64() == Some(u64::from(number)))
}

fn source_text(route: Option<&Value>) -> String {
    route.map_or_else(
        || "nothing".to_owned(),
        |route| {
            format!(
                "{}:{}",
                views::text(&route["tx_device"]),
                views::text(&route["tx_channel"])
            )
        },
    )
}

fn route_effect(routes: &[Route]) -> String {
    let named = |source: &str| {
        let (device, channel) = source.split_once(':').unwrap_or((source, ""));
        format!("{channel} from {device}")
    };
    let sentences = routes
        .iter()
        .map(|route| {
            let after = route.requested();
            let receiver = &route.receiver_label;

            if route.before == after {
                let source = if after == "nothing" {
                    "nothing".to_owned()
                } else {
                    named(&after)
                };
                format!("{receiver} already hears {source}; no change")
            } else if route.before == "nothing" {
                format!("{receiver} would start hearing {}", named(&after))
            } else if after == "nothing" {
                format!(
                    "{receiver} would stop hearing {} and hear nothing",
                    named(&route.before)
                )
            } else {
                format!(
                    "{receiver} would stop hearing {} and hear {} instead",
                    named(&route.before),
                    named(&after)
                )
            }
        })
        .collect::<Vec<_>>();
    format!("{}.", sentences.join(". "))
}
