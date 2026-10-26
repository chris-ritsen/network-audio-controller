use super::{
    NEXT_STEP, Prepared, PreparedDevice, PreparedKind, channel_number, require_record,
    required_text,
};
use crate::devices::audio_configuration::{AudioConfiguration, AudioKind, parse_pullup_selection};
use crate::devices::clock::{ClockOperation, parse_subdomain};
use crate::devices::operations::{
    ChannelDirection, ControlCompletion, ControlError, ControlOperation,
};
use crate::devices::performance::PerformanceOperation;
use crate::mcp::service::CallError;
use crate::mcp::views::{self, Records};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

pub struct PreparedSetting {
    reference: String,
    device: String,
    operation: String,
    change: Value,
    observation: Observation,
}

enum Observation {
    MulticastPrefix,
    Aes67,
    Analog(u16),
    Network,
    Redundancy,
    Audio(AudioKind),
    Channel {
        direction: &'static str,
        number: u16,
        requested: String,
    },
    Latency,
    Identify,
    Performance,
    Clock(BTreeMap<String, String>),
}

pub fn requires_clock(operation: &str) -> bool {
    matches!(
        operation,
        "configure_clock" | "set_clock_source" | "set_clock_subdomain" | "set_preferred_leader"
    )
}

pub fn supports(operation: &str) -> bool {
    audio_kind(operation).is_some()
        || requires_clock(operation)
        || matches!(
            operation,
            "set_gain"
                | "set_redundancy"
                | "set_interface"
                | "set_aes67"
                | "set_aes67_multicast_prefix"
                | "rename_channel"
                | "set_latency"
                | "identify"
                | "configure_flow_performance"
                | "set_receive_flow_default_slots"
                | "store_current_configuration"
        )
}

pub fn audio_kind(operation: &str) -> Option<AudioKind> {
    match operation {
        "set_encoding" => Some(AudioKind::Encoding),
        "set_sample_rate_pullup" => Some(AudioKind::SampleRatePullup),
        _ => None,
    }
}

pub fn prepare(operation: &str, request: &Value, records: &Records) -> Result<Prepared, CallError> {
    let record = require_record(records, &request["device"])?;
    let reference = required_text(record, "server_name")?.to_owned();
    let device = required_text(record, "name")?.to_owned();
    let mut warnings = Vec::new();
    let mut preview = json!({
        "changed": false,
        "confirmation_required": true,
        "operation": operation,
        "device": device,
        "next": NEXT_STEP,
    });
    let (control, observation, change) = match operation {
        "set_gain" => {
            let direction = required_text(request, "device_type")?;
            let group = match direction {
                "input" => "transmitters",
                "output" => "receivers",
                _ => return Err(CallError::invalid("direction must be input or output")),
            };
            let channel = channel_number(record, &request["channel"], group)?;
            let configuration = crate::devices::analog::Configuration::new(
                &json!(channel),
                &request["gain_level"],
                &request["device_type"],
            )
            .map_err(|error| CallError::invalid(error.to_string()))?;
            configuration
                .command(record)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            (
                ControlOperation::StatusConfiguration(
                    crate::devices::status_configuration::Configuration::Analog(configuration),
                ),
                Observation::Analog(channel),
                json!({"level": {"from": record["gain_adapter"]["channel_levels"].get(usize::from(channel - 1)), "to": request["gain_level"]}, "channel": channel, "direction": direction}),
            )
        }
        "set_redundancy" => {
            let configuration = crate::devices::redundancy::Configuration::new(&request["mode"])
                .map_err(|error| CallError::invalid(error.to_string()))?;
            configuration
                .command(record)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            preview["effect"] = json!(
                "Changes the configured Dante network mode. A reboot may be required and is not performed."
            );
            (
                ControlOperation::NetworkConfiguration(
                    crate::devices::network::Change::Redundancy(configuration),
                ),
                Observation::Redundancy,
                json!({"mode": {"from": record["dante_redundancy"]["configured"], "to": request["mode"]}}),
            )
        }
        "set_interface" => {
            let configuration = crate::devices::network_configuration::Configuration::new(request)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            configuration
                .command(record)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            preview["effect"] = json!(
                "Changes network configuration. An address change can make the device unreachable; a reboot may be required and is not performed."
            );
            let change = json!({"interface": configuration.interface, "configuration": {"from": configuration.selected(&record["interfaces"]).map(|selected| &selected["configured"]), "to": configuration.requested()["configuration"]}});
            (
                ControlOperation::NetworkConfiguration(crate::devices::network::Change::Interface(
                    configuration,
                )),
                Observation::Network,
                change,
            )
        }
        "set_aes67_multicast_prefix" => {
            let configuration =
                crate::devices::multicast_prefix::Configuration::new(&request["prefix"])
                    .map_err(|error| CallError::invalid(error.to_string()))?;
            configuration
                .command(record)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            (
                ControlOperation::MulticastPrefix(configuration),
                Observation::MulticastPrefix,
                json!({"prefix": {"from": record["aes67_multicast_prefix"], "to": request["prefix"]}}),
            )
        }
        "set_aes67" => {
            let configuration = crate::devices::aes67::Configuration::new(&request["enabled"])
                .map_err(|error| CallError::invalid(error.to_string()))?;
            preview["effect"] = json!(
                "Changes AES67 configuration; the device may require a reboot before it takes effect."
            );
            (
                ControlOperation::StatusConfiguration(
                    crate::devices::status_configuration::Configuration::Aes67(configuration),
                ),
                Observation::Aes67,
                json!({"enabled": {"from": record["aes67_configured"], "to": request["enabled"]}}),
            )
        }
        name if audio_kind(name).is_some() => {
            let kind = audio_kind(name).expect("audio operation matched");
            let value = if matches!(kind, AudioKind::SampleRatePullup) {
                parse_pullup_selection(&request["value"])
                    .map_err(|error| CallError::invalid(error.to_string()))?
            } else {
                request[kind.parameter()].clone()
            };
            let configuration = AudioConfiguration::new(kind, &value)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let label = audio_parameter(kind);
            (
                ControlOperation::AudioConfiguration(configuration),
                Observation::Audio(kind),
                json!({label: {
                    "from": audio_value(kind, &record[kind.current_field()]),
                    "to": audio_value(kind, &value),
                }}),
            )
        }
        name if requires_clock(name) && crate::managed::control::required(record) => {
            let changes = match name {
                "set_preferred_leader" => {
                    Map::from_iter([("preferred_leader".into(), request["preferred"].clone())])
                }
                "configure_clock" => request["changes"]
                    .as_object()
                    .cloned()
                    .ok_or_else(|| CallError::invalid("changes must be an object"))?,
                _ => {
                    return Err(CallError::invalid(
                        "Managed clock control currently supports preferred leader only",
                    ));
                }
            };
            let operation = ClockOperation::new(changes, None)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let control = ControlOperation::Clock(operation);
            let command = crate::managed::control::plan(record, &control)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let netaudio_core::managed_api::control::Control::PreferredLeader { enabled } = command
            else {
                unreachable!()
            };
            let label = if name == "set_preferred_leader" {
                "preferred"
            } else {
                "preferred_leader"
            };
            (
                control,
                Observation::Clock(BTreeMap::from([(label.into(), "preferred_leader".into())])),
                json!({label: {"from": record["preferred_leader"], "to": enabled}}),
            )
        }
        name if requires_clock(name) => {
            let changes = match name {
                "configure_clock" => request["changes"]
                    .as_object()
                    .cloned()
                    .ok_or_else(|| CallError::invalid("changes must be an object"))?,
                "set_clock_source" => {
                    Map::from_iter([("clock_source".to_owned(), request["clock_source"].clone())])
                }
                "set_preferred_leader" => {
                    Map::from_iter([("preferred_leader".to_owned(), request["preferred"].clone())])
                }
                "set_clock_subdomain" => {
                    let subdomain = parse_subdomain(required_text(request, "subdomain")?)
                        .map_err(|error| CallError::invalid(error.to_string()))?;
                    Map::from_iter([("subdomain".to_owned(), subdomain)])
                }
                _ => unreachable!(),
            };
            let operation = ClockOperation::new(changes, None)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let plan = operation
                .plan(record)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let mut change = Map::new();
            let mut fields = BTreeMap::new();

            for (field, requested) in &plan.requested {
                let status_field = match field.as_str() {
                    "clock_source" => "clock_source_code",
                    "subdomain" => "clock_subdomain",
                    "ports" => "extended_ports",
                    _ => field,
                };
                let label = if name == "set_preferred_leader" {
                    "preferred"
                } else {
                    field
                };
                change.insert(
                    label.to_owned(),
                    json!({
                        "from": clock_value(status_field, &plan.before[field]),
                        "to": clock_value(status_field, requested),
                    }),
                );
                fields.insert(label.to_owned(), status_field.to_owned());
            }

            (
                ControlOperation::Clock(operation),
                Observation::Clock(fields),
                Value::Object(change),
            )
        }
        "identify" => {
            preview["effect"] = json!(format!(
                "{device}'s identify light flashes so someone can find it; audio is not affected"
            ));
            (ControlOperation::Identify, Observation::Identify, json!({}))
        }
        "rename_channel" => {
            let direction: ChannelDirection =
                serde_json::from_value(request["channel_type"].clone())
                    .map_err(|error| CallError::invalid(error.to_string()))?;
            let field = match direction {
                ChannelDirection::Receiver => "receivers",
                ChannelDirection::Transmitter => "transmitters",
            };
            let number = channel_number(record, &request["channel"], field)?;
            let before = views::channel_label(&record["channels"][field][number.to_string()]);
            let name = required_text(request, "name")?.to_owned();

            if direction == ChannelDirection::Transmitter {
                let routes: Vec<_> = records
                    .values()
                    .flat_map(|receiver| {
                        let before = &before;
                        let device = &device;
                        views::subscriptions(receiver)
                            .filter(move |route| {
                                route["tx_device"] == *device && route["tx_channel"] == *before
                            })
                            .map(|route| {
                                format!(
                                    "{}:{}",
                                    views::text(&receiver["name"]),
                                    views::text(&route["rx_channel"])
                                )
                            })
                    })
                    .collect();

                if !routes.is_empty() {
                    warnings.push(format!(
                        "{} route(s) subscribe to this channel by name and will go unresolved: {}",
                        routes.len(),
                        routes.join("; ")
                    ));
                }
            }

            let label = format!("{} channel {number}", direction.command_value());
            let change = json!({label: {
                "from": before,
                "to": if name.trim().is_empty() { "the device default" } else { &name },
            }});
            (
                ControlOperation::RenameChannel {
                    direction,
                    number,
                    name: name.clone(),
                },
                Observation::Channel {
                    direction: field,
                    number,
                    requested: name,
                },
                change,
            )
        }
        "set_latency" => {
            let milliseconds = request["latency"]
                .as_f64()
                .ok_or_else(|| CallError::invalid("latency must be a number"))?;
            let before = record
                .get("configured_latency_ms")
                .filter(|value| !value.is_null())
                .unwrap_or(&record["latency_ms"]);

            if let Some(choices) = record["standard_latency_choices_ms"].as_array()
                && !choices.is_empty()
                && !choices
                    .iter()
                    .any(|value| value.as_f64() == Some(milliseconds))
            {
                warnings.push(format!(
                    "{device} supports latency values {}; {milliseconds} will be rejected",
                    record["standard_latency_choices_ms"]
                ));
            }

            if before.as_f64() == Some(milliseconds) {
                warnings.push(format!(
                    "{device} already has latency {milliseconds}; nothing would change"
                ));
            }

            (
                ControlOperation::SetLatency { milliseconds },
                Observation::Latency,
                json!({"latency": {"from": milliseconds_label(before), "to": format!("{milliseconds} ms")}}),
            )
        }
        _ => {
            let kind = match operation {
                "configure_flow_performance" => match request["kind"].as_str() {
                    Some("receive") => "receive_flow_performance",
                    Some("transmit") => "transmit_flow_performance",
                    Some("unicast") => "unicast_performance",
                    _ => {
                        return Err(CallError::invalid(
                            "kind must be receive, transmit or unicast",
                        ));
                    }
                },
                "set_receive_flow_default_slots" => "receive_flow_default_slots",
                "store_current_configuration" => "store_current_configuration",
                _ => return Err(CallError::invalid("unsupported configuration operation")),
            };
            let mut specification = json!({"operation": kind});

            for field in ["latency_microseconds", "frames_per_packet", "default_slots"] {
                if let Some(value) = request.get(field) {
                    specification[field] = value.clone();
                }
            }

            let control: PerformanceOperation = serde_json::from_value(specification)
                .map_err(|error| CallError::invalid(error.to_string()))?;
            let current = performance_snapshot(record)?;
            let change = if operation == "configure_flow_performance" {
                let previous = &current[kind];
                let latency = request["latency_microseconds"]
                    .as_f64()
                    .map(|value| value / 1000.0);
                let before = previous["latency_microseconds"]
                    .as_f64()
                    .map(|value| value / 1000.0);
                json!({format!("{} flows", views::text(&request["kind"])): {
                    "latency": {
                        "from": before.map(|value| format!("{value} ms")).unwrap_or_else(|| "unknown".to_owned()),
                        "to": latency.map(|value| format!("{value} ms")),
                    },
                    "frames_per_packet": {
                        "from": previous.get("frames_per_packet").map(views::text).unwrap_or_else(|| "unknown".to_owned()),
                        "to": views::text(&request["frames_per_packet"]),
                    },
                }})
            } else if operation == "set_receive_flow_default_slots" {
                json!({"default_slots": {
                    "from": current.get(kind).cloned().unwrap_or_else(|| json!("unknown")),
                    "to": request["default_slots"],
                }})
            } else {
                preview["effect"] = json!(format!(
                    "{device} saves its current settings so it starts with them after a reboot; audio is not affected"
                ));
                json!({})
            };
            (
                ControlOperation::Performance(control),
                Observation::Performance,
                change,
            )
        }
    };

    if change.as_object().is_some_and(|change| !change.is_empty()) {
        preview["change"] = change.clone();
    }

    if !warnings.is_empty() {
        preview["warnings"] = json!(warnings);
    }

    let precondition = control.precondition(record);
    Ok(Prepared {
        preview,
        devices: vec![PreparedDevice {
            reference: reference.clone(),
            operation: control,
            precondition,
        }],
        kind: PreparedKind::Setting(PreparedSetting {
            reference,
            device,
            operation: operation.to_owned(),
            change,
            observation,
        }),
    })
}

impl PreparedSetting {
    pub fn outcome(
        mut self,
        outcomes: &BTreeMap<String, Result<ControlCompletion, ControlError>>,
        records: &Records,
    ) -> Result<(Value, bool), CallError> {
        let execution = outcomes
            .get(&self.reference)
            .ok_or_else(|| CallError::invalid("configuration execution result is missing"))?;
        let completion = execution.as_ref().ok();
        let observation = completion.and_then(|completion| completion.observation.as_ref());
        let record = views::resolve(records, &self.reference)?;
        let mut failed = execution.is_err();
        let mut verified = false;

        if matches!(
            self.observation,
            Observation::Aes67
                | Observation::MulticastPrefix
                | Observation::Channel { .. }
                | Observation::Analog(_)
                | Observation::Network
                | Observation::Redundancy
                | Observation::Audio(_)
        ) && let Some(result) = crate::managed::control::outcome(execution)
        {
            return Ok((result.clone(), failed));
        }

        match self.observation {
            Observation::MulticastPrefix => {
                verified =
                    observation.is_some_and(|observed| observed["configuration_confirmed"] == true);
                failed |= !verified;
                self.change["prefix"]["now"] =
                    observation.map_or(Value::Null, |observed| observed["prefix"].clone());
            }
            Observation::Analog(channel) => {
                verified = observation
                    .is_some_and(|observed| observed["effective_state_confirmed"] == true);
                failed |= !verified;
                self.change["level"]["now"] = observation
                    .and_then(|observed| {
                        observed["status"]["channel_levels"].get(usize::from(channel - 1))
                    })
                    .cloned()
                    .unwrap_or(Value::Null);
            }
            Observation::Redundancy => {
                if let Some(observed) = observation {
                    return Ok((
                        observed.clone(),
                        observed["configuration_confirmed"] != true,
                    ));
                }
            }
            Observation::Network => {
                verified =
                    observation.is_some_and(|observed| observed["configuration_confirmed"] == true);
                failed |= !verified;
                self.change["configuration"]["now"] =
                    observation.map_or(Value::Null, |observed| observed["configured"].clone());
            }
            Observation::Aes67 => {
                verified =
                    observation.is_some_and(|observed| observed["configuration_confirmed"] == true);
                failed |= !verified;
                self.change["enabled"]["now"] =
                    observation.map_or(Value::Null, |observed| observed["configured"].clone());
            }
            Observation::Audio(kind) => {
                let value = observation
                    .map(|observed| &observed["current_value"])
                    .unwrap_or(&Value::Null);
                self.change[audio_parameter(kind)]["now"] = audio_value(kind, value);
                verified = observation
                    .is_some_and(|observed| observed["effective_state_confirmed"] == true);
                failed |= !verified;
            }
            Observation::Clock(fields) => {
                for (label, field) in fields {
                    self.change[&label]["now"] = observation
                        .map(|observed| clock_value(&field, &observed["status"][&field]))
                        .unwrap_or(Value::Null);
                }

                verified = observation
                    .is_some_and(|observed| observed["effective_state_confirmed"] == true);
                failed |= !verified;
            }
            Observation::Performance => {
                if let Some(observed) = observation {
                    failed = matches!(
                        observed["state"].as_str(),
                        Some("contradicted" | "rejected" | "unverified")
                    );
                    return Ok((observed.clone(), failed));
                }
            }
            Observation::Identify => {
                if let Some(result) = crate::managed::control::outcome(execution) {
                    return Ok((result.clone(), failed));
                }
                if !failed {
                    return Ok((json!({"accepted": true, "verified": false}), false));
                }
            }
            Observation::Latency => {
                if let Some(result) = crate::managed::control::outcome(execution) {
                    return Ok((result.clone(), failed));
                }
                let now = observation
                    .map(|observed| observed["configured_latency_ms"].clone())
                    .unwrap_or(Value::Null);
                self.change["latency"]["now"] = milliseconds_label(&now);
                verified = observation
                    .is_some_and(|observed| observed["effective_state_confirmed"] == true);
                failed |= !verified;
            }
            Observation::Channel {
                direction,
                number,
                requested,
            } => {
                let channel =
                    record.map(|record| &record["channels"][direction][number.to_string()]);
                let now = channel.map(views::channel_label).unwrap_or(Value::Null);
                verified = !requested.trim().is_empty() && now.as_str() == Some(requested.as_str());

                for entry in self
                    .change
                    .as_object_mut()
                    .into_iter()
                    .flat_map(|change| change.values_mut())
                {
                    entry["now"] = now.clone();
                }
            }
        }

        let mut result = json!({
            "changed": !failed || verified,
            "operation": self.operation,
            "device": self.device,
            "change": self.change,
            "verified": verified,
        });

        if requires_clock(&self.operation)
            || audio_kind(&self.operation).is_some()
            || self.operation == "set_gain"
        {
            for field in ["request_sent", "request_acknowledged", "persistence"] {
                result[field] = observation
                    .map(|observed| observed[field].clone())
                    .unwrap_or(Value::Null);
            }

            if result["request_sent"] == false {
                result["changed"] = json!(false);
            }
        }

        if self.operation == "set_aes67_multicast_prefix" {
            for field in [
                "request_sent",
                "request_acknowledgement",
                "configuration_confirmed",
                "persistence_confirmed",
            ] {
                result[field] = observation.map_or(Value::Null, |observed| observed[field].clone());
            }

            if result["request_sent"] == false {
                result["changed"] = json!(false);
            }
        }

        if matches!(self.operation.as_str(), "set_aes67" | "set_interface") {
            for field in [
                "request_sent",
                "request_acknowledged",
                "configuration_confirmed",
                "effective_state_confirmed",
                "current",
                "configured",
                "reboot_required",
                "persistence_confirmed",
            ] {
                result[field] = observation.map_or(Value::Null, |observed| observed[field].clone());
            }

            if result["request_sent"] == false {
                result["changed"] = json!(false);
            }
        }

        if let Err(error) = execution {
            result["error"] = json!(error.to_string());
        } else if failed {
            result["error"] =
                json!("the requested configuration was not confirmed by fresh readback");
        }

        if failed || !verified {
            result["note"] = json!(
                "Read the observed values before retrying; the device may have accepted the request without confirming its result."
            );
        }

        let result = if matches!(
            self.operation.as_str(),
            "set_aes67" | "set_interface" | "set_gain" | "set_aes67_multicast_prefix"
        ) {
            result
        } else {
            views::compact(result)
        };
        Ok((result, failed))
    }
}

fn audio_parameter(kind: AudioKind) -> &'static str {
    match kind {
        AudioKind::Encoding => "encoding",
        AudioKind::SampleRatePullup => "value",
    }
}

fn audio_value(kind: AudioKind, value: &Value) -> Value {
    let Some(number) = value.as_u64() else {
        return Value::Null;
    };

    match kind {
        AudioKind::Encoding => json!(format!("{number}-bit")),
        AudioKind::SampleRatePullup => netaudio_core::capabilities::sample_rate_pullup_choices()
            .into_iter()
            .find(|choice| u64::from(choice.value) == number)
            .map_or_else(|| json!(number), |choice| json!(choice.label)),
    }
}

fn clock_value(field: &str, value: &Value) -> Value {
    if field == "clock_subdomain" && !value.is_null() {
        json!(netaudio_core::clock_configuration::subdomain_presentation(value).label)
    } else {
        value.clone()
    }
}

fn milliseconds_label(value: &Value) -> Value {
    value
        .as_f64()
        .map_or(Value::Null, |value| json!(format!("{value} ms")))
}

fn performance_snapshot(record: &Value) -> Result<Value, CallError> {
    crate::devices::performance::snapshot(record)
        .map_err(|error| CallError::invalid(error.to_string()))
}
