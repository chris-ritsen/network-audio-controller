use super::audio_configuration::{AudioConfiguration, AudioKind};
use super::clock::{ClockOperation, ClockReadback};
use super::performance::{PerformanceOperation, PerformancePlan};
use super::record::DeviceRecord;
use super::subscriptions::outcome::SubscriptionFailure;
use super::transport::{CommandIntent, DeviceTransport, TransportError};
use netaudio_core::responses::parse_command_acknowledgement;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const IDENTIFY_DURATION_SECONDS: u8 = 6;

pub(crate) fn identify_event(reference: &str) -> Value {
    json!({
        "event": "identify_started",
        "server_name": reference,
        "duration": IDENTIFY_DURATION_SECONDS,
    })
}
const VALIDATION_HOST_ADDRESS: [u8; 6] = [0; 6];
const VALIDATION_MESSAGE_IDENTIFIER: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ChannelDirection {
    #[serde(rename = "rx")]
    Receiver,
    #[serde(rename = "tx")]
    Transmitter,
}

impl ChannelDirection {
    pub fn command_value(self) -> &'static str {
        match self {
            Self::Receiver => "rx",
            Self::Transmitter => "tx",
        }
    }

    fn inventory_field(self) -> &'static str {
        match self {
            Self::Receiver => "receivers",
            Self::Transmitter => "transmitters",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubscriptionChange {
    Set {
        #[serde(rename = "rx_channel")]
        receiver_channel: u16,
        #[serde(rename = "tx_channel")]
        transmitter_channel: String,
        #[serde(rename = "tx_device")]
        transmitter_device: String,
    },
    Clear {
        #[serde(rename = "rx_channel")]
        receiver_channel: u16,
    },
}

impl SubscriptionChange {
    pub fn receiver_channel(&self) -> u16 {
        match self {
            Self::Set {
                receiver_channel, ..
            }
            | Self::Clear { receiver_channel } => *receiver_channel,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ControlOperation {
    Panel(super::panels::Operation),
    Locking(super::locking::Operation),
    Maintenance(super::maintenance::Operation),
    SampleRate(super::sample_rate::Operation),
    MulticastPrefix(super::multicast_prefix::Configuration),
    InspectMulticastPrefix,
    FlowAuthoring(super::flow_authoring::Operation),
    StatusConfiguration(super::status_configuration::Configuration),
    NetworkConfiguration(super::network::Change),
    InspectStatus(super::registry::Inspection),
    RefreshExternalReceiver,
    ExternalSubscription(super::external_subscription::ExternalSubscription),
    AudioConfiguration(AudioConfiguration),
    RefreshAudio(AudioKind),
    RenameDevice {
        name: String,
    },
    RenameChannel {
        direction: ChannelDirection,
        number: u16,
        name: String,
    },
    SetLatency {
        milliseconds: f64,
    },
    Identify,
    Clock(ClockOperation),
    RefreshClock {
        profile: u16,
    },
    InspectTransmitFlows,
    Performance(PerformanceOperation),
    Subscriptions {
        changes: Vec<SubscriptionChange>,
    },
}

#[derive(Debug)]
pub enum ControlError {
    Managed {
        status: u16,
        result: Value,
    },
    Invalid(String),
    NotFound(String),
    Unavailable(String),
    ObservationUnavailable(String),
    Conflict(String),
    Subscription(SubscriptionFailure),
    Rejected {
        result_code: u16,
        acknowledged_commands: usize,
    },
    Transport {
        error: TransportError,
        acknowledged_commands: usize,
    },
    Internal(anyhow::Error),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Managed { result, .. } => formatter.write_str(
                result["error"]
                    .as_str()
                    .unwrap_or("Managed command outcome requires inspection before retrying"),
            ),
            Self::Invalid(message)
            | Self::NotFound(message)
            | Self::Unavailable(message)
            | Self::ObservationUnavailable(message)
            | Self::Conflict(message) => formatter.write_str(message),
            Self::Rejected {
                result_code,
                acknowledged_commands,
            } => {
                write!(
                    formatter,
                    "device rejected change with result {result_code}; {acknowledged_commands} preceding commands acknowledged"
                )
            }
            Self::Transport { error, .. } => error.fmt(formatter),
            Self::Subscription(failure) => failure.cause.fmt(formatter),
            Self::Internal(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ControlError {}

impl From<anyhow::Error> for ControlError {
    fn from(error: anyhow::Error) -> Self {
        if error.is::<crate::state::AmbiguousDevice>() {
            Self::Conflict(error.to_string())
        } else {
            Self::Internal(error)
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ControlCompletion {
    pub acknowledged_commands: usize,
    pub readback_completed: bool,
    pub readback_errors: Vec<String>,
    pub observation: Option<Value>,
}

pub enum ControlReadback {
    AudioConfiguration {
        operation: AudioConfiguration,
        request_sent: bool,
    },
    AudioStatus(AudioKind),
    Inventory,
    Latency {
        milliseconds: f64,
    },
    None,
    Performance(PerformancePlan),
    Clock(ClockReadback),
    TransmitFlows(Vec<u16>),
}

pub struct ControlPlan {
    commands: Vec<Value>,
    subscription_receivers: Option<Vec<Vec<u16>>>,
    channel_name_probe: Option<(Value, ChannelDirection)>,
    acknowledgement_required: bool,
    pub readback: ControlReadback,
    pub pending_events: Vec<Value>,
    pub acknowledgements: Vec<Vec<u8>>,
}

impl ControlOperation {
    pub fn precondition(&self, record: &Value) -> Value {
        let mut condition = json!({
            "name": record["name"],
            "online": record["online"],
            "is_locked": record["is_locked"],
            "mac_address": record["mac_address"],
            "ipv4": record["ipv4"],
        });

        if crate::managed::control::required(record) {
            condition["managed_target"] = json!({
                "server": record["ddm_server_profile"],
                "device": record["ddm_device_id"],
                "domain": record["ddm_domain_id"],
                "context": record["ddm_context"],
                "permissions": record["managed_operation_permissions"],
            });
            if matches!(self, Self::Clock(_)) {
                condition["managed_preferred_leader"] = record["preferred_leader"].clone();
            }
        }

        if let Self::Subscriptions { changes } = self {
            let numbers: std::collections::BTreeSet<_> = changes
                .iter()
                .map(SubscriptionChange::receiver_channel)
                .collect();
            let channels = record["channels"]["receivers"]
                .as_object()
                .into_iter()
                .flatten()
                .filter(|(number, _)| {
                    number
                        .parse::<u16>()
                        .is_ok_and(|number| numbers.contains(&number))
                })
                .map(|(number, channel)| {
                    let channel = if crate::managed::control::required(record) {
                        json!({"name": channel["name"], "ddm_channel_id": channel["ddm_channel_id"], "ddm_media_type": channel["ddm_media_type"], "managed_can_subscribe_self": channel["managed_can_subscribe_self"], "managed_can_subscribe_self_fresh": channel["managed_can_subscribe_self_fresh"]})
                    } else { channel.clone() };
                    (number.clone(), channel)
                })
                .collect::<serde_json::Map<_, _>>();
            let routes: Vec<_> = record["subscriptions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|route| {
                    route["rx_channel_number"].as_u64().is_some_and(|number| {
                        u16::try_from(number).is_ok_and(|number| numbers.contains(&number))
                    })
                })
                .map(|route| {
                    json!({
                        "receiver": route["rx_channel_number"],
                        "source": route["tx_device"],
                        "channel": route["tx_channel"],
                    })
                })
                .collect();
            condition["channels"] = json!(channels);
            condition["routes"] = json!(routes);
        }

        match self {
            Self::Panel(operation) => {
                condition["panel"] = operation.precondition(record);
            }
            Self::Locking(_) => {
                condition["managed"] = record["requires_managed_control"].clone();
                condition["locking_supported"] = record["device_locking_supported"].clone();
            }
            Self::Maintenance(_) => {
                condition["managed"] = record["requires_managed_control"].clone();
                if crate::managed::control::required(record) {
                    condition["can_reset"] = record["ddm_capabilities"]["can_reset"].clone();
                }
            }
            Self::SampleRate(operation) => {
                condition["sample_rate"] = operation.precondition(record);
            }
            Self::MulticastPrefix(configuration) => {
                condition["prefix"] = configuration.precondition(record);
            }
            Self::FlowAuthoring(operation) => {
                condition["flow_authoring"] = operation.precondition(record);
            }
            Self::NetworkConfiguration(operation) => {
                condition["network"] = operation.precondition(record);
            }
            Self::StatusConfiguration(operation) => {
                condition["configuration"] = operation.precondition(record);
            }
            Self::ExternalSubscription(operation) => {
                condition["external_subscription"] = operation.precondition(record);
            }
            Self::AudioConfiguration(operation) => {
                condition["audio"] = operation.precondition(record);
            }
            Self::Clock(operation) => {
                condition["clock"] = operation.precondition(record);
            }
            Self::RenameChannel {
                direction, number, ..
            } => {
                let channel = &record["channels"][direction.inventory_field()][number.to_string()];
                condition["channel"] = if crate::managed::control::required(record) {
                    json!({"name": channel["name"], "identity": channel["ddm_channel_id"], "media_type": channel["ddm_media_type"]})
                } else {
                    channel.clone()
                };
            }
            Self::SetLatency { .. } => {
                condition["configured_latency_ms"] = record["configured_latency_ms"].clone();
                condition["latency_ms"] = record["latency_ms"].clone();
                if crate::managed::control::required(record) {
                    condition["latency_settings"] = record["latency_settings"].clone();
                }
            }
            Self::Identify => {
                condition["identify_supported"] = record["identify_supported"].clone();
            }
            Self::Performance(_) => {
                for field in [
                    "settings_properties",
                    "performance_settings",
                    "platform_software_version",
                    "performance_operation_availability",
                    "requires_managed_control",
                ] {
                    condition[field] = record[field].clone();
                }
            }
            _ => {}
        }

        condition
    }

    pub fn plan(&self, record: &DeviceRecord) -> Result<ControlPlan, ControlError> {
        if !record.online {
            return Err(ControlError::Unavailable("device is offline".to_owned()));
        }

        if record.properties.get("is_locked").and_then(Value::as_bool) == Some(true)
            && !matches!(
                self,
                Self::Identify
                    | Self::Panel(_)
                    | Self::Locking(_)
                    | Self::SampleRate(_)
                    | Self::InspectMulticastPrefix
                    | Self::InspectStatus(_)
                    | Self::RefreshExternalReceiver
                    | Self::RefreshClock { .. }
                    | Self::InspectTransmitFlows
                    | Self::RefreshAudio(_)
            )
        {
            return Err(ControlError::Conflict("device is locked".to_owned()));
        }

        let performance = match self {
            Self::Performance(operation) => Some(operation.plan(record)?),
            _ => None,
        };
        let clock = match self {
            Self::Clock(operation) => Some(operation.plan(&record.presentation()?)?),
            _ => None,
        };
        let mut subscription_receivers = None;
        let (commands, pending_events) = match self {
            Self::Locking(operation) => {
                operation.validate(&record.presentation()?)?;
                (Vec::new(), Vec::new())
            }
            Self::Maintenance(operation) => {
                operation.validate(&record.presentation()?)?;
                (Vec::new(), Vec::new())
            }
            Self::Panel(_)
            | Self::SampleRate(_)
            | Self::FlowAuthoring(_)
            | Self::MulticastPrefix(_)
            | Self::InspectMulticastPrefix => (Vec::new(), Vec::new()),
            Self::NetworkConfiguration(operation) => (
                operation
                    .command(&record.presentation()?)?
                    .into_iter()
                    .collect(),
                Vec::new(),
            ),
            Self::StatusConfiguration(operation) => (
                operation
                    .command(&record.presentation()?)?
                    .into_iter()
                    .collect(),
                Vec::new(),
            ),
            Self::InspectStatus(_) | Self::RefreshExternalReceiver => (Vec::new(), Vec::new()),
            Self::ExternalSubscription(operation) => (vec![operation.command(record)?], Vec::new()),
            Self::RefreshAudio(_) => (Vec::new(), Vec::new()),
            Self::AudioConfiguration(operation) => (
                operation
                    .command(&record.presentation()?)?
                    .into_iter()
                    .collect(),
                Vec::new(),
            ),
            Self::RefreshClock { .. } | Self::InspectTransmitFlows => (Vec::new(), Vec::new()),
            Self::Clock(_) => {
                let plan = clock.as_ref().expect("clock plan prepared");
                let commands = if plan.changes.is_empty() {
                    Vec::new()
                } else {
                    vec![json!({"command": "clock_control", "control": plan.control})]
                };
                (commands, Vec::new())
            }
            Self::Performance(_) => (
                vec![
                    performance
                        .as_ref()
                        .expect("performance plan prepared")
                        .command
                        .clone(),
                ],
                Vec::new(),
            ),
            Self::RenameDevice { name } => {
                let command = if name.trim().is_empty() {
                    json!({"command": "reset_name"})
                } else {
                    netaudio_core::protocol::validate_dante_name(name)
                        .map_err(|error| ControlError::Invalid(error.to_string()))?;
                    json!({"command": "set_name", "name": name})
                };
                (vec![command], Vec::new())
            }
            Self::Identify => {
                match record
                    .properties
                    .get("identify_supported")
                    .and_then(Value::as_bool)
                {
                    Some(true) => {}
                    Some(false) => {
                        return Err(ControlError::Conflict(
                            "device does not support identification".to_owned(),
                        ));
                    }
                    None => {
                        return Err(ControlError::Unavailable(
                            "device identification capability is unavailable".to_owned(),
                        ));
                    }
                }

                (
                    vec![json!({"command": "identify"})],
                    vec![identify_event(&record.key)],
                )
            }
            Self::SetLatency { milliseconds } => {
                netaudio_core::latency_configuration::requested_nanoseconds(*milliseconds)
                    .map_err(|error| ControlError::Invalid(error.to_string()))?;
                (
                    vec![json!({"command": "set_latency", "latency": milliseconds})],
                    Vec::new(),
                )
            }
            Self::RenameChannel {
                direction,
                number,
                name,
            } => {
                let channels = match direction {
                    ChannelDirection::Receiver => &record.receivers,
                    ChannelDirection::Transmitter => &record.transmitters,
                };
                let channel = channels.get(number).ok_or_else(|| {
                    ControlError::NotFound(format!(
                        "{} channel {number} not found",
                        direction.command_value()
                    ))
                })?;

                if *direction == ChannelDirection::Receiver {
                    match channel.get("can_rename").and_then(Value::as_bool) {
                        Some(true) => {}
                        Some(false) => {
                            return Err(ControlError::Conflict(format!(
                                "receiver channel {number} rename capability prohibits renaming"
                            )));
                        }
                        None => {
                            return Err(ControlError::Unavailable(format!(
                                "receiver channel {number} rename capability is unavailable"
                            )));
                        }
                    }
                }

                let command = if name.trim().is_empty() {
                    json!({
                        "command": "reset_channel_name",
                        "channel_type": direction.command_value(),
                        "channel_number": number,
                    })
                } else {
                    let protocol = record.routing_protocol()?.ok_or_else(|| {
                        ControlError::Conflict("device has no ARC service metadata".to_owned())
                    })?;
                    json!({
                        "command": "set_channel_name",
                        "channel_type": direction.command_value(),
                        "channel_number": number,
                        "name": name,
                        "protocol_id": protocol.channel_name_probe_protocol_id,
                    })
                };
                (vec![command], Vec::new())
            }
            Self::Subscriptions { changes } => {
                let protocol = record.routing_protocol()?.ok_or_else(|| {
                    ControlError::Conflict("device has no ARC service metadata".to_owned())
                })?;

                for change in changes {
                    let receiver_channel = change.receiver_channel();
                    let channel = record.receivers.get(&receiver_channel).ok_or_else(|| {
                        ControlError::NotFound(format!("rx channel {receiver_channel} not found"))
                    })?;

                    if let SubscriptionChange::Set {
                        transmitter_device, ..
                    } = change
                    {
                        let self_connection = transmitter_device == "."
                            || transmitter_device.eq_ignore_ascii_case(&record.name)
                            || transmitter_device.eq_ignore_ascii_case(&record.key);

                        if self_connection {
                            match channel.get("can_subscribe_self").and_then(Value::as_bool) {
                                Some(true) => {}
                                Some(false) => {
                                    return Err(ControlError::Conflict(format!(
                                        "receiver channel {receiver_channel} does not support self-connections"
                                    )));
                                }
                                None => {
                                    return Err(ControlError::Unavailable(format!(
                                        "receiver channel {receiver_channel} self-connection capability is unavailable"
                                    )));
                                }
                            }
                        }
                    }
                }

                let groups = netaudio_core::spec::plan_subscription_command_groups(
                    &json!({
                        "protocol_id": protocol.protocol_id,
                        "channels": record.receivers.iter().map(|(number, channel)| json!({
                            "number": number,
                            "media_type_code": channel.get("media_type_code"),
                        })).collect::<Vec<_>>(),
                        "records": changes,
                    })
                    .to_string(),
                )
                .map_err(|error| ControlError::Invalid(error.to_string()))?;
                let (receivers, commands) = groups
                    .into_iter()
                    .map(|group| (group.receiver_channels, group.command))
                    .unzip();
                subscription_receivers = Some(receivers);
                let events = changes
                    .iter()
                    .map(|change| match change {
                        SubscriptionChange::Set {
                            receiver_channel,
                            transmitter_channel,
                            transmitter_device,
                        } => json!({
                            "event": "subscription_pending",
                            "action": "add",
                            "rx_device": record.name,
                            "rx_channel": receiver_channel,
                            "tx_channel": transmitter_channel,
                            "tx_device": transmitter_device,
                        }),
                        SubscriptionChange::Clear { receiver_channel } => json!({
                            "event": "subscription_pending",
                            "action": "remove",
                            "rx_device": record.name,
                            "rx_channel": receiver_channel,
                            "tx_channel": "",
                            "tx_device": "",
                        }),
                    })
                    .collect();
                (commands, events)
            }
        };

        for command in &commands {
            netaudio_core::spec::build_routed_command(
                &command.to_string(),
                Some(VALIDATION_HOST_ADDRESS),
                || VALIDATION_MESSAGE_IDENTIFIER,
            )
            .map_err(|error| ControlError::Invalid(error.to_string()))?;
        }

        let channel_name_probe = match self {
            Self::RenameChannel {
                direction, name, ..
            } if !name.trim().is_empty() => {
                let protocol = record.routing_protocol()?.ok_or_else(|| {
                    ControlError::Conflict("device has no ARC service metadata".to_owned())
                })?;
                let inventory = netaudio_core::channel_inventory::ChannelInventory::new(
                    direction.command_value(),
                    protocol.channel_name_probe_protocol_id,
                    1,
                )
                .map_err(|error| ControlError::Invalid(error.to_owned()))?;
                let netaudio_core::inventory::InventoryState::Pending { next_command, .. } =
                    inventory.state()
                else {
                    return Err(ControlError::Internal(anyhow::anyhow!(
                        "channel frontend probe has no initial command"
                    )));
                };
                Some((Value::Object(next_command), *direction))
            }
            _ => None,
        };
        let request_sent = !commands.is_empty();
        Ok(ControlPlan {
            commands,
            subscription_receivers,
            channel_name_probe,
            acknowledgement_required: !matches!(
                self,
                Self::Identify | Self::Clock(_) | Self::AudioConfiguration(_)
            ),
            readback: match self {
                Self::RefreshAudio(kind) => ControlReadback::AudioStatus(*kind),
                Self::AudioConfiguration(operation) => ControlReadback::AudioConfiguration {
                    operation: operation.clone(),
                    request_sent,
                },
                Self::InspectTransmitFlows => {
                    ControlReadback::TransmitFlows(super::flows::protocols(record)?)
                }
                Self::RefreshClock { profile } => ControlReadback::Clock(ClockReadback {
                    requested: Default::default(),
                    profile: *profile,
                    request_sent: false,
                }),
                Self::Clock(operation) => {
                    let plan = clock.expect("clock plan prepared");
                    ControlReadback::Clock(ClockReadback {
                        requested: plan.requested,
                        profile: operation.profile,
                        request_sent: !plan.changes.is_empty(),
                    })
                }
                Self::Performance(_) => {
                    ControlReadback::Performance(performance.expect("performance plan prepared"))
                }
                Self::Identify => ControlReadback::None,
                Self::SetLatency { milliseconds } => ControlReadback::Latency {
                    milliseconds: *milliseconds,
                },
                _ => ControlReadback::Inventory,
            },
            pending_events,
            acknowledgements: Vec::new(),
        })
    }
}

impl ControlPlan {
    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    pub async fn execute(
        &mut self,
        transport: &mut DeviceTransport,
    ) -> Result<usize, ControlError> {
        let outcome = self.execute_commands(transport).await;

        match (&self.subscription_receivers, outcome) {
            (Some(receivers), Err(error)) => Err(ControlError::Subscription(
                SubscriptionFailure::new(receivers, error),
            )),
            (_, outcome) => outcome,
        }
    }

    async fn execute_commands(
        &mut self,
        transport: &mut DeviceTransport,
    ) -> Result<usize, ControlError> {
        if let Some((query, direction)) = &self.channel_name_probe {
            let response = transport
                .execute(query, CommandIntent::Read)
                .await
                .map_err(|error| ControlError::Transport {
                    error,
                    acknowledged_commands: 0,
                })?;
            let protocol = netaudio_core::responses::parse_channel_name_protocol(
                &response,
                *direction == ChannelDirection::Receiver,
            )
            .ok_or(ControlError::Transport {
                error: TransportError::MalformedResponse("channel naming frontend probe"),
                acknowledged_commands: 0,
            })?;

            for command in &mut self.commands {
                command["protocol_id"] = json!(protocol);
            }
        }

        for (acknowledged_commands, command) in self.commands.iter().enumerate() {
            let response = transport
                .execute(command, CommandIntent::Write)
                .await
                .map_err(|error| ControlError::Transport {
                    error,
                    acknowledged_commands,
                })?;

            if !self.acknowledgement_required {
                continue;
            }

            self.acknowledgements.push(response.clone());
            let acknowledgement = parse_command_acknowledgement(&response).ok_or_else(|| {
                ControlError::Transport {
                    error: TransportError::WriteOutcomeUnknown(
                        "received an invalid acknowledgement; command was not retried".to_owned(),
                    ),
                    acknowledged_commands,
                }
            })?;

            if !acknowledgement.accepted {
                return Err(ControlError::Rejected {
                    result_code: acknowledgement.result_code,
                    acknowledged_commands,
                });
            }
        }

        Ok(if self.acknowledgement_required {
            self.commands.len()
        } else {
            0
        })
    }
}
