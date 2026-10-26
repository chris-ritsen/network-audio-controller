use crate::devices::operations::{
    ChannelDirection, ControlError, ControlOperation, SubscriptionChange,
};
use crate::devices::{
    analog, audio_configuration, clock, flow_authoring, network, network_configuration, panels,
    performance, redundancy, sample_rate, status_configuration,
};
use netaudio_core::presets::planning::{ActionKind, RequestedAction};
use serde_json::json;

pub fn operations(
    request: &RequestedAction,
    confirm_destructive: bool,
) -> Result<Vec<ControlOperation>, ControlError> {
    let value = &request.requested;
    let single = match request.kind {
        ActionKind::DeviceName => {
            let name = value
                .as_str()
                .ok_or_else(|| invalid("Device name must be a string."))?;
            netaudio_core::protocol::validate_dante_name(name)
                .map_err(|error| invalid(error.to_string()))?;
            ControlOperation::RenameDevice { name: name.into() }
        }
        ActionKind::RedundancyMode => ControlOperation::NetworkConfiguration(
            network::Change::Redundancy(redundancy::Configuration::new(value)?),
        ),
        ActionKind::SampleRate => ControlOperation::SampleRate(sample_rate::Operation::new(
            &json!({"sample_rate": value, "confirm_destructive": confirm_destructive}),
        )?),
        ActionKind::Encoding | ActionKind::SampleRatePullup => {
            ControlOperation::AudioConfiguration(audio_configuration::AudioConfiguration::new(
                if request.kind == ActionKind::Encoding {
                    audio_configuration::AudioKind::Encoding
                } else {
                    audio_configuration::AudioKind::SampleRatePullup
                },
                value,
            )?)
        }
        ActionKind::Latency => {
            let milliseconds = value
                .as_f64()
                .ok_or_else(|| invalid("Latency must be a number."))?;
            netaudio_core::latency_configuration::requested_nanoseconds(milliseconds)
                .map_err(|error| invalid(error.to_string()))?;
            ControlOperation::SetLatency { milliseconds }
        }
        ActionKind::ReceiveFlowPerformance
        | ActionKind::TransmitFlowPerformance
        | ActionKind::UnicastPerformance
        | ActionKind::ReceiveFlowDefaultSlots => {
            let mut requested = if request.kind == ActionKind::ReceiveFlowDefaultSlots {
                json!({"default_slots": value})
            } else {
                value.clone()
            };
            if !requested.is_object() {
                return Err(invalid("Performance configuration must be an object."));
            }
            requested["operation"] =
                serde_json::to_value(request.kind).map_err(anyhow::Error::from)?;
            let operation: performance::PerformanceOperation =
                serde_json::from_value(requested).map_err(|error| invalid(error.to_string()))?;
            ControlOperation::Performance(operation)
        }
        ActionKind::DeviceControl => {
            ControlOperation::Panel(panels::Operation::Apply(panels::Change {
                category: value["category"]
                    .as_str()
                    .ok_or_else(|| invalid("Panel category must be a string."))?
                    .into(),
                requested: value["requested"].clone(),
                confirm_clear: false,
            }))
        }
        ActionKind::ClockConfiguration => ControlOperation::Clock(clock::ClockOperation::new(
            value
                .as_object()
                .cloned()
                .ok_or_else(|| invalid("Clock configuration must be an object."))?,
            None,
        )?),
        ActionKind::TransmitterChannelNames | ActionKind::ReceiverChannelNames => {
            let direction = if request.kind == ActionKind::TransmitterChannelNames {
                ChannelDirection::Transmitter
            } else {
                ChannelDirection::Receiver
            };
            return value
                .as_object()
                .ok_or_else(|| invalid("Channel names must be an object."))?
                .iter()
                .map(|(number, name)| {
                    Ok(ControlOperation::RenameChannel {
                        direction,
                        number: number
                            .parse()
                            .map_err(|_| invalid("Channel number is invalid."))?,
                        name: name
                            .as_str()
                            .ok_or_else(|| invalid("Channel name must be a string."))?
                            .into(),
                    })
                })
                .collect();
        }
        ActionKind::TransmitFlow => {
            ControlOperation::FlowAuthoring(flow_authoring::Operation::create(value, false)?)
        }
        ActionKind::ReceiverSubscriptions => {
            let changes = value
                .as_object()
                .ok_or_else(|| invalid("Subscriptions must be an object."))?
                .iter()
                .map(|(number, source)| {
                    let receiver_channel = number
                        .parse()
                        .map_err(|_| invalid("Receiver channel is invalid."))?;
                    Ok(if source.is_null() {
                        SubscriptionChange::Clear { receiver_channel }
                    } else {
                        if source["kind"] != "native_dante" {
                            return Err(invalid("Unsupported receiver subscription kind."));
                        }
                        SubscriptionChange::Set {
                            receiver_channel,
                            transmitter_channel: source["tx_channel"]
                                .as_str()
                                .ok_or_else(|| invalid("Transmitter channel must be a string."))?
                                .into(),
                            transmitter_device: source["tx_device"]
                                .as_str()
                                .ok_or_else(|| invalid("Transmitter device must be a string."))?
                                .into(),
                        }
                    })
                })
                .collect::<Result<_, ControlError>>()?;
            ControlOperation::Subscriptions { changes }
        }
        ActionKind::CodecGain => ControlOperation::StatusConfiguration(
            status_configuration::Configuration::Analog(analog::Configuration::new(
                &value["channel"],
                &value["level"],
                &value["device_type"],
            )?),
        ),
        ActionKind::Interface => {
            if !matches!(value["identity"].as_str(), Some("primary" | "secondary")) {
                return Ok(Vec::new());
            }
            ControlOperation::NetworkConfiguration(network::Change::Interface(
                network_configuration::Configuration::new(&json!({
                    "interface": value["identity"], "mode": if value["mode"] == "dynamic" { json!("dhcp") } else { value["mode"].clone() },
                    "ip": value["ip_address"], "netmask": value["netmask"], "dns": value["dns_server"], "gateway": value["gateway"],
                }))?,
            ))
        }
        ActionKind::ExternalReceiverSubscriptions | ActionKind::Preserved => return Ok(Vec::new()),
    };
    Ok(vec![single])
}

fn invalid(message: impl Into<String>) -> ControlError {
    ControlError::Invalid(message.into())
}
