use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::flow_specification::{
    FlowType, MediaMode, RedundancyConstraint, TransmitFlowSpecification,
};

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowCreateRequest {
    pub protocol_id: Option<u16>,
    pub specification: TransmitFlowSpecification,
    pub device: FlowDeviceFacts,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowDeviceFacts {
    pub managed: bool,
    pub locked: Option<bool>,
    pub capability_word: Option<Value>,
    pub advertised_protocol: Option<u16>,
    pub protocol_version: Option<String>,
    pub sample_rate: Option<u32>,
    pub encoding: Option<u16>,
    pub channels: Option<Vec<u16>>,
    pub channel_capacity: Option<Value>,
    pub capabilities: std::collections::BTreeMap<String, Value>,
}

impl FlowDeviceFacts {
    fn reasons(&self, deletion: bool) -> Vec<String> {
        let mut reasons = Vec::new();
        if self
            .capability_word
            .as_ref()
            .and_then(Value::as_u64)
            .is_none()
        {
            reasons.push("transmit-flow authoring capability word is unavailable".into());
        }
        if self.managed {
            reasons.push(format!(
                "managed transmit-flow {} have no documented or independently observed transport",
                if deletion { "deletions" } else { "writes" }
            ));
        }
        match self.locked {
            Some(true) => reasons.push("device is locked".into()),
            None => reasons.push("device lock state is unknown".into()),
            Some(false) => {}
        }
        reasons
    }

    fn create_reasons(
        &self,
        request: &TransmitFlowSpecification,
        protocol: Option<u16>,
    ) -> Vec<String> {
        let mut reasons = self.reasons(false);
        for capability in &request.protocol.required_capabilities {
            let value = self.capabilities.get(capability);
            if value != Some(&Value::Bool(true)) {
                reasons.push(format!(
                    "required capability {capability:?} is {}",
                    if value.is_none_or(Value::is_null) {
                        "unknown"
                    } else {
                        "not advertised"
                    }
                ));
            }
        }
        if protocol.is_none() {
            reasons.push("flow protocol is unknown".into());
        }
        if self.advertised_protocol.is_some()
            && request.protocol.protocol_id.is_some()
            && self.advertised_protocol != protocol
        {
            reasons.push(
                "requested flow protocol does not match the device's advertised authoring protocol"
                    .into(),
            );
        }
        if let Some(required) = &request.protocol.protocol_version {
            match &self.protocol_version {
                None => reasons.push("device protocol version is unknown".into()),
                Some(observed) if observed != required => reasons.push(format!(
                    "required protocol version {required:?} does not match {observed:?}"
                )),
                _ => {}
            }
        }
        for (requested, observed, label) in [
            (
                request.sample_rate_hz.map(|v| v.get()),
                self.sample_rate,
                "sample rate",
            ),
            (
                request.encoding_bits.map(|v| u32::from(v.get())),
                self.encoding.map(u32::from),
                "encoding",
            ),
        ] {
            if let Some(requested) = requested {
                match observed {
                    None => reasons.push(format!("current device {label} is unknown")),
                    Some(current) if current != requested => reasons.push(format!(
                        "requested {label} differs from the current device-wide {label}"
                    )),
                    _ => {}
                }
            }
        }
        match &self.channels {
            None => reasons.push("transmitter channel inventory is unavailable".into()),
            Some(channels) => {
                let missing: std::collections::BTreeSet<_> = request
                    .channel_slots
                    .iter()
                    .map(|slot| slot.transmitter_channel.get())
                    .filter(|channel| !channels.contains(channel))
                    .collect();
                if !missing.is_empty() {
                    reasons.push(format!(
                        "transmitter channels are unavailable: {}",
                        missing
                            .iter()
                            .map(u16::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            }
        }
        if let Some(capacity) = self.channel_capacity.as_ref().and_then(Value::as_u64) {
            if request.channel_slots.len() as u64 > capacity {
                reasons.push(format!("requested channel-slot count exceeds the advertised audio transmit capacity of {capacity}"));
            }
        }
        reasons
    }
}

#[derive(Debug, Default, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FlowCommandPlan {
    pub reasons: Vec<String>,
    pub serializer_cohort: Option<&'static str>,
    pub wire_authored_fields: Vec<&'static str>,
    pub command: Option<serde_json::Map<String, Value>>,
}

impl FlowCommandPlan {
    fn reject(&mut self, reason: &str) {
        self.reasons.push(reason.to_owned());
    }

    fn validate_command(&mut self, command: Value) {
        if !self.reasons.is_empty() {
            return;
        }

        let Some(command) = command.as_object() else {
            self.reject("flow command must be an object");
            return;
        };

        // Validate the returned command with the real serializer without
        // assigning a transaction identifier in the caller's session.
        let mut validation = Value::Object(command.clone());
        validation["message_id"] = json!(1);

        match crate::spec::build_command_from_json(&validation.to_string()) {
            Ok(_) => self.command = Some(command.clone()),
            Err(error) => self.reject(&error.to_string()),
        }
    }
}

pub fn plan_create(input: &FlowCreateRequest) -> FlowCommandPlan {
    let mut plan = FlowCommandPlan {
        reasons: input
            .device
            .create_reasons(&input.specification, input.protocol_id),
        ..Default::default()
    };
    let request = &input.specification;

    if let Err(reason) = request.validate() {
        plan.reject(&reason);

        return plan;
    }

    if request.protocol.protocol_id.is_some() && request.protocol.protocol_id != input.protocol_id {
        plan.reject("requested flow protocol does not match the selected authoring protocol");
    }

    let destinations: Vec<_> = [&request.primary_destination, &request.secondary_destination]
        .into_iter()
        .flatten()
        .collect();
    let request_options_word = request
        .raw_fields
        .get("request_options_word")
        .filter(|value| !value.is_null());

    if !matches!(request.flow_type, FlowType::Multicast) {
        plan.reject("explicit unicast transmit-flow creation is unsupported by retained evidence");
    }

    if !matches!(request.redundancy, RedundancyConstraint::DeviceDefault)
        || destinations
            .iter()
            .any(|destination| destination.interface.is_some())
    {
        plan.reject(
            "the supported serializers do not encode an interface or redundancy constraint",
        );
    }

    if request
        .channel_slots
        .iter()
        .enumerate()
        .any(|(index, slot)| usize::from(slot.slot.get()) != index + 1)
    {
        plan.reject("flow channel slots must be contiguous starting at one");
    }

    let channels: Vec<u16> = request
        .channel_slots
        .iter()
        .map(|slot| slot.transmitter_channel.get())
        .collect();
    let command = match input.protocol_id {
        Some(crate::commands::PROTOCOL_DANTE_FLOW) => {
            plan.serializer_cohort = Some("legacy_2729_explicit_slot_multicast");
            plan.wire_authored_fields = vec!["identity.global_flow_id", "channel_slots"];

            if request.identity.global_flow_id.is_none() {
                plan.reject("legacy creation requires an explicit global flow identifier");
            }

            if request_options_word.is_some() {
                plan.reject("legacy creation does not accept request_options_word");
            }

            if !matches!(request.media_mode, MediaMode::NativeDante) {
                plan.reject("legacy creation supports only native Dante audio");
            }

            if request.identity.media_local_flow_id.is_some() {
                plan.reject("legacy creation does not encode a media-local flow identifier");
            }

            if request.name.is_some() {
                plan.reject("legacy creation does not encode a flow name");
            }

            if request.frames_per_packet.is_some() {
                plan.reject("legacy creation does not encode frames per packet");
            }

            if !destinations.is_empty() {
                plan.reject("legacy creation does not encode caller-selected destinations");
            }

            json!({"command": "create_tx_flow", "flow_protocol_id": input.protocol_id,
                "flow_slot": request.identity.global_flow_id, "channels": channels})
        }
        Some(crate::protocol::PROTOCOL_ARC_2809) => {
            plan.serializer_cohort = Some(if matches!(request.media_mode, MediaMode::RtpAes67) {
                "modern_2809_static_rtp_aes67"
            } else {
                "modern_2809_device_allocated_native"
            });
            plan.wire_authored_fields = vec![
                "media_mode",
                "identity.media_local_flow_id",
                "name",
                "channel_slots",
                "frames_per_packet",
                "primary_destination",
                "secondary_destination",
            ];

            if request.identity.global_flow_id.is_some() {
                plan.reject("ARC 2.8.9 allocation assigns the global flow identifier");
            }

            if !matches!(request.identity.media_type_code, None | Some(3)) {
                plan.reject("ARC 2.8.9 creation is scoped to audio media type 3");
            }

            if request.identity.media_local_flow_id.is_none() {
                plan.reject("ARC 2.8.9 creation requires an explicit media-local flow identifier");
            }

            if !matches!(
                request.protocol.cohort.as_deref(),
                None | Some("modern_2809")
            ) {
                plan.reject("requested protocol cohort does not match ARC 2.8.9");
            }

            let transport = match request.media_mode {
                MediaMode::NativeDante => "native",
                MediaMode::RtpAes67 => "rtp_aes67",
                MediaMode::Unknown => {
                    plan.reject("transmit-flow creation requires an explicit native Dante or RTP/AES67 media mode");
                    "native"
                }
            };
            let destinations: Vec<Value> = destinations
                .iter()
                .map(
                    |destination| json!({"address": destination.address, "port": destination.port}),
                )
                .collect();

            json!({"command": "create_multicast_flow_2809", "channels": channels,
                "media_local_flow_id": request.identity.media_local_flow_id, "transport": transport,
                "flow_name": request.name, "frames_per_packet": request.frames_per_packet.map_or(0, |value| value.get()),
                "destinations": destinations, "request_options_word": request_options_word.cloned().unwrap_or(json!(0))})
        }
        Some(crate::commands::PROTOCOL_DANTE_FLOW_2801) => {
            plan.reject("this revision has no digest-bound create request/acknowledgement fixture");
            Value::Null
        }
        _ => {
            plan.reject("flow protocol is unknown or unsupported for creation");
            Value::Null
        }
    };

    plan.validate_command(command);

    plan
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowDeleteRequest {
    pub protocol_id: Option<u16>,
    pub flow_id: u16,
    pub device: FlowDeviceFacts,
}

pub fn plan_delete(request: &FlowDeleteRequest) -> FlowCommandPlan {
    let mut plan = FlowCommandPlan {
        reasons: request.device.reasons(true),
        ..Default::default()
    };
    plan.serializer_cohort = match request.protocol_id {
        Some(crate::commands::PROTOCOL_DANTE_FLOW) => Some("legacy_2729_explicit_slot_delete"),
        Some(crate::protocol::PROTOCOL_ARC_2809) => Some("modern_2809_global_flow_2_delete"),
        Some(crate::commands::PROTOCOL_DANTE_FLOW_2801) => {
            plan.reject("this revision has no digest-bound delete request/acknowledgement fixture");
            None
        }
        _ => {
            plan.reject("flow protocol is unknown or unsupported for deletion");
            None
        }
    };
    plan.validate_command(
        json!({"command": "delete_tx_flow", "flow_protocol_id": request.protocol_id,
        "flow_slot": request.flow_id}),
    );

    plan
}
