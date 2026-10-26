use super::operations::ControlError;
use super::record::DeviceRecord;
use crate::sap::inventory::Identity;
use crate::state::ReadModel;
use netaudio_core::spec::{ExternalSubscriptionPlanRequest, plan_external_subscription};
use serde_json::{Value, json};

pub const SOURCE_CHANGED: &str = "source announcement expired or changed; refresh the source";

#[derive(Clone, Debug)]
pub struct ExternalSubscription {
    pub identity: Identity,
    pub content_sha256: String,
    pub receiver_channel_ids: Vec<u16>,
    pub flow_slot_assignments: Vec<u16>,
    flow: Value,
}

impl ExternalSubscription {
    pub fn prepare(parameters: &Value, model: &ReadModel) -> Result<Self, ControlError> {
        let source_ipv4 = parameters["source_ipv4"]
            .as_str()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| invalid("source_ipv4 must be an IPv4 address"))?;
        let session_id = match &parameters["session_id"] {
            Value::String(value)
                if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) =>
            {
                value.parse().ok()
            }
            Value::Number(value) => value.as_u64(),
            _ => None,
        }
        .ok_or_else(|| {
            invalid("external session ID must be a decimal string in the unsigned 64-bit range")
        })?;
        let identity = Identity {
            source_ipv4,
            session_id,
        };
        let flow = model
            .fresh_external_flow(&identity)?
            .ok_or_else(|| ControlError::NotFound("external flow not found".to_owned()))?;
        let content_sha256 = flow["content_sha256"]
            .as_str()
            .ok_or_else(|| invalid("source announcement has no content digest"))?
            .to_owned();

        if let Some(expected) = parameters.get("content_sha256")
            && expected.as_str() != Some(&content_sha256)
        {
            return Err(ControlError::Conflict(SOURCE_CHANGED.to_owned()));
        }

        let channels = |field| {
            serde_json::from_value::<Vec<u16>>(parameters[field].clone()).map_err(|_| {
                invalid(
                    "receiver channels and flow-slot assignments must be lists of channel numbers",
                )
            })
        };
        Ok(Self {
            identity,
            content_sha256,
            receiver_channel_ids: channels("receiver_channel_ids")?,
            flow_slot_assignments: channels("flow_slot_assignments")?,
            flow,
        })
    }

    pub fn validate_source(&self, model: &ReadModel) -> Result<(), ControlError> {
        let current = model.fresh_external_flow(&self.identity)?;

        if current
            .as_ref()
            .is_none_or(|flow| flow["content_sha256"].as_str() != Some(&self.content_sha256))
        {
            return Err(ControlError::Conflict(SOURCE_CHANGED.to_owned()));
        }

        Ok(())
    }

    pub fn requested(&self) -> Value {
        json!({
            "source_ipv4": self.identity.source_ipv4,
            "session_id": self.identity.session_id.to_string(),
            "content_sha256": self.content_sha256,
            "receiver_channel_ids": self.receiver_channel_ids,
            "flow_slot_assignments": self.flow_slot_assignments,
        })
    }

    pub fn precondition(&self, record: &Value) -> Value {
        let mut result = self.requested();

        for field in [
            "sample_rate_hz",
            "encoding",
            "aes67_configuration_supported",
            "aes67_current",
            "switch_redundancy_supported",
            "requires_managed_control",
        ] {
            result[field] = record[field].clone();
        }

        result["channels"] = self.receiver_channel_ids.iter().map(|number| {
            json!({"number": number, "record": record["channels"]["receivers"][number.to_string()]})
        }).collect();
        result
    }

    pub fn command(&self, record: &DeviceRecord) -> Result<Value, ControlError> {
        if self.flow["routable"] != true {
            return Err(invalid(format!(
                "external flow is not routable: {}",
                self.flow["routability_errors"]
            )));
        }

        let record_value = record.presentation()?;

        if record_value["requires_managed_control"] == true {
            return Err(ControlError::Conflict(
                "direct external RTP subscription is unavailable for managed-only devices"
                    .to_owned(),
            ));
        }

        for number in &self.receiver_channel_ids {
            if !record.receivers.contains_key(number) {
                return Err(ControlError::NotFound(format!(
                    "receiver channel not found: {number}"
                )));
            }
        }

        let protocol = record
            .routing_protocol()?
            .ok_or_else(|| invalid("device has no ARC protocol metadata"))?;
        let request: ExternalSubscriptionPlanRequest = serde_json::from_value(json!({
            "device_protocol": protocol.protocol_id,
            "receiver": {
                "locked": record_value["is_locked"],
                "aes67_supported": record_value["aes67_configuration_supported"],
                "aes67_enabled": record_value["aes67_current"],
                "sample_rate": record_value["sample_rate_hz"],
                "encoding": record_value["encoding"],
                "redundancy_supported": record_value["switch_redundancy_supported"],
            },
            "source_sample_rate": self.flow["sample_rate"],
            "source_encoding": self.flow["encoding"],
            "source_direction": self.flow["direction"],
            "receiver_channel_ids": self.receiver_channel_ids,
            "flow_slot_assignments": self.flow_slot_assignments,
            "advertised_flow_slot_count": self.flow["channel_count"],
            "source_address": self.identity.source_ipv4,
            "session_id": self.identity.session_id,
            "clock_offset": self.flow["clock_offset"],
            "primary_destination": self.flow["primary_destination"],
            "secondary_address": self.flow["secondary_destination"]["address"],
            "secondary_port": self.flow["secondary_destination"]["port"],
            "receiver_supports_multiple_interfaces": record_value["switch_redundancy_supported"] == true,
        })).map_err(|error| invalid(error.to_string()))?;
        let command =
            plan_external_subscription(request).map_err(|error| invalid(error.to_string()))?;
        Ok(serde_json::to_value(command).map_err(anyhow::Error::from)?)
    }

    pub fn completion(
        &self,
        command: &Value,
        before: Option<Value>,
        after: Option<Value>,
        receipt: Option<&[u8]>,
        sent: bool,
    ) -> Result<Value, ControlError> {
        let acknowledgement =
            receipt.and_then(netaudio_core::responses::parse_command_acknowledgement);
        let acknowledged = acknowledgement
            .as_ref()
            .is_some_and(|receipt| receipt.accepted);
        let mut result = serde_json::to_value(
            netaudio_core::spec::external_subscription_readback(
                &json!({"kind": "command", "specification": command, "inventory": after})
                    .to_string(),
            )
            .map_err(|error| invalid(error.to_string()))?,
        )
        .map_err(anyhow::Error::from)?;
        result["result_code"] = json!(acknowledgement.map(|receipt| receipt.result_code));
        result["request_acknowledged"] = json!(acknowledged);
        result["mutation_sent"] = json!(sent);
        result["message"] = json!(if !sent {
            "complete fresh receiver-flow baseline was unavailable; no request was sent"
        } else if !acknowledged {
            "external receiver subscription was not acknowledged"
        } else if result["arc_effective_state_confirmed"] == true {
            "external receiver subscription confirmed by complete fresh ARC readback"
        } else if result["arc_effective_state_confirmed"] == false {
            "request acknowledged but fresh ARC readback contradicted the requested identity"
        } else {
            "request acknowledged but complete fresh ARC readback was unavailable"
        });

        for field in [
            "rtp_packet_reception_confirmed",
            "clock_lock_confirmed",
            "persistence_confirmed",
            "decoded_audio_confirmed",
        ] {
            result[field] = Value::Null;
        }

        result["flow_identity"] = json!({"source_ipv4": self.identity.source_ipv4, "session_id": self.identity.session_id.to_string()});
        result["receiver_channel_ids"] = json!(self.receiver_channel_ids);
        result["flow_slot_assignments"] = json!(self.flow_slot_assignments);
        result["receiver_flow_before"] = json!(before);
        result["receiver_flow_after"] = json!(after);
        Ok(result)
    }
}

fn invalid(message: impl Into<String>) -> ControlError {
    ControlError::Invalid(message.into())
}

pub enum Outcome {
    Confirmed,
    Pending,
    Rejected,
    Contradicted,
    Unavailable,
}

impl Outcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Confirmed | Self::Pending)
    }
}

pub fn response(mut result: Value) -> (Outcome, Value) {
    let status = if result["mutation_sent"] == false {
        Outcome::Unavailable
    } else if result["request_acknowledged"] != true {
        if result["result_code"].is_null() {
            Outcome::Unavailable
        } else {
            Outcome::Rejected
        }
    } else if result["arc_effective_state_confirmed"] == false {
        Outcome::Contradicted
    } else if result["arc_effective_state_confirmed"] == true {
        Outcome::Confirmed
    } else {
        Outcome::Pending
    };

    if status.is_success() {
        result["success"] = json!(true);
    } else {
        result["error"] = result["message"].clone();
    }

    crate::representation::browser_values(&mut result);
    (status, result)
}
