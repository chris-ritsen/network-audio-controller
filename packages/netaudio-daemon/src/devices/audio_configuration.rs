use super::operations::ControlError;
use crate::monitoring::requests::StatusQuery;
use netaudio_core::capabilities::{AvailabilityRequest, availability, sample_rate_pullup_choices};
use netaudio_core::responses::AudioCapabilityReadback;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioKind {
    Encoding,
    SampleRatePullup,
}

impl AudioKind {
    pub fn query(self) -> StatusQuery {
        match self {
            Self::Encoding => StatusQuery::Encoding,
            Self::SampleRatePullup => StatusQuery::Pullup,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Encoding => "encoding",
            Self::SampleRatePullup => "sample_rate_pullup",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Encoding => "encoding",
            Self::SampleRatePullup => "sample-rate pull-up",
        }
    }

    pub fn parameter(self) -> &'static str {
        match self {
            Self::Encoding => "encoding",
            Self::SampleRatePullup => "raw_value",
        }
    }

    pub fn current_field(self) -> &'static str {
        match self {
            Self::Encoding => "encoding",
            Self::SampleRatePullup => "sample_rate_pullup_raw_value",
        }
    }

    pub fn deadline(self) -> Duration {
        match self {
            Self::Encoding => Duration::from_secs(20),
            Self::SampleRatePullup => Duration::from_secs(4),
        }
    }

    pub fn status(self, record: &Value) -> Value {
        let (requested, supported) = match self {
            Self::Encoding => ("requested_encoding", "supported_encodings"),
            Self::SampleRatePullup => (
                "requested_sample_rate_pullup_raw_value",
                "supported_sample_rate_pullup_raw_values",
            ),
        };
        let mut status = json!({
            "current_value": record[self.current_field()],
            "requested_value": record[requested],
            "available_values": record[supported],
            "update_mode": record[format!("{}_update_mode", self.name())],
        });

        if matches!(self, Self::SampleRatePullup) {
            status["host_disabled"] = record["sample_rate_pullup_host_disabled"].clone();
            status["flags"] = record["sample_rate_pullup_flags"].clone();
        }

        status
    }
}

#[derive(Clone, Debug)]
pub struct AudioConfiguration {
    pub kind: AudioKind,
    pub requested: u32,
}

impl AudioConfiguration {
    pub fn new(kind: AudioKind, value: &Value) -> Result<Self, ControlError> {
        let minimum = u32::from(matches!(kind, AudioKind::Encoding));
        let requested = value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value >= minimum)
            .ok_or_else(|| {
                ControlError::Invalid(format!(
                    "{} must be an integer from {minimum} through {}",
                    kind.parameter(),
                    u32::MAX
                ))
            })?;
        Ok(Self { kind, requested })
    }

    pub fn precondition(&self, record: &Value) -> Value {
        json!({
            "status": self.kind.status(record),
            "supported": record[format!("{}_configuration_supported", self.kind.name())],
            "managed": record["requires_managed_control"],
        })
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        let status = self.kind.status(record);
        let facts: AvailabilityRequest = serde_json::from_value(json!({
            "operation": self.kind,
            "supported": record[format!("{}_configuration_supported", self.kind.name())],
            "readable": status["current_value"].is_u64(),
            "locked": record["is_locked"],
            "audio": {
                "update_mode": status["update_mode"],
                "available_values": status["available_values"],
                "requested_value": self.requested,
                "host_disabled": status["host_disabled"],
            },
            "transport_available": true,
            "has_adapter": false,
            "managed": record["requires_managed_control"] == true,
            "permission": record["managed_operation_permissions"][self.kind.name()],
        }))
        .map_err(anyhow::Error::from)?;
        let permission = availability(facts);

        if !permission.write_permitted {
            return Err(ControlError::Conflict(format!(
                "{} is unavailable: {}",
                self.kind.label(),
                permission.reasons.join(", ")
            )));
        }

        if status["current_value"] == self.requested && status["requested_value"] == self.requested
        {
            return Ok(None);
        }

        Ok(Some(json!({
            "command": format!("set_{}", self.kind.name()),
            self.kind.parameter(): self.requested,
        })))
    }

    pub fn matches(&self, fields: &Map<String, Value>) -> bool {
        fields
            .get(self.kind.current_field())
            .and_then(Value::as_u64)
            == Some(u64::from(self.requested))
    }

    pub fn completion(
        &self,
        observed: Option<&Map<String, Value>>,
        request_sent: bool,
    ) -> Result<Value, ControlError> {
        let status = observed.map(|fields| self.kind.status(&json!(fields)));
        let outcome = AudioCapabilityReadback {
            status: status.as_ref().and_then(Value::as_object).cloned(),
            requested_value: self.requested,
        }
        .resolve();
        let mut result = serde_json::to_value(outcome).map_err(anyhow::Error::from)?;
        result["status"] = status.unwrap_or(Value::Null);
        result["request_sent"] = json!(request_sent);
        result["request_acknowledged"] = Value::Null;
        result["persistence"] = json!("unknown");
        Ok(result)
    }
}

pub fn parse_pullup_selection(value: &Value) -> Result<Value, ControlError> {
    let text = match value {
        Value::String(text) => text.trim().to_owned(),
        Value::Number(number) => number.to_string(),
        _ => String::new(),
    };

    if text.is_empty() {
        return Err(ControlError::Invalid(
            "sample-rate pull-up value is required".to_owned(),
        ));
    }

    if text.bytes().all(|byte| byte.is_ascii_digit()) {
        return text.parse::<u32>().map(|value| json!(value)).map_err(|_| {
            ControlError::Invalid("sample-rate pull-up value exceeds 4294967295".to_owned())
        });
    }

    sample_rate_pullup_choices()
        .into_iter()
        .find(|choice| {
            choice
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(&text))
        })
        .map(|choice| json!(choice.value))
        .ok_or_else(|| {
            ControlError::Invalid(
                "sample-rate pull-up must be none, +4.1667%, +0.1%, -0.1%, -4.0%, or a raw integer"
                    .to_owned(),
            )
        })
}
