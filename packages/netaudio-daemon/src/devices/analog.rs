use super::operations::ControlError;
use netaudio_core::responses::{
    AnalogAccess, AnalogLevelPlan, AnalogLevelRequest, analog_level_control,
};
use serde_json::{Map, Value, json};
use std::sync::OnceLock;

pub fn present(record: &mut Map<String, Value>) {
    static METADATA: OnceLock<netaudio_core::responses::GainMetadata> = OnceLock::new();
    let metadata = METADATA.get_or_init(netaudio_core::responses::gain_metadata);
    let supported = record
        .get("supported_gain_levels")
        .and_then(Value::as_array)
        .cloned();

    if let Some(supported) = &supported {
        record.insert("gain_configurable".to_owned(), json!(!supported.is_empty()));
    }

    let direction = match record.get("gain_device_type").and_then(Value::as_str) {
        Some("input") => Some((&metadata.input, "transmitters")),
        Some("output") => Some((&metadata.output, "receivers")),
        _ => None,
    };
    let label = |level: &Value| {
        direction
            .and_then(|(metadata, _)| {
                metadata
                    .levels
                    .iter()
                    .find(|entry| level.as_u64() == Some(u64::from(entry.value)))
            })
            .map_or("Unknown", |entry| entry.label)
    };

    if direction.is_some()
        && let Some(supported) = supported
    {
        record.insert(
            "gain_level_choices".to_owned(),
            json!(
                supported
                    .iter()
                    .map(|level| json!({"value": level, "label": label(level)}))
                    .collect::<Vec<_>>()
            ),
        );
    }

    let levels = record.get("gain_levels").and_then(Value::as_array).cloned();

    if let Some(channels) = record.get_mut("channels").and_then(Value::as_object_mut) {
        for (group, channels) in channels {
            for (number, channel) in channels.as_object_mut().into_iter().flatten() {
                let level = direction
                    .filter(|(_, expected)| group == expected)
                    .and_then(|_| number.parse::<usize>().ok())
                    .and_then(|number| number.checked_sub(1))
                    .and_then(|index| levels.as_ref()?.get(index));

                if let Some(channel) = channel.as_object_mut() {
                    if let Some(level) = level {
                        channel.insert("gain_level".to_owned(), level.clone());
                        channel.insert("gain_level_label".to_owned(), json!(label(level)));
                    } else {
                        channel.remove("gain_level");
                        channel.remove("gain_level_label");
                    }
                }
            }
        }
    }
}

pub fn access(record: &Value, write: bool) -> Result<Option<&'static str>, ControlError> {
    let access: AnalogAccess = serde_json::from_value(json!({
        "managed": record["requires_managed_control"] == true,
        "managed_context_available": record["ddm_fresh"] == true && record["ddm_connection_state"] == "READY",
        "managed_write_permitted": record["managed_operation_permissions"]["codec_control"],
        "address_available": !record["ipv4"].is_null(),
        "online": record["online"],
        "supported": record["generic_codec_control_supported"],
        "locked": record["is_locked"],
        "write": write,
    }))
    .map_err(anyhow::Error::from)?;
    Ok(access.denial())
}

#[derive(Clone, Debug)]
pub struct Configuration {
    pub channel: u16,
    pub level: u8,
    pub direction: Option<String>,
}

impl Configuration {
    pub fn new(channel: &Value, level: &Value, direction: &Value) -> Result<Self, ControlError> {
        let channel = channel
            .as_u64()
            .and_then(|channel| u16::try_from(channel).ok())
            .filter(|channel| *channel > 0)
            .ok_or_else(|| {
                ControlError::Invalid("Channel and level must be positive integers.".to_owned())
            })?;
        let level = level
            .as_u64()
            .and_then(|level| u8::try_from(level).ok())
            .filter(|level| *level > 0)
            .ok_or_else(|| {
                ControlError::Invalid("Channel and level must be positive integers.".to_owned())
            })?;
        let direction = match direction {
            Value::Null => None,
            Value::String(direction) if matches!(direction.as_str(), "input" | "output") => {
                Some(direction.clone())
            }
            _ => {
                return Err(ControlError::Invalid(
                    "direction must be input or output".to_owned(),
                ));
            }
        };
        Ok(Self {
            channel,
            level,
            direction,
        })
    }

    pub fn requested(&self) -> Value {
        json!({"channel": self.channel, "level": self.level, "direction": self.direction})
    }

    pub fn precondition(&self, record: &Value) -> Value {
        json!({
            "adapter": record["gain_adapter"],
            "supported": record["generic_codec_control_supported"],
            "managed": record["requires_managed_control"],
            "permission": record["managed_operation_permissions"]["codec_control"],
        })
    }

    pub(crate) fn plan(&self, adapter: &Value) -> Result<AnalogLevelPlan, ControlError> {
        let request: AnalogLevelRequest = serde_json::from_value(json!({
            "adapter": adapter,
            "channel": self.channel,
            "level": self.level,
            "direction": self.direction,
        }))
        .map_err(anyhow::Error::from)?;
        Ok(analog_level_control(request))
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        if let Some(reason) = access(record, true)? {
            return Err(ControlError::Conflict(reason.to_owned()));
        }

        match self.plan(&record["gain_adapter"])? {
            AnalogLevelPlan::Unchanged { .. } => Ok(None),
            AnalogLevelPlan::Change { direction, .. } => Ok(Some(json!({
                "command": "set_gain_level",
                "channel_number": self.channel,
                "gain_level": self.level,
                "device_type": direction,
            }))),
            AnalogLevelPlan::Unsupported { reason } | AnalogLevelPlan::Ambiguous { reason } => {
                Err(ControlError::Conflict(reason.to_owned()))
            }
        }
    }

    pub fn matches(&self, fields: &Map<String, Value>) -> bool {
        fields.get("gain_adapter").is_some_and(|adapter| {
            matches!(self.plan(adapter), Ok(AnalogLevelPlan::Unchanged { .. }))
        })
    }

    pub fn completion(&self, observed: Option<&Map<String, Value>>, sent: bool) -> Value {
        let confirmed = observed.map(|fields| self.matches(fields));
        json!({
            "request_sent": sent,
            "request_acknowledged": null,
            "effective_state_confirmed": confirmed,
            "configuration_confirmed": confirmed,
            "status": observed.and_then(|fields| fields.get("gain_adapter")),
            "persistence": "unknown",
            "media_readiness": "unknown",
        })
    }
}
