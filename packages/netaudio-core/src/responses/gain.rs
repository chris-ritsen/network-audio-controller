use super::*;
use crate::commands::{GAIN_INPUT_DIRECTION, GAIN_LEVELS, GAIN_OUTPUT_DIRECTION};

#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AnalogAccess {
    pub managed: bool,
    #[serde(default)]
    pub managed_context_available: bool,
    #[serde(default)]
    pub managed_write_permitted: Option<bool>,
    pub address_available: bool,
    pub online: Option<bool>,
    pub supported: Option<bool>,
    pub locked: Option<bool>,
    pub write: bool,
}

impl AnalogAccess {
    pub fn denial(&self) -> Option<&'static str> {
        if self.managed {
            self.managed_denial()
        } else if !self.address_available || self.online == Some(false) {
            Some("Device is unavailable.")
        } else if self.supported != Some(true) {
            Some("Codec-control support is unavailable.")
        } else if self.write && self.locked != Some(false) {
            Some("Device is locked or its lock state is unknown.")
        } else {
            None
        }
    }

    fn managed_denial(&self) -> Option<&'static str> {
        if !self.managed_context_available {
            Some("Managed device context is unavailable.")
        } else if self.online == Some(false) {
            Some("Device is unavailable.")
        } else if self.supported == Some(false) {
            Some("Codec-control support is unavailable.")
        } else if self.write && self.managed_write_permitted != Some(true) {
            Some("Managed codec-control write permission is unavailable.")
        } else if self.write && self.locked == Some(true) {
            Some("Device is locked.")
        } else {
            None
        }
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GainLevel {
    pub value: u32,
    pub label: &'static str,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GainDirection {
    pub channel_type: &'static str,
    pub levels: Vec<GainLevel>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GainMetadata {
    pub input: GainDirection,
    pub output: GainDirection,
}

pub fn gain_metadata() -> GainMetadata {
    let levels = |head| {
        GAIN_LEVELS
            .iter()
            .zip([head, "+4 dBu", "0 dBu", "0 dBV", "-10 dBV"])
            .map(|(value, label)| GainLevel {
                value: *value,
                label,
            })
            .collect::<Vec<_>>()
    };

    GainMetadata {
        input: GainDirection {
            channel_type: "tx",
            levels: levels("+24 dBu"),
        },
        output: GainDirection {
            channel_type: "rx",
            levels: levels("+18 dBu"),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GainStatus {
    pub channel_levels: Vec<u32>,
    pub device_type: String,
    pub supported_levels: Vec<u32>,
}

#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AnalogLevelRequest {
    pub adapter: Option<GainStatus>,
    pub channel: serde_json::Value,
    pub level: serde_json::Value,
    pub direction: Option<String>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AnalogLevelPlan {
    Unsupported {
        reason: &'static str,
    },
    Ambiguous {
        reason: &'static str,
    },
    Unchanged {
        reason: (),
        before: u32,
        direction: String,
    },
    Change {
        reason: (),
        before: u32,
        direction: String,
    },
}

pub fn analog_level_control(request: AnalogLevelRequest) -> AnalogLevelPlan {
    let unsupported = |reason| AnalogLevelPlan::Unsupported { reason };
    let channel = request.channel.as_u64().filter(|value| *value > 0);
    let level = request.level.as_u64().filter(|value| *value > 0);

    let (Some(channel), Some(level)) = (channel, level) else {
        return unsupported("Channel and level must be positive integers.");
    };

    let Some(adapter) = request.adapter else {
        return AnalogLevelPlan::Ambiguous {
            reason: "Codec status does not identify one unambiguous analog control.",
        };
    };

    if request
        .direction
        .is_some_and(|direction| direction != adapter.device_type)
    {
        return unsupported("Requested direction differs from device status.");
    }

    let Some(before) = usize::try_from(channel - 1)
        .ok()
        .and_then(|index| adapter.channel_levels.get(index))
    else {
        return unsupported("Channel is not reported by the device.");
    };

    if !adapter
        .supported_levels
        .iter()
        .any(|value| u64::from(*value) == level)
    {
        return unsupported("Level is not supported by the device's analog control.");
    }

    if u64::from(*before) == level {
        AnalogLevelPlan::Unchanged {
            reason: (),
            before: *before,
            direction: adapter.device_type,
        }
    } else {
        AnalogLevelPlan::Change {
            reason: (),
            before: *before,
            direction: adapter.device_type,
        }
    }
}

fn gain_device_type(parameter: &CodecParameterStatus) -> Option<&'static str> {
    match u16::from_be_bytes([parameter.parameter_type, parameter.mode]) {
        GAIN_INPUT_DIRECTION => Some("input"),
        GAIN_OUTPUT_DIRECTION => Some("output"),
        _ => None,
    }
}

pub(crate) fn gain_adapter_from_parameters(
    parameters: &[CodecParameterStatus],
) -> Option<GainStatus> {
    let mut matches = parameters.iter().filter_map(|parameter| {
        gain_device_type(parameter).map(|device_type| (device_type, parameter))
    });
    let (device_type, parameter) = matches.next()?;
    if matches.next().is_some()
        || parameters
            .iter()
            .any(|p| matches!(p.parameter_type, 1 | 2) && gain_device_type(p).is_none())
        || parameter.values.len() > 2
        || parameter.values.is_empty()
        || parameter
            .values
            .iter()
            .any(|value| !GAIN_LEVELS.contains(value))
    {
        return None;
    }
    Some(GainStatus {
        channel_levels: parameter.values.clone(),
        device_type: device_type.to_owned(),
        supported_levels: GAIN_LEVELS.to_vec(),
    })
}
