use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    profile, CodecFormatStatus, PanelFamily, PanelProfileRequest, PanelRequest, SerialSettings,
    VideoFormatStatus,
};

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PanelPlanRequest {
    pub profile: PanelProfileRequest,
    pub category: String,
    pub requested: Value,
    pub fresh_values: BTreeMap<String, Value>,
    pub confirm_clear: bool,
}

#[derive(Clone, Copy, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PanelPlanAction {
    Unavailable,
    Unsupported,
    Unchanged,
    Change,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelPlan {
    pub category: String,
    pub requested: Value,
    pub action: PanelPlanAction,
    pub reason: Option<String>,
    pub requests: Vec<PanelRequest>,
    pub before: Option<Value>,
    pub expected: Option<Value>,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PanelReadbackRequest {
    pub observed: Value,
    pub expected: Value,
}

pub fn matches(observed: &Value, expected: &Value) -> bool {
    match (observed.as_object(), expected.as_object()) {
        (Some(observed), Some(expected)) => expected
            .iter()
            .all(|(key, value)| observed.get(key) == Some(value)),
        _ => observed == expected,
    }
}

type Failure = (PanelPlanAction, String);

fn unsupported(message: impl ToString) -> Failure {
    (PanelPlanAction::Unsupported, message.to_string())
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, Failure> {
    serde_json::from_value(value.clone()).map_err(unsupported)
}

impl PanelPlanRequest {
    fn value(&self, category: &str) -> Result<&Value, Failure> {
        self.fresh_values.get(category).ok_or_else(|| {
            (
                PanelPlanAction::Unavailable,
                format!(
                    "Fresh {} status is unavailable.",
                    category.replace('_', " ")
                ),
            )
        })
    }

    fn prepare(&self, current: &Value) -> Result<(Vec<PanelRequest>, Value), Failure> {
        let category = self.category.as_str();
        let specification = match category {
            "bluetooth_pairing" => {
                if self.requested != "clear" || !self.confirm_clear {
                    return Err(unsupported(
                        "Clearing remembered devices requires explicit confirmation.",
                    ));
                }

                json!({"operation": "bluetooth_clear_pairing", "confirmed": true})
            }
            "bluetooth_discovery" => json!({"operation": category, "discoverable": self.requested}),
            "codec_format" => json!({"operation": category, "format": self.requested}),
            "serial" => json!({"operation": category, "settings": self.requested}),
            _ => {
                let mut fields = self
                    .requested
                    .as_object()
                    .cloned()
                    .ok_or_else(|| unsupported("Settings must be an object."))?;

                if fields.contains_key("operation") {
                    return Err(unsupported(
                        "Operation is selected by the setting category.",
                    ));
                }

                fields.insert("operation".into(), json!(category));
                Value::Object(fields)
            }
        };
        let request: PanelRequest = decode(&specification)?;
        // Encoder validation applies even to no-ops; an unknown setting is not confirmed by equality.
        request.encode().map_err(unsupported)?;
        let mut extra = Vec::new();
        let expected = match &request {
            PanelRequest::BluetoothIdentification {
                name_source,
                custom_name,
            } => {
                if !matches!(current["name_source"].as_u64(), Some(1 | 2)) {
                    return Err(unsupported(
                        "The current Bluetooth naming mode is unsupported.",
                    ));
                }

                json!({"name_source": name_source, "custom_name": custom_name})
            }
            PanelRequest::BluetoothDiscovery { discoverable } => {
                if !matches!(current.as_u64(), Some(1 | 2)) {
                    return Err(unsupported(
                        "The device has not reported a supported discoverability state.",
                    ));
                }

                json!(if *discoverable { 1 } else { 2 })
            }
            PanelRequest::BluetoothClearPairing { .. } => json!(0),
            PanelRequest::VideoFormat { format, selection } => {
                let status: VideoFormatStatus = decode(current)?;

                if status.direction != 0 || !self.profile.video_transmission_supported {
                    return Err(unsupported("Video format changes are supported only on the advertised reference transmitter."));
                }

                let supported = status.supported.iter().any(|candidate| {
                    (!selection.manual_resolution || candidate.resolution == format.resolution)
                        && (!selection.manual_bit_depth
                            || candidate.bit_depth & format.bit_depth != 0)
                        && (!selection.manual_color_space
                            || candidate.color_space & format.color_space != 0)
                });

                if !supported {
                    return Err(unsupported(
                        "Format combination is not advertised by the device.",
                    ));
                }

                let changed = status.configured.as_ref().is_none_or(|value| {
                    value.bit_depth != format.bit_depth || value.color_space != format.color_space
                }) || status.selection.as_ref().is_none_or(|value| {
                    value.manual_bit_depth != selection.manual_bit_depth
                        || value.manual_color_space != selection.manual_color_space
                });

                if (changed || selection.manual_resolution)
                    && self.value("visca")?["capability"].as_u64() != Some(1)
                {
                    return Err(unsupported(
                        "Fresh scoped video-format capability is required.",
                    ));
                }

                if selection.manual_resolution {
                    let visca = PanelRequest::VideoViscaFormat {
                        format: format.clone(),
                        selection: selection.clone(),
                    };
                    visca.encode().map_err(unsupported)?;
                    extra.push(visca);
                }

                json!({"configured": format, "selection": selection})
            }
            PanelRequest::CodecFormat { format } => {
                let status: CodecFormatStatus = decode(current)?;

                if !status.supported.iter().any(|candidate| {
                    candidate.codec_type == format.codec_type
                        && candidate.profile == format.profile
                        && candidate.level & format.level != 0
                }) {
                    return Err(unsupported(
                        "Codec combination is not advertised by the device.",
                    ));
                }

                json!({"current": format})
            }
            PanelRequest::Serial { settings } => {
                let observed: SerialSettings = decode(current)?;
                PanelRequest::Serial { settings: observed }.encode()
                    .map_err(|_| unsupported("The device has not reported a supported serial format; refusing to overwrite it."))?;
                json!(settings)
            }
            PanelRequest::Bandwidth { target, enabled } => {
                if self.value("video_format")?["direction"].as_u64() != Some(0)
                    || !self.profile.video_transmission_supported
                {
                    return Err(unsupported("Bandwidth is writable only on a transmitter."));
                }

                let minimum = current["minimum"].as_u64().filter(|value| *value > 0);
                let maximum = current["maximum"].as_u64().filter(|value| *value > 0);
                let (Some(minimum), Some(maximum)) = (minimum, maximum) else {
                    return Err(unsupported("Bandwidth control is unavailable."));
                };

                if *enabled && !(minimum..=maximum).contains(&u64::from(*target)) {
                    return Err(unsupported(
                        "Target is outside the device's supported Mbit/s range.",
                    ));
                }

                json!({"target": target, "enabled": u8::from(*enabled)})
            }
            PanelRequest::Hdcp { mode } => {
                if !current["supported_modes"]
                    .as_array()
                    .is_some_and(|modes| modes.contains(&json!(mode)))
                {
                    return Err(unsupported("HDCP mode is not advertised by the device."));
                }

                json!({"configured_mode": mode})
            }
            _ => return Err(unsupported("This setting has no supported writer.")),
        };
        let mut requests = vec![request];
        requests.extend(extra);
        Ok((requests, expected))
    }
}

pub fn plan(input: PanelPlanRequest) -> PanelPlan {
    let profile = profile(&input.profile);
    let mut plan = PanelPlan {
        category: input.category.clone(),
        requested: input.requested.clone(),
        action: PanelPlanAction::Unavailable,
        reason: None,
        requests: Vec::new(),
        before: None,
        expected: None,
    };
    let result = (|| {
        let family = match input.category.as_str() {
            "bluetooth_identification" | "bluetooth_discovery" | "bluetooth_pairing" => {
                PanelFamily::Bluetooth
            }
            "video_format" | "codec_format" | "serial" | "bandwidth" | "hdcp" => {
                PanelFamily::DanteAv
            }
            _ => return Err(unsupported("This setting has no supported writer.")),
        };

        if profile.family != Some(family) {
            return Err(unsupported(
                "This setting does not belong to the device's panel.",
            ));
        }

        let current = input.value(&input.category)?;
        plan.before = Some(current.clone());
        let (requests, expected) = input.prepare(current)?;
        let unchanged = input.category != "bluetooth_pairing" && matches(current, &expected);
        plan.expected = Some(expected);

        if unchanged {
            plan.action = PanelPlanAction::Unchanged;
        } else if let Some(reason) = profile.write_unavailable_reason {
            plan.reason = Some(reason.to_owned());
        } else {
            plan.action = PanelPlanAction::Change;
            plan.requests = requests;
        }

        Ok(())
    })();

    if let Err((action, reason)) = result {
        plan.action = action;
        plan.reason = Some(reason);
    }

    plan
}
