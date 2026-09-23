//! Client-facing choices use the same planner that authorizes device commands.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    planning::{plan, PanelPlan, PanelPlanAction, PanelPlanRequest},
    profile, PanelProfileRequest, BANDWIDTH_MAXIMUM, BLUETOOTH_NAME_LIMIT, SERIAL_BAUD_RATES,
};
use crate::configuration::panel_setting;

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PanelPresentationRequest {
    pub profile: PanelProfileRequest,
    pub values: BTreeMap<String, Value>,
    pub fresh_values: BTreeMap<String, Value>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelField {
    pub key: String,
    pub label: String,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelVariant {
    pub requested: Value,
    pub fields: BTreeMap<String, PanelField>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelBandwidth {
    pub minimum: u32,
    pub maximum: u32,
    pub enable: Value,
    pub disable: Value,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelEditor {
    pub initial: Value,
    pub initial_fields: BTreeMap<String, PanelField>,
    pub variants: Vec<PanelVariant>,
    pub reason: Option<String>,
    pub details: BTreeMap<String, String>,
    pub bandwidth: Option<PanelBandwidth>,
    pub custom_name_limit: Option<usize>,
    pub custom_name_source: Option<u32>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PanelPresentation {
    pub editors: BTreeMap<String, PanelEditor>,
    pub summary: BTreeMap<String, String>,
}

const DEPTHS: &[(u32, &str)] = &[
    (1, "6-bit"),
    (2, "8-bit"),
    (4, "10-bit"),
    (8, "12-bit"),
    (16, "14-bit"),
    (32, "16-bit"),
];
const COLORS: &[(u32, &str)] = &[
    (1, "RGB 4:4:4"),
    (2, "YCbCr 4:0:0"),
    (4, "YCbCr 4:2:0"),
    (8, "YCbCr 4:2:2"),
    (16, "YCbCr 4:4:4"),
    (32, "YCbCr 4:2:2:4"),
];
const RESOLUTIONS: &[(u32, &str)] = &[
    (0, "Automatic"),
    (256, "800×480 60 Hz"),
    (257, "800×600 60 Hz"),
    (258, "1024×768 60 Hz"),
    (259, "1280×1024 60 Hz"),
    (260, "1600×1200 60 Hz"),
    (261, "1920×1200 60 Hz"),
    (262, "1920×1200 60 Hz reduced blanking"),
];
const HDCP_MODES: &[(u32, &str)] = &[(1, "None"), (2, "HDCP 1.x"), (3, "Automatic")];
const NAME_SOURCES: &[(u32, &str)] = &[(1, "Dante device name"), (2, "Custom name")];
const PARITIES: &[(u32, &str)] = &[(0, "None"), (1, "Even"), (2, "Odd")];

fn label(value: &Value, choices: &[(u32, &'static str)]) -> &'static str {
    choices
        .iter()
        .find(|(id, _)| value.as_u64() == Some(u64::from(*id)))
        .map_or("Unknown", |(_, name)| *name)
}

fn format_label(format: &Value) -> String {
    if !format.is_object() {
        return "Unavailable".into();
    }

    format!(
        "{}, {}, {}",
        label(&format["resolution"], RESOLUTIONS),
        label(&format["bit_depth"], DEPTHS),
        label(&format["color_space"], COLORS)
    )
}

fn field(value: &Value, name: impl Into<String>) -> PanelField {
    PanelField {
        key: value.to_string(),
        label: name.into(),
    }
}

fn fields(category: &str, requested: &Value) -> BTreeMap<String, PanelField> {
    let mut fields = BTreeMap::new();

    match category {
        "video_format" => {
            for (name, labels) in [
                ("resolution", RESOLUTIONS),
                ("bit_depth", DEPTHS),
                ("color_space", COLORS),
            ] {
                let manual =
                    requested["selection"][format!("manual_{name}")].as_bool() == Some(true);
                let value = &requested["format"][name];
                fields.insert(
                    name.into(),
                    if manual {
                        field(value, label(value, labels))
                    } else {
                        PanelField {
                            key: "automatic".into(),
                            label: "Automatic".into(),
                        }
                    },
                );
            }
        }
        "codec_format" => {
            let codec = label(&requested["codec_type"], &[(1, "JPEG 2000")]);
            let profile = label(
                &requested["profile"],
                &[(1, "Broadcast"), (2, "Ultra-low-latency")],
            );
            let level = label(
                &requested["level"],
                &[(1, "1"), (2, "2"), (4, "3"), (8, "4"), (16, "5")],
            );
            fields.insert(
                "codec".into(),
                field(requested, format!("{codec}, {profile}, level {level}")),
            );
        }
        "serial" => {
            for name in ["baud_rate", "data_bits", "stop_bits"] {
                let value = &requested[name];
                fields.insert(name.into(), field(value, value.to_string()));
            }
            fields.insert(
                "parity".into(),
                field(&requested["parity"], label(&requested["parity"], PARITIES)),
            );
        }
        "hdcp" => {
            fields.insert(
                "mode".into(),
                field(&requested["mode"], label(&requested["mode"], HDCP_MODES)),
            );
        }
        "bluetooth_identification" => {
            fields.insert(
                "name_source".into(),
                field(
                    &requested["name_source"],
                    label(&requested["name_source"], NAME_SOURCES),
                ),
            );
        }
        _ => {}
    }

    fields
}

impl PanelPresentationRequest {
    fn plan(&self, category: &str, requested: Value) -> PanelPlan {
        plan(PanelPlanRequest {
            profile: self.profile.clone(),
            category: category.into(),
            requested,
            fresh_values: self.fresh_values.clone(),
            confirm_clear: false,
        })
    }

    fn candidates(&self, category: &str, initial: &Value) -> Vec<Value> {
        let current = &self.values[category];
        let mut candidates = Vec::new();

        match category {
            "video_format" => {
                let resolutions: BTreeSet<u64> = std::iter::once(0)
                    .chain(
                        current["supported"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|v| v["resolution"].as_u64()),
                    )
                    .collect();

                for resolution in resolutions {
                    for depth in std::iter::once(0).chain(DEPTHS.iter().map(|(value, _)| *value)) {
                        for color in
                            std::iter::once(0).chain(COLORS.iter().map(|(value, _)| *value))
                        {
                            candidates.push(json!({
                                "format": {"resolution": resolution, "bit_depth": depth, "color_space": color},
                                "selection": {"manual_resolution": resolution != 0, "manual_bit_depth": depth != 0, "manual_color_space": color != 0}
                            }));
                        }
                    }
                }
            }
            "codec_format" => {
                for supported in current["supported"].as_array().into_iter().flatten() {
                    for level in [1, 2, 4, 8, 16] {
                        candidates.push(json!({"codec_type": supported["codec_type"], "profile": supported["profile"], "level": level}));
                    }
                }
            }
            "serial" => {
                for baud in SERIAL_BAUD_RATES {
                    for data in [7, 8] {
                        for (parity, _) in PARITIES {
                            for stop in [1, 2] {
                                candidates.push(json!({"baud_rate": baud, "data_bits": data, "parity": parity,
                                    "stop_bits": stop, "hardware_flow_control": 0, "software_flow_control": 0}));
                            }
                        }
                    }
                }
            }
            "hdcp" => {
                for (mode, _) in HDCP_MODES {
                    candidates.push(json!({"mode": mode}));
                }
            }
            "bluetooth_identification" => {
                for (source, _) in NAME_SOURCES {
                    candidates.push(
                        json!({"name_source": source, "custom_name": initial["custom_name"]}),
                    );
                }
            }
            _ => {}
        }

        candidates
    }
}

pub fn presentation(input: PanelPresentationRequest) -> PanelPresentation {
    let profile = profile(&input.profile);
    let mut output = PanelPresentation {
        editors: BTreeMap::new(),
        summary: BTreeMap::new(),
    };

    for (category, current) in &input.values {
        let Some(initial) = panel_setting(category, current) else {
            continue;
        };
        let plan = input.plan(category, initial.clone());
        let mut editor = PanelEditor {
            initial_fields: fields(category, &initial),
            initial,
            variants: Vec::new(),
            reason: profile
                .write_unavailable_reason
                .map(str::to_owned)
                .or(plan.reason),
            details: BTreeMap::new(),
            bandwidth: None,
            custom_name_limit: None,
            custom_name_source: None,
        };

        if editor.reason.is_none() {
            let mut seen = BTreeSet::new();

            for requested in input.candidates(category, &editor.initial) {
                if !seen.insert(requested.to_string()) {
                    continue;
                }
                let plan = input.plan(category, requested.clone());

                if matches!(
                    plan.action,
                    PanelPlanAction::Change | PanelPlanAction::Unchanged
                ) {
                    editor.variants.push(PanelVariant {
                        fields: fields(category, &requested),
                        requested,
                    });
                }
            }
        }

        match category.as_str() {
            "video_format" => {
                editor
                    .details
                    .insert("configured".into(), format_label(&current["configured"]));
                editor
                    .details
                    .insert("actual".into(), format_label(&current["actual"]));
                editor.details.insert(
                    "direction".into(),
                    label(
                        &current["direction"],
                        &[(0, "Transmitter"), (1, "Receiver")],
                    )
                    .into(),
                );
            }
            "bandwidth" => {
                let minimum = current["minimum"]
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok());
                let maximum = current["maximum"]
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .map(|v| v.min(BANDWIDTH_MAXIMUM));

                if let (Some(minimum), Some(maximum)) = (minimum, maximum) {
                    if minimum > 0 && minimum <= maximum {
                        let target = current["target"]
                            .as_u64()
                            .and_then(|v| u32::try_from(v).ok())
                            .filter(|v| (minimum..=maximum).contains(v))
                            .unwrap_or(minimum);
                        editor.bandwidth = Some(PanelBandwidth {
                            minimum,
                            maximum,
                            enable: json!({"target": target, "enabled": true}),
                            disable: json!({"target": 0, "enabled": false}),
                        });
                    }
                }

                if editor.bandwidth.is_none() {
                    editor.reason = Some("Bandwidth control is unavailable.".into());
                }
            }
            "bluetooth_identification" => {
                editor.custom_name_limit = Some(BLUETOOTH_NAME_LIMIT);
                editor.custom_name_source = Some(2);
            }
            _ => {}
        }

        output.editors.insert(category.clone(), editor);
    }

    if let Some(connection) = input.values.get("bluetooth_connection") {
        output.summary.insert(
            "connection".into(),
            label(
                &connection["state"],
                &[
                    (0, "Unknown"),
                    (1, "Connected"),
                    (2, "Disconnected"),
                    (3, "Link lost"),
                ],
            )
            .into(),
        );
    }

    if let Some(video) = input.values.get("video_channel") {
        output.summary.insert(
            "signal".into(),
            label(
                &video["status_code"],
                &[
                    (0, "Unknown"),
                    (16, "Valid unprotected signal"),
                    (17, "Valid protected signal"),
                    (32, "Disconnected"),
                    (33, "Invalid unprotected signal"),
                    (34, "HDCP negotiation failed"),
                    (35, "Negotiating"),
                    (36, "Incompatible HDCP"),
                    (48, "HDCP unsupported"),
                    (49, "HDMI failure"),
                    (50, "Invalid video format"),
                    (51, "No video"),
                    (52, "Codec failure"),
                    (53, "Sink does not support the video"),
                    (54, "Source-format mismatch"),
                ],
            )
            .into(),
        );
        output.summary.insert(
            "observed_hdcp".into(),
            if video["observed_hdcp_version"].is_null() {
                "Unavailable"
            } else {
                label(
                    &video["observed_hdcp_version"],
                    &[(0, "Undefined"), (1, "None"), (2, "1.x"), (3, "2.x")],
                )
            }
            .into(),
        );
    }

    output
}
