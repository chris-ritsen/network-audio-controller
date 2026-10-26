use super::operations::ControlError;
use super::record::DeviceRecord;
use anyhow::{Context, Result};
use netaudio_core::device_controls::{PanelFamily, PanelProfileRequest, PanelStatus, profile};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

const FRESHNESS_SECONDS: f64 = 10.0;

#[derive(Clone, Debug)]
pub struct Change {
    pub category: String,
    pub requested: Value,
    pub confirm_clear: bool,
}

#[derive(Clone, Debug)]
pub enum Operation {
    Inspect,
    Plan(Change),
    Apply(Change),
}

impl Change {
    pub fn parse(parameters: &Map<String, Value>) -> Result<Self, ControlError> {
        let category = parameters
            .get("category")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ControlError::Invalid("category is required".into()))?;
        let requested = parameters
            .get("requested")
            .context("requested is required")
            .map_err(|error| ControlError::Invalid(error.to_string()))?;
        let confirm_clear = match parameters.get("confirm_clear") {
            None => false,
            Some(Value::Bool(value)) => *value,
            _ => {
                return Err(ControlError::Invalid(
                    "confirm_clear must be a boolean".into(),
                ));
            }
        };
        Ok(Self {
            category: category.to_owned(),
            requested: requested.clone(),
            confirm_clear,
        })
    }
}

impl Operation {
    pub fn change(&self) -> Option<&Change> {
        match self {
            Self::Inspect => None,
            Self::Plan(change) | Self::Apply(change) => Some(change),
        }
    }

    pub fn precondition(&self, record: &Value) -> Value {
        let mut values = Map::new();
        if let Some(change) = self.change() {
            let facts = facts(
                record.as_object().unwrap_or(&Map::new()),
                record["online"].as_bool(),
                !record["ipv4"].is_null(),
            );
            for query in profile(&facts).queries {
                if query.category == change.category {
                    for category in query.prerequisites {
                        values.insert(
                            category.to_owned(),
                            record["device_controls"]["observations"][category]["value"].clone(),
                        );
                    }
                }
            }
        }
        json!({
            "platform": record["platform_versions_record"],
            "managed": record["requires_managed_control"],
            "supported": record["virtual_panel_supported"],
            "read_allowed": record["panel_read_allowed"],
            "write_allowed": record["panel_write_allowed"],
            "video_transmission_supported": record["video_transmission_supported"],
            "values": values,
            "analog": record["gain_adapter"],
        })
    }
}

pub fn facts(
    properties: &Map<String, Value>,
    online: Option<bool>,
    address_available: bool,
) -> PanelProfileRequest {
    let versions = properties
        .get("platform_versions_record")
        .unwrap_or(&Value::Null);
    PanelProfileRequest {
        plugins: versions["plugin_identifiers"]
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .map(|value| value.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default(),
        platform_model_identifier_hexadecimal: versions["platform_model_identifier_hexadecimal"]
            .as_str()
            .map(str::to_owned),
        managed: properties.get("requires_managed_control") == Some(&json!(true)),
        address_available,
        online,
        virtual_panel_supported: properties.get("virtual_panel_supported") == Some(&json!(true)),
        read_allowed: properties
            .get("panel_read_allowed")
            .is_none_or(|value| value == true),
        write_allowed: properties
            .get("panel_write_allowed")
            .is_none_or(|value| value == true),
        locked: properties.get("is_locked").and_then(Value::as_bool),
        video_transmission_supported: properties.get("video_transmission_supported")
            == Some(&json!(true)),
    }
}

pub fn record_facts(record: &DeviceRecord) -> PanelProfileRequest {
    facts(
        &record.properties,
        Some(record.online),
        !record.address.is_unspecified(),
    )
}

pub fn family_name(family: Option<PanelFamily>) -> Option<&'static str> {
    family.map(|family| match family {
        PanelFamily::Bluetooth => "bluetooth",
        PanelFamily::DanteAv => "dante_av",
    })
}

fn identity(properties: &Map<String, Value>) -> Value {
    let mut identity = properties
        .get("platform_versions_record")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    identity.insert(
        "advertised_model_id".into(),
        properties.get("model_id").cloned().unwrap_or(Value::Null),
    );
    Value::Object(identity)
}

fn state(properties: &Map<String, Value>) -> Map<String, Value> {
    let identity = identity(properties);
    let mut state = properties
        .get("device_controls")
        .and_then(Value::as_object)
        .filter(|state| state.get("identity") == Some(&identity))
        .cloned()
        .unwrap_or_default();
    state.insert("identity".into(), identity);
    state
}

pub fn observe(
    record: &mut DeviceRecord,
    status: &PanelStatus,
    correlated: bool,
    observed: f64,
) -> Result<()> {
    let mut state = state(&record.properties);
    let mut diagnostic = serde_json::to_value(status)?;
    diagnostic["correlated"] = json!(correlated);
    diagnostic["observed_at_unix"] = json!(observed);
    state.insert("latest_diagnostic".into(), diagnostic.clone());
    let mut observations = state
        .remove("observations")
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    if status.diagnostic_error.is_some() {
        for observation in observations.values_mut() {
            observation["available"] = json!(false);
        }
    }
    let store = state
        .entry(if correlated {
            "correlated_status"
        } else {
            "unsolicited_status"
        })
        .or_insert_with(|| json!({}));
    for item in &status.observations {
        let item = serde_json::to_value(item)?;
        let category = item["category"]
            .as_str()
            .context("panel category is unavailable")?;
        let mut observation = diagnostic
            .as_object()
            .context("panel status is not an object")?
            .clone();
        observation.remove("observations");
        observation.remove("followup_requests");
        observation.insert("value".into(), item["value"].clone());
        observation.insert("available".into(), json!(true));
        observation.insert("source".into(), json!("conmon_panel_status"));
        store[category] = json!(observation);
        observations.insert(category.to_owned(), json!(observation));
        if category == "bluetooth_connection" {
            record.properties.insert(
                "bluetooth_connected".into(),
                item["value"]["connected"].clone(),
            );
            record.properties.insert(
                "bluetooth_device".into(),
                item["value"]["peer_name"].clone(),
            );
        }
    }
    state.insert("observations".into(), json!(observations));
    record
        .properties
        .insert("device_controls".into(), json!(state));
    Ok(())
}

pub fn unavailable(record: &mut DeviceRecord, category: &str) {
    if let Some(observation) = record
        .properties
        .get_mut("device_controls")
        .and_then(|state| state.get_mut("observations"))
        .and_then(|observations| observations.get_mut(category))
    {
        observation["available"] = json!(false);
    }
}

pub fn snapshot(
    properties: &Map<String, Value>,
    online: bool,
    address_available: bool,
) -> Result<Value> {
    let facts = facts(properties, Some(online), address_available);
    let profile = profile(&facts);
    let mut state = state(properties);
    let now = chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0;
    let mut values = BTreeMap::new();
    let mut fresh_values = BTreeMap::new();
    for (category, observation) in state
        .get_mut("observations")
        .and_then(Value::as_object_mut)
        .into_iter()
        .flatten()
    {
        let fresh = online
            && observation["available"] == true
            && observation["observed_at_unix"]
                .as_f64()
                .is_some_and(|observed| (0.0..=FRESHNESS_SECONDS).contains(&(now - observed)));
        observation["fresh"] = json!(fresh);
        values.insert(category.clone(), observation["value"].clone());
        if fresh {
            fresh_values.insert(category.clone(), observation["value"].clone());
        }
    }
    let presentation = netaudio_core::device_controls::presentation::presentation(
        netaudio_core::device_controls::presentation::PanelPresentationRequest {
            profile: facts,
            values,
            fresh_values,
        },
    );
    let metadata = json!({
        "family": profile.family, "panels": profile.panels, "categories": profile.categories,
        "selection": profile.selection, "unrecognized_panels": profile.unrecognized_panels,
        "read_unavailable_reason": profile.read_unavailable_reason, "write_unavailable_reason": profile.write_unavailable_reason,
        "readable": profile.read_unavailable_reason.is_none(), "writable": profile.write_unavailable_reason.is_none(),
        "presentation": presentation,
    });
    state.extend(metadata.as_object().unwrap().clone());
    Ok(json!(state))
}
