use anyhow::Result;
use netaudio_core::responses::DeviceSettings;
use serde_json::{Map, Value, json};

pub fn apply_settings(
    properties: &mut Map<String, Value>,
    settings: &DeviceSettings,
) -> Result<()> {
    use netaudio_core::responses::{
        DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS, DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS,
        DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS, DEVICE_SETTINGS_INFO_MAX_LATENCY_NS,
        DEVICE_SETTINGS_INFO_MIN_LATENCY_NS,
    };

    let fields = [
        (
            DEVICE_SETTINGS_INFO_ACTIVE_LATENCY_NS,
            "active_latency_ns",
            settings.active_latency_ns,
        ),
        (
            DEVICE_SETTINGS_INFO_CONFIGURED_LATENCY_NS,
            "configured_latency_ns",
            settings.configured_latency_ns,
        ),
        (
            DEVICE_SETTINGS_INFO_DEFAULT_LATENCY_NS,
            "default_latency_ns",
            settings.default_latency_ns,
        ),
        (
            DEVICE_SETTINGS_INFO_MIN_LATENCY_NS,
            "min_latency_ns",
            settings.min_latency_ns,
        ),
        (
            DEVICE_SETTINGS_INFO_MAX_LATENCY_NS,
            "max_latency_ns",
            settings.max_latency_ns,
        ),
    ];
    let mut observed = false;

    for (property, field, value) in fields {
        if value.is_some()
            || settings.unavailable_property_ids.contains(&property)
            || settings.ambiguous_property_ids.contains(&property)
        {
            properties.insert(field.to_owned(), json!(value));
            observed = true;
        }
    }

    if !observed {
        return Ok(());
    }

    let configuration = netaudio_core::latency_configuration::configuration(properties)
        .map_err(anyhow::Error::msg)?;
    let configuration = serde_json::to_value(configuration)?;

    if let Some(controls) = configuration.get("controls").and_then(Value::as_object) {
        for (key, value) in controls {
            properties.insert(format!("{key}_ms"), value.clone());
        }
    }

    properties.insert(
        "standard_latency_choices_ms".to_owned(),
        configuration
            .pointer("/state/latency_options_ms")
            .cloned()
            .unwrap_or(Value::Null),
    );
    Ok(())
}
