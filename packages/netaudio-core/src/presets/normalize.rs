use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

use crate::flow_specification::TransmitFlowSpecification;

pub fn normalize_configuration(value: Value) -> Result<Map<String, Value>, String> {
    let mut configuration = value
        .as_object()
        .cloned()
        .ok_or("preset configuration must be an object")?;

    for field in ["name", "device_name"] {
        if field == "name" || configuration.contains_key(field) {
            let name = configuration
                .get(field)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{field} must be a non-empty string"))?;

            if name.is_empty() || name.contains('\0') {
                return Err(format!("{field} must be a non-empty string without NUL"));
            }
        }
    }

    if let Some(identity) = configuration.get("device_identity") {
        let identity = identity
            .as_object()
            .ok_or("device_identity must be an object")?;
        let mut specified = false;

        for field in ["server_name", "mac_address", "inventory_id"] {
            if let Some(value) = identity.get(field) {
                let value = value
                    .as_str()
                    .ok_or("device identity values must be strings")?;
                specified |= !value.is_empty();
            }
        }

        if !specified {
            return Err(
                "device_identity must contain server_name, mac_address, or inventory_id".into(),
            );
        }
    }

    if let Some(value) = configuration.get_mut("interfaces") {
        if value.is_null() {
            *value = json!([]);
        }

        let interfaces = value.as_array_mut().ok_or("interfaces must be a list")?;
        let mut identities = BTreeSet::new();

        for (index, value) in interfaces.iter_mut().enumerate() {
            let interface = value
                .as_object_mut()
                .ok_or("interface entries must be objects")?;
            let default_identity = match index {
                0 => "primary".into(),
                1 => "secondary".into(),
                _ => index.to_string(),
            };
            let identity = interface
                .entry("identity")
                .or_insert(Value::String(default_identity))
                .as_str()
                .ok_or("interface identity must be a string")?
                .to_owned();

            if identity.is_empty() || !identities.insert(identity) {
                return Err("interface identities must be distinct non-empty strings".into());
            }

            match interface.get("mode").and_then(Value::as_str) {
                Some("dynamic" | "dhcp") => {}
                Some("static") => {
                    for field in ["ip_address", "netmask", "gateway", "dns_server"] {
                        if !interface
                            .get(field)
                            .and_then(Value::as_str)
                            .is_some_and(|text| !text.is_empty())
                        {
                            return Err(format!("static interface is missing {field}"));
                        }
                    }
                }
                _ => return Err("interface mode must be dynamic, dhcp or static".into()),
            }
        }
    }

    for field in [
        "transmitter_channel_names",
        "receiver_channel_names",
        "rx_subscriptions",
    ] {
        if let Some(value) = configuration.get_mut(field) {
            if value.is_null() {
                *value = json!({});
            }

            let entries = value
                .as_object()
                .ok_or_else(|| format!("{field} must be an object"))?;
            let mut normalized = Map::new();

            for (number, value) in entries {
                let number = number
                    .parse::<std::num::NonZeroU16>()
                    .map_err(|_| format!("{field}: invalid channel identifier"))?
                    .to_string();
                let mut value = value.clone();

                if field == "rx_subscriptions" {
                    if !value.is_null() {
                        let subscription = value
                            .as_object_mut()
                            .ok_or("receiver subscriptions must be objects or null")?;
                        subscription.entry("kind").or_insert(json!("native_dante"));
                    }
                } else if !value.is_string() {
                    return Err(format!("{field}: channel names must be strings"));
                }

                if normalized.insert(number, value).is_some() {
                    return Err(format!("{field}: duplicate channel identifier"));
                }
            }

            *value = Value::Object(normalized);
        }
    }

    if let Some(value) = configuration.get_mut("transmit_flows") {
        let flows = value
            .as_array_mut()
            .ok_or("transmit_flows must be a list")?;

        for value in flows {
            let specification: TransmitFlowSpecification = serde_json::from_value(value.clone())
                .map_err(|error| format!("transmit_flows: {error}"))?;
            specification.validate()?;
            *value = serde_json::to_value(specification).map_err(|error| error.to_string())?;
        }
    }

    if let Some(value) = configuration.get("device_controls") {
        let controls = value
            .as_object()
            .ok_or("device_controls must contain a settings object")?;

        if let Some(settings) = controls.get("settings") {
            let settings = settings
                .as_object()
                .ok_or("device_controls must contain a settings object")?;

            if settings
                .keys()
                .any(|name| name.to_lowercase().contains("pair"))
            {
                return Err("pairing-list clearing cannot be saved or applied in a preset".into());
            }
        }
    }

    if let Some(value) = configuration.get_mut("clock_subdomain") {
        *value = crate::clock_configuration::normalize_subdomain(value)?;
    }

    if configuration
        .get("unknown_fields")
        .is_some_and(|value| !value.is_object())
    {
        return Err("unknown_fields must be an object".into());
    }

    crate::configuration::validate(Value::Object(configuration.clone()))?;
    Ok(configuration)
}
