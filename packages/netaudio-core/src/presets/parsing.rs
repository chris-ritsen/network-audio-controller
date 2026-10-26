use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use super::xml::{Content, Element};
use super::{
    normalize_configuration, Device, Document, EXTENSION, MAXIMUM_DEVICES, SCHEMA_VERSION,
};

const IDENTITY_TAGS: &[&str] = &[
    "name",
    "default_name",
    "instance_id",
    "manufacturer_id",
    "manufacturer_name",
    "model_id",
    "model_name",
    "model_version",
    "device_type",
    "device_type_string",
    "friendly_name",
];
const SETTING_TAGS: &[&str] = &[
    "preferred_master",
    "samplerate",
    "encoding",
    "unicast_latency",
    "interface",
    "txchannel",
    "rxchannel",
    EXTENSION,
];

pub(super) fn parse(content: &str) -> Result<Document, String> {
    let root = super::xml::parse(content)?;

    if root.name != "preset" || root.namespace.is_some() {
        return Err("expected a Dante preset XML document".into());
    }

    let name = root.child_text("name")?.unwrap_or_else(|| "unknown".into());
    let mut devices = BTreeMap::new();

    for element in root.children("device") {
        let device = device(element)?;
        let name = device.configuration["name"].as_str().unwrap().to_owned();

        if devices.insert(name.clone(), device).is_some() {
            return Err(format!("duplicate preset device name: {name:?}"));
        }

        if devices.len() > MAXIMUM_DEVICES {
            return Err("a preset must contain at most 128 named devices".into());
        }
    }

    Ok(Document {
        name,
        devices,
        root,
    })
}

fn device(element: &Element) -> Result<Device, String> {
    let name = element
        .child_text("friendly_name")?
        .filter(|value| !value.is_empty())
        .or(element.child_text("name")?)
        .unwrap_or_default();

    if name.is_empty() {
        return Err("preset device must have a non-empty name".into());
    }

    let mut configuration = Map::new();
    configuration.insert("name".into(), json!(name));

    for (tag, field) in [("samplerate", "sample_rate"), ("encoding", "encoding")] {
        if let Some(value) = element.child_text(tag)?.filter(|value| !value.is_empty()) {
            configuration.insert(
                field.into(),
                json!(value
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| format!("{name}: {tag} must be an integer"))?),
            );
        }
    }

    if let Some(preferred) = element.one("preferred_master")? {
        let value = preferred
            .attributes
            .get("value")
            .map(|value| value.trim().to_lowercase());
        configuration.insert(
            "preferred_leader".into(),
            match value.as_deref() {
                Some("true") => json!(true),
                Some("false") => json!(false),
                _ => {
                    return Err(format!(
                        "{name}: preferred_master value must be true or false"
                    ))
                }
            },
        );
    }

    if let Some(value) = element
        .child_text("unicast_latency")?
        .filter(|value| !value.is_empty())
    {
        let microseconds = value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{name}: unicast_latency must be an integer"))?;
        configuration.insert("latency".into(), json!(microseconds as f64 / 1000.0));
    }

    let interfaces = interfaces(element)?;

    if !interfaces.is_empty() {
        configuration.insert("interfaces".into(), json!(interfaces));
    }

    channels(element, &mut configuration)?;

    if let Some(extension) = element.one(EXTENSION)? {
        let version = extension
            .attributes
            .get("schema_version")
            .and_then(|value| value.parse::<u32>().ok());

        if version != Some(SCHEMA_VERSION) {
            return Err(format!(
                "{name}: unsupported NetAudio preset schema version"
            ));
        }

        let content = extension
            .child_text("json")?
            .ok_or_else(|| format!("{name}: NetAudio extension has no JSON configuration"))?;
        let extension = normalize_configuration(
            super::json::parse(&content)
                .map_err(|error| format!("{name}: invalid NetAudio extension JSON: {error}"))?,
        )?;
        validate_projection(&name, &configuration, &extension)?;
        configuration = extension;
    }

    let unknown: Vec<Value> = element
        .contents
        .iter()
        .filter_map(|content| match content {
            Content::Element(child)
                if child.namespace.is_some()
                    || (matches!(child.name.as_str(), "txchannel" | "rxchannel")
                        && !audio_channel(child))
                    || (!IDENTITY_TAGS.contains(&child.name.as_str())
                        && !SETTING_TAGS.contains(&child.name.as_str())) =>
            {
                Some(json!(child.render()))
            }
            _ => None,
        })
        .collect();

    if !unknown.is_empty() || !element.attributes.is_empty() {
        let fields = configuration
            .entry("unknown_fields")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or("unknown_fields must be an object")?;

        if !element.attributes.is_empty() {
            let attributes = fields
                .entry("xml_attributes")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .ok_or("xml_attributes must be an object")?;
            attributes.extend(
                element
                    .attributes
                    .iter()
                    .map(|(key, value)| (key.clone(), json!(value))),
            );
        }

        if !unknown.is_empty() {
            let preserved = fields
                .entry("xml_elements")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .ok_or("xml_elements must be a list")?;

            for value in unknown {
                if !preserved.contains(&value) {
                    preserved.push(value);
                }
            }
        }
    }

    Ok(Device {
        configuration: normalize_configuration(Value::Object(configuration))?,
        element: element.clone(),
    })
}

fn interfaces(element: &Element) -> Result<Vec<Value>, String> {
    let mut interfaces = Vec::new();
    let mut identities = BTreeSet::new();

    for (index, interface) in element.children("interface").enumerate() {
        let network = interface.attributes.get("network").map(String::as_str);
        let identity = match network {
            None | Some("0") if index == 0 => "primary".into(),
            Some("1") => "secondary".into(),
            Some(value) if !value.is_empty() => value.into(),
            _ => index.to_string(),
        };

        if !identities.insert(identity.clone()) {
            return Err(format!("duplicate interface identity: {identity}"));
        }

        let Some(address) = interface.one("ipv4_address")? else {
            continue;
        };
        let mode = address
            .attributes
            .get("mode")
            .map(String::as_str)
            .unwrap_or("dynamic");
        let mut fields = json!({"identity": identity, "mode": mode});

        if mode == "static" {
            for (tag, field) in [
                ("ip_address", "ip_address"),
                ("subnet_mask", "netmask"),
                ("gateway", "gateway"),
                ("dns_server", "dns_server"),
            ] {
                fields[field] = json!(address.child_text(tag)?.unwrap_or_default());
            }
        }

        interfaces.push(fields);
    }

    Ok(interfaces)
}

fn channel_number(element: &Element) -> Result<String, String> {
    let value = element
        .attributes
        .get("danteId")
        .ok_or("channel is missing danteId")?;
    value
        .parse::<std::num::NonZeroU16>()
        .map(|number| number.to_string())
        .map_err(|_| "channel danteId must be a positive 16-bit integer".into())
}

fn channels(element: &Element, configuration: &mut Map<String, Value>) -> Result<(), String> {
    for (tag, field, label) in [
        ("txchannel", "transmitter_channel_names", "label"),
        ("rxchannel", "receiver_channel_names", "name"),
    ] {
        let mut names = Map::new();
        let mut subscriptions = Map::new();
        let mut identifiers = BTreeSet::new();

        for channel in element
            .children(tag)
            .filter(|channel| audio_channel(channel))
        {
            let identifier = channel_number(channel)?;

            if !identifiers.insert(identifier.clone()) {
                return Err(format!("duplicate {tag} danteId {identifier}"));
            }

            if let Some(name) = channel.child_text(label)? {
                names.insert(identifier.clone(), json!(name));
            }

            if tag == "rxchannel" {
                let transmitter = channel
                    .child_text("subscribed_device")?
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
                let name = channel
                    .child_text("subscribed_channel")?
                    .unwrap_or_default()
                    .trim()
                    .to_owned();

                if name.is_empty() && !transmitter.is_empty() {
                    return Err(format!(
                        "receiver channel {identifier} has a subscribed device without a channel"
                    ));
                }

                subscriptions.insert(identifier, if name.is_empty() {
                    Value::Null
                } else {
                    json!({"tx_channel": name, "tx_device": if transmitter.is_empty() { "." } else { &transmitter }})
                });
            }
        }

        if !names.is_empty() {
            configuration.insert(field.into(), json!(names));
        }

        if !subscriptions.is_empty() {
            configuration.insert("rx_subscriptions".into(), json!(subscriptions));
        }
    }

    Ok(())
}

pub(super) fn audio_channel(element: &Element) -> bool {
    element
        .attributes
        .get("mediaType")
        .is_none_or(|value| value == "audio")
}

fn validate_projection(
    name: &str,
    conventional: &Map<String, Value>,
    extension: &Map<String, Value>,
) -> Result<(), String> {
    for (field, conventional) in conventional {
        let value = extension
            .get(field)
            .ok_or_else(|| format!("{name}: NetAudio extension omits XML field {field}"))?;
        let projected = match field.as_str() {
            "interfaces" => interface_projection(value)?,
            "rx_subscriptions" => Value::Object(value.as_object().ok_or("rx_subscriptions must be an object")?.iter().map(|(number, subscription)| {
                let projected = if subscription.get("kind").and_then(Value::as_str) == Some("native_dante") {
                    json!({"tx_channel": subscription["tx_channel"], "tx_device": subscription["tx_device"]})
                } else { Value::Null };
                (number.clone(), projected)
            }).collect()),
            _ => value.clone(),
        };

        let matches = match field.as_str() {
            "latency" => projected.as_f64() == conventional.as_f64(),
            "interfaces" => projected == interface_projection(conventional)?,
            _ => &projected == conventional,
        };

        if !matches {
            return Err(format!(
                "{name}: conflicting {field} values in XML and NetAudio extension"
            ));
        }
    }

    Ok(())
}

fn interface_projection(value: &Value) -> Result<Value, String> {
    let interfaces = value.as_array().ok_or("interfaces must be a list")?;
    let fields = [
        "identity",
        "mode",
        "ip_address",
        "netmask",
        "gateway",
        "dns_server",
    ];
    let mut projected = Vec::new();

    for interface in interfaces {
        let interface = interface
            .as_object()
            .ok_or("interfaces must contain objects")?;
        let mut selected = Map::new();

        for field in fields {
            if let Some(value) = interface.get(field) {
                let value = if field == "mode" && value == "dhcp" {
                    json!("dynamic")
                } else {
                    value.clone()
                };
                selected.insert(field.into(), value);
            }
        }

        projected.push(Value::Object(selected));
    }

    Ok(Value::Array(projected))
}
