use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use super::xml::{Content, Element};
use super::{Device, Document, XmlIdentity, EXTENSION, MAXIMUM_BYTES, SCHEMA_VERSION};

pub(super) fn serialize(document: &Document) -> Result<String, String> {
    let mut root = document.root.clone();
    root.remove("device");
    root.set_text("name", &document.name);

    for device in document.devices.values() {
        root.push(device_element(device)?);
    }

    let content = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n{}",
        root.render()
    );

    if content.len() > MAXIMUM_BYTES {
        return Err("preset XML must be no larger than 4 MiB".into());
    }

    super::parsing::parse(&content)?;
    Ok(content)
}

pub(super) fn identity_element(name: &str, identity: XmlIdentity) -> Result<Element, String> {
    let mut element = Element::new("device");
    element.set_text("name", name);
    let server = identity.server_name.as_deref().unwrap_or_default();
    element.set_text(
        "default_name",
        server.strip_suffix(".local.").unwrap_or(server),
    );
    let mut instance = Element::new("instance_id");

    if let Some(address) = identity.mac_address.filter(|value| !value.is_empty()) {
        let mut address = address.replace([':', '-'], "").to_uppercase();

        if ![12, 16].contains(&address.len())
            || !address.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(
                "preset device MAC identity must contain six or eight hexadecimal bytes".into(),
            );
        }

        if address.len() == 12 {
            address.insert_str(6, "FFFE");
        }

        instance.set_text("device_id", address);
    }

    instance.set_text("process_id", "0");
    element.push(instance);

    if let Some(manufacturer) = identity.manufacturer.filter(|value| !value.is_empty()) {
        element.set_text("manufacturer_id", hex_identity(&manufacturer));
        element.set_text("manufacturer_name", manufacturer);
    }

    if let Some(identifier) = identity.model_identifier.filter(|value| !value.is_empty()) {
        let encoded = hex_identity(&identifier);
        element.set_text("model_id", &encoded);
        element.set_text(
            "model_name",
            identity.model_name.unwrap_or_else(|| identifier.clone()),
        );
        element.set_text("device_type", encoded);
        element.set_text("device_type_string", identifier);

        if let Some(version) = identity.model_version {
            element.set_text("model_version", version);
        }
    }

    element.set_text("friendly_name", name);
    Ok(element)
}

fn hex_identity(value: &str) -> String {
    let mut encoded = value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();

    while encoded.len() < 16 {
        encoded.push('0');
    }

    encoded
}

fn device_element(device: &Device) -> Result<Element, String> {
    let configuration = &device.configuration;
    let mut element = device.element.clone();
    element.set_text("friendly_name", configuration["name"].as_str().unwrap());

    for (field, tag) in [("sample_rate", "samplerate"), ("encoding", "encoding")] {
        if let Some(value) = configuration.get(field) {
            element.set_text(tag, value.to_string());
        }
    }

    if let Some(value) = configuration
        .get("preferred_leader")
        .filter(|value| !value.is_null())
    {
        let mut preferred = element
            .one("preferred_master")?
            .cloned()
            .unwrap_or_else(|| Element::new("preferred_master"));
        preferred
            .attributes
            .insert("value".into(), value.to_string());
        element.remove("preferred_master");
        element.push(preferred);
    }

    if let Some(latency) = configuration.get("latency") {
        let milliseconds = latency.as_f64().ok_or("latency must be a number")?;
        let microseconds = milliseconds * 1000.0;

        if !microseconds.is_finite()
            || microseconds < 0.0
            || microseconds > u32::MAX as f64
            || microseconds.round() / 1000.0 != milliseconds
        {
            return Err("preset latency must be representable as whole microseconds".into());
        }

        element.set_text("unicast_latency", (microseconds.round() as u32).to_string());
    }

    interfaces(&mut element, configuration)?;
    channels(&mut element, configuration)?;
    preserve_unknown(&mut element, configuration.get("unknown_fields"))?;
    element.remove(EXTENSION);
    let mut extension = Element::new(EXTENSION);
    extension
        .attributes
        .insert("schema_version".into(), SCHEMA_VERSION.to_string());
    extension.set_text(
        "json",
        serde_json::to_string(configuration).map_err(|error| error.to_string())?,
    );
    element.push(extension);
    Ok(element)
}

fn interfaces(element: &mut Element, configuration: &Map<String, Value>) -> Result<(), String> {
    let Some(interfaces) = configuration.get("interfaces").and_then(Value::as_array) else {
        return Ok(());
    };
    let original: Vec<_> = element.children("interface").cloned().collect();
    element.remove("interface");

    for configuration in interfaces {
        let identity = configuration["identity"].as_str().unwrap();
        let network = match identity {
            "primary" => "0",
            "secondary" => "1",
            value => value,
        };
        let mut interface = original
            .iter()
            .enumerate()
            .find(|(position, interface)| {
                interface
                    .attributes
                    .get("network")
                    .map(String::as_str)
                    .unwrap_or(if *position == 0 { "0" } else { "" })
                    == network
            })
            .map(|(_, element)| element.clone())
            .unwrap_or_else(|| Element::new("interface"));
        interface
            .attributes
            .insert("network".into(), network.into());
        let mut address = interface
            .one("ipv4_address")?
            .cloned()
            .unwrap_or_else(|| Element::new("ipv4_address"));
        let mode = configuration["mode"].as_str().unwrap();
        address.attributes.insert("mode".into(), mode.into());

        for (field, tag) in [
            ("ip_address", "ip_address"),
            ("netmask", "subnet_mask"),
            ("gateway", "gateway"),
            ("dns_server", "dns_server"),
        ] {
            if mode == "static" {
                address.set_text(tag, configuration[field].as_str().unwrap());
            }
        }

        interface.remove("ipv4_address");
        interface.push(address);
        preserve_unknown(&mut interface, configuration.get("unknown_fields"))?;
        element.push(interface);
    }

    for interface in original {
        if interface.one("ipv4_address")?.is_none() {
            element.push(interface);
        }
    }

    Ok(())
}

fn channels(element: &mut Element, configuration: &Map<String, Value>) -> Result<(), String> {
    let empty = Map::new();
    let subscriptions = configuration
        .get("rx_subscriptions")
        .and_then(Value::as_object)
        .unwrap_or(&empty);

    for (tag, field, label) in [
        ("txchannel", "transmitter_channel_names", "label"),
        ("rxchannel", "receiver_channel_names", "name"),
    ] {
        let names = configuration
            .get(field)
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let original: BTreeMap<u16, Element> = element
            .children(tag)
            .filter(|channel| super::parsing::audio_channel(channel))
            .map(|channel| {
                channel
                    .attributes
                    .get("danteId")
                    .ok_or("channel is missing danteId")
                    .and_then(|value| value.parse::<u16>().map_err(|_| "invalid channel danteId"))
                    .map(|number| (number, channel.clone()))
            })
            .collect::<Result<_, _>>()?;
        let mut identifiers: BTreeSet<u16> = original.keys().copied().collect();
        identifiers.extend(names.keys().map(|value| value.parse::<u16>().unwrap()));

        if tag == "rxchannel" {
            identifiers.extend(
                subscriptions
                    .keys()
                    .map(|value| value.parse::<u16>().unwrap()),
            );
        }

        let preserved: Vec<_> = element
            .children(tag)
            .filter(|channel| !super::parsing::audio_channel(channel))
            .cloned()
            .collect();
        element.remove(tag);

        for number in identifiers {
            let number_text = number.to_string();
            let mut channel = original
                .get(&number)
                .cloned()
                .unwrap_or_else(|| Element::new(tag));
            channel
                .attributes
                .insert("danteId".into(), number_text.clone());
            channel
                .attributes
                .entry("mediaType".into())
                .or_insert_with(|| "audio".into());

            if let Some(name) = names.get(&number_text).and_then(Value::as_str) {
                channel.set_text(label, name);
            }

            if tag == "rxchannel" {
                if let Some(subscription) = subscriptions.get(&number_text) {
                    channel.remove("subscribed_device");
                    channel.remove("subscribed_channel");

                    if subscription.get("kind").and_then(Value::as_str) == Some("native_dante") {
                        channel.set_text(
                            "subscribed_device",
                            subscription["tx_device"].as_str().unwrap(),
                        );
                        channel.set_text(
                            "subscribed_channel",
                            subscription["tx_channel"].as_str().unwrap(),
                        );
                    }
                }
            }

            element.push(channel);
        }

        for channel in preserved {
            element.push(channel);
        }
    }

    Ok(())
}

fn preserve_unknown(element: &mut Element, unknown: Option<&Value>) -> Result<(), String> {
    let Some(unknown) = unknown else {
        return Ok(());
    };

    if let Some(attributes) = unknown.get("xml_attributes") {
        for (name, value) in attributes
            .as_object()
            .ok_or("xml_attributes must be an object")?
        {
            let value = value.as_str().ok_or("XML attributes must be strings")?;

            if element
                .attributes
                .get(name)
                .is_some_and(|existing| existing != value)
            {
                return Err(format!("conflicting preserved XML attribute: {name}"));
            }

            element.attributes.insert(name.clone(), value.into());
        }
    }

    if let Some(elements) = unknown.get("xml_elements") {
        for value in elements.as_array().ok_or("xml_elements must be a list")? {
            let fragment = super::xml::parse(
                value
                    .as_str()
                    .ok_or("XML extension elements must be strings")?,
            )?;

            if !element.contents.iter().any(|content| matches!(content, Content::Element(existing) if existing.render() == fragment.render())) {
                element.push(fragment);
            }
        }
    }

    Ok(())
}
