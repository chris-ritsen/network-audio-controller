use super::Records;
use netaudio_core::shure::{Protocol, described};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

pub fn view(
    payload: &Value,
    records: &Records,
    correlations: &crate::shure::correlations::Snapshot,
) -> Value {
    let mut feeds: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for record in records.values() {
        for subscription in super::subscriptions(record) {
            if let (Some(device), Some(channel)) = (
                subscription["tx_device"].as_str(),
                subscription["tx_channel"].as_str(),
            ) {
                feeds
                    .entry((device.to_owned(), channel.to_owned()))
                    .or_default()
                    .push(format!(
                        "{}:{}",
                        record["name"].as_str().unwrap_or(""),
                        subscription["rx_channel"].as_str().unwrap_or("")
                    ));
            }
        }
    }
    for destinations in feeds.values_mut() {
        destinations.sort();
    }
    let mut devices = Vec::new();
    for (mac, device) in payload.as_object().into_iter().flatten() {
        let protocol = match device["device_type"].as_str() {
            Some("ad4d") => Protocol::Ad4d,
            Some("p10t") => Protocol::P10t,
            _ => continue,
        };
        let dante = crate::shure::correlations::linked_device(mac, device, records, correlations);
        let mut channels = Vec::new();
        for (number, channel) in device["channels"].as_object().into_iter().flatten() {
            let mut value = channel_view(protocol, channel);
            value["number"] = number
                .parse::<u64>()
                .map_or_else(|_| json!(number), |number| json!(number));
            if let Some(dante) = dante {
                let output = super::channel_label(&dante["channels"]["transmitters"][number]);
                if let (Some(name), Some(label)) = (dante["name"].as_str(), output.as_str()) {
                    value["dante_output"] = json!(format!("{name}:{label}"));
                    let destinations = feeds.get(&(name.to_owned(), label.to_owned()));
                    value["feeds"] = destinations
                        .filter(|destinations| !destinations.is_empty())
                        .map_or(json!("nothing"), |destinations| json!(destinations));
                }
            }
            channels.push(value);
        }
        channels.sort_by_key(|channel| channel["number"].as_u64());
        let settings: Map<_, _> = device
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(key, _)| {
                !matches!(
                    key.as_str(),
                    "name"
                        | "device_type"
                        | "model"
                        | "online"
                        | "ip"
                        | "mac"
                        | "firmware_version"
                        | "last_seen"
                        | "channels"
                )
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        devices.push(super::compact(json!({
            "name": device["name"],
            "type": protocol.name(),
            "model": device["model"],
            "online": device["online"],
            "ip": device["ip"],
            "mac": mac,
            "firmware": device["firmware_version"],
            "last_seen": super::local_time(&device["last_seen"]),
            "dante_device": dante.map(|record| &record["name"]),
            "settings": settings,
            "channels": channels,
        })));
    }
    devices.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    let mut result = json!({"devices": devices});
    let conditions = conditions(payload);
    if !conditions.is_empty() {
        result["conditions"] = json!(conditions);
    }
    if let Some(error) = &correlations.error {
        result["correlation_error"] = json!(error);
    }
    result
}

pub fn conditions(devices: &Value) -> Vec<String> {
    let mut conditions = Vec::new();
    for (_, device) in devices.as_object().into_iter().flatten() {
        if device["online"] != true {
            continue;
        }
        for (number, channel) in device["channels"].as_object().into_iter().flatten() {
            let label = format!(
                "{} ch{number} {}",
                device["name"].as_str().unwrap_or(""),
                channel["name"].as_str().unwrap_or("")
            )
            .trim()
            .to_owned();
            if channel["active"] == true {
                let transmitter = &channel["transmitter"];
                let percent = transmitter["battery_charge_percent"].as_i64();
                let minutes = transmitter["battery_minutes"].as_i64();
                if percent.is_some_and(|value| (0..=25).contains(&value))
                    || minutes.is_some_and(|value| (0..=60).contains(&value))
                {
                    let remaining = percent
                        .filter(|value| (0..=100).contains(value))
                        .map(|percent| format!("{percent}%"))
                        .or_else(|| {
                            minutes.map(|minutes| format!("{}h{:02}m", minutes / 60, minutes % 60))
                        })
                        .unwrap_or_default();
                    conditions.push(format!("{label}: transmitter battery low ({remaining})"));
                }
                if transmitter["mute_status"].as_str().is_some_and(|value| {
                    matches!(value.to_ascii_uppercase().as_str(), "ON" | "MUTE" | "MUTED")
                }) {
                    conditions.push(format!("{label}: transmitter muted"));
                }
            }
            if let Some(interference) = channel["interference_status"]
                .as_str()
                .filter(|value| !matches!(*value, "" | "NONE"))
            {
                conditions.push(format!("{label}: interference {interference}"));
            }
            if device["encryption_mode"]
                .as_str()
                .is_some_and(|value| value != "OFF")
                && let Some(encryption) = channel["encryption_status"]
                    .as_str()
                    .filter(|value| !matches!(*value, "" | "OK"))
            {
                conditions.push(format!("{label}: encryption {encryption}"));
            }
            if channel["audio_mute"] == true {
                conditions.push(format!("{label}: audio muted on the receiver"));
            }
        }
    }
    conditions
}

fn channel_view(protocol: Protocol, channel: &Value) -> Value {
    let mut value = json!({"name": channel["name"]});
    if let Some(active) = channel["active"].as_bool() {
        value["transmitter"] = json!(if active {
            "receiving a transmitter"
        } else {
            "no transmitter signal"
        });
    }
    if let Some(frequency) = channel["frequency"].as_f64() {
        value["frequency_mhz"] = json!(frequency / 1000.0);
    }
    if channel["active"] != false
        && let Some(level) = level(channel)
    {
        value["level"] = json!(level);
    }
    for (key, field) in channel.as_object().into_iter().flatten() {
        if matches!(
            key.as_str(),
            "name"
                | "active"
                | "frequency"
                | "audio_level_rms"
                | "audio_level_peak"
                | "audio_in_level_l"
                | "audio_in_level_r"
        ) || (key == "signal_quality" && channel["active"] == false)
        {
            continue;
        }
        let label = match key.as_str() {
            "audio_gain" => "gain",
            "audio_mute" => "mute",
            "audio_in_level" => "input_level",
            "audio_in_line_level" => "line_level",
            "rf_tx_level" => "rf_power",
            "audio_tx_mode" => "transmit_mode",
            key => key,
        };
        value[label] = if key == "audio_gain" && protocol == Protocol::Ad4d {
            described(protocol, "AUDIO_GAIN", &field.to_string())
        } else {
            field.clone()
        };
    }
    value
}

pub fn level(channel: &Value) -> Option<String> {
    if let Some(root_mean_square) = channel["audio_level_rms"].as_i64() {
        let peak = channel["audio_level_peak"]
            .as_i64()
            .map_or(String::new(), |peak| format!(", peak {} dBFS", peak - 120));
        return Some(format!("RMS {} dBFS{peak}", root_mean_square - 120));
    }
    let left = channel["audio_in_level_l"].as_i64();
    let right = channel["audio_in_level_r"].as_i64();
    (left.is_some() || right.is_some()).then(|| {
        format!(
            "input meter left {}, right {} in the P10T's own units; larger is louder",
            left.map_or("unknown".to_owned(), |value| value.to_string()),
            right.map_or("unknown".to_owned(), |value| value.to_string())
        )
    })
}

pub fn remembered(records: BTreeMap<String, Value>) -> Vec<Value> {
    let mut devices: Vec<_> = records
        .into_iter()
        .map(|(mac, record)| {
            super::compact(json!({
                "mac": mac,
                "name": record["name"],
                "type": record["device_type"],
                "model": record["model"],
                "state": "off the network",
                "last_seen": super::local_time(&record["last_seen"]),
            }))
        })
        .collect();
    devices.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    devices
}
