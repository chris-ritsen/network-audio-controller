use super::{Records, channel_label, channels, compact, last_seen, text};
use serde_json::{Map, Value, json};

pub fn signal_text(reading: &Value) -> String {
    let state = match reading["state"].as_str() {
        Some("signal_present") => "signal",
        Some("below_threshold") => "very quiet",
        Some("mute_or_floor" | "muted") => "silent",
        Some(state) => state,
        None => "unknown",
    };

    if let Some(level) = reading["dbfs"].as_f64() {
        if state == "signal" {
            return format!("{level} dBFS");
        }

        if matches!(state, "very quiet" | "clipping") {
            return format!("{state} ({level} dBFS)");
        }
    }

    state.to_owned()
}

pub fn channel_signals(payload: &mut Value, cache: &Value, records: &Records) {
    let Some(channels) = payload["channels"].as_array_mut() else {
        return;
    };
    let mut unknown = false;

    for channel in channels {
        if channel.get("number").is_none() {
            continue;
        }

        let record = channel["device"]
            .as_str()
            .and_then(|name| super::resolve(records, name).ok().flatten());
        let source = record.and_then(|record| record["server_name"].as_str());
        let direction = channel["channel_type"].as_str().unwrap_or("");
        let number = text(&channel["number"]);
        let signal = source
            .map(|source| signal_text(&cache[source][format!("{direction}_normalized")][&number]))
            .unwrap_or_else(|| "unknown".to_owned());
        unknown |= signal == "unknown";
        channel["signal"] = json!(signal);
    }

    if unknown {
        payload["signal_note"] =
            json!("unknown means no recent meter reading; get_signal_levels samples the device");
    }
}

pub fn signal_levels(
    cache: &Value,
    records: &Records,
    selected: Option<&Value>,
    debug: bool,
) -> Value {
    if let Some(record) = selected
        && record["server_name"]
            .as_str()
            .is_none_or(|name| cache.get(name).is_none())
        && let Some(seen) = last_seen(record)
    {
        return json!({text(&record["name"]): format!("no levels: it is off the network, {seen}")});
    }

    let mut devices = Map::new();

    for (server, record) in records {
        if selected.is_some_and(|selected| selected["server_name"] != record["server_name"]) {
            continue;
        }

        let Some(sample) = cache.get(server) else {
            continue;
        };
        let mut device = json!({
            "metering_source": sample["metering_source"],
            "observed_at": if debug {
                sample["observed_at"].clone()
            } else {
                json!(super::local_time(&sample["observed_at"]))
            },
        });

        for (direction, inventory, count) in [
            ("rx", "receivers", "rx_count"),
            ("tx", "transmitters", "tx_count"),
        ] {
            let Some(normalized) = sample[format!("{direction}_normalized")].as_object() else {
                continue;
            };
            let channels = &record["channels"][inventory];
            let mut readings = Map::new();

            for (number, reading) in normalized {
                if let (Ok(number), Some(count)) = (number.parse::<u64>(), record[count].as_u64())
                    && number > count
                {
                    continue;
                }

                let state = if reading["state"] == "muted" {
                    json!("mute_or_floor")
                } else {
                    reading["state"].clone()
                };
                let mut value = json!({
                    "state": state,
                    "dbfs": reading["dbfs"],
                    "name": channel_label(&channels[number]),
                });

                if debug {
                    value["raw"] = reading["raw"].clone();
                }

                readings.insert(number.clone(), compact(value));
            }

            device[direction] = if debug || selected.is_none() {
                Value::Object(readings)
            } else {
                compact_channels(&readings, channels)
            };
        }

        devices.insert(text(&record["name"]), compact(device));
    }

    if !debug && selected.is_none() {
        network_levels(devices)
    } else {
        compact(Value::Object(devices))
    }
}

fn compact_channels(readings: &Map<String, Value>, records: &Value) -> Value {
    let mut signal = Map::new();
    let mut quiet_names = Vec::new();
    let mut quiet_numbers = Vec::new();
    let mut silent_names = Vec::new();
    let mut silent_numbers = Vec::new();
    let mut sorted: Vec<_> = readings.iter().collect();
    sorted.sort_by_key(|(number, _)| number.parse::<u64>().unwrap_or_default());

    for (number, reading) in sorted {
        let label = reading["name"].as_str().unwrap_or("");
        let custom = !label.is_empty() && !channels::factory_named(number, &records[number]);
        let label = if custom {
            format!("{number} {label}")
        } else {
            number.clone()
        };
        let description = signal_text(reading);
        let group = if description == "silent" {
            Some((&mut silent_names, &mut silent_numbers))
        } else if description.starts_with("very quiet") {
            Some((&mut quiet_names, &mut quiet_numbers))
        } else {
            None
        };

        if let Some((names, numbers)) = group {
            if custom {
                names.push(label);
            } else if let Ok(number) = number.parse::<u64>() {
                numbers.push(number);
            }
        } else {
            signal.insert(label, json!(description));
        }
    }

    compact(json!({
        "with_signal": if signal.is_empty() { Value::Null } else { Value::Object(signal) },
        "very_quiet": groups(quiet_names, quiet_numbers),
        "silent": groups(silent_names, silent_numbers),
    }))
}

fn groups(mut names: Vec<String>, numbers: Vec<u64>) -> Value {
    if !numbers.is_empty() {
        names.push(channels::number_ranges(numbers));
    }

    if names.is_empty() {
        Value::Null
    } else {
        json!(names.join(", "))
    }
}

fn network_levels(devices: Map<String, Value>) -> Value {
    let mut summary = Map::new();
    let mut silent_devices = Vec::new();

    for (name, device) in devices {
        let mut directions = Map::new();
        let mut audible = false;
        let mut entirely_silent = true;
        let mut observed_channels = 0;

        for direction in ["rx", "tx"] {
            let mut active = Vec::new();
            let mut silent = 0;
            let mut quiet = 0;
            let mut unknown = 0;
            let readings = device[direction].as_object().into_iter().flatten();

            for (number, reading) in readings {
                observed_channels += 1;
                match reading["state"].as_str() {
                    Some("signal_present" | "clipping") => {
                        let label = reading["name"]
                            .as_str()
                            .filter(|name| !name.is_empty())
                            .map(|name| format!("{number} {name}"))
                            .unwrap_or_else(|| number.clone());
                        active.push(format!("{label} {}", signal_text(reading)));
                    }
                    Some("mute_or_floor" | "muted") => silent += 1,
                    Some("below_threshold") => quiet += 1,
                    _ => unknown += 1,
                }
            }

            audible |= !active.is_empty();
            entirely_silent &= active.is_empty() && quiet == 0 && unknown == 0;
            directions.insert(
                direction.to_owned(),
                compact(json!({
                    "signal": (!active.is_empty()).then_some(active),
                    "silent_channels": (silent != 0).then_some(silent),
                    "very_quiet_channels": (quiet != 0).then_some(quiet),
                    "unknown_channels": (unknown != 0).then_some(unknown),
                })),
            );
        }

        if entirely_silent && observed_channels > 0 {
            silent_devices.push(name);
            continue;
        }

        directions.insert("observed_at".to_owned(), device["observed_at"].clone());
        directions.insert("has_signal".to_owned(), json!(audible));
        if observed_channels == 0 {
            directions.insert(
                "unavailable".to_owned(),
                json!("no channel measurements in this sample"),
            );
        }
        summary.insert(name, Value::Object(directions));
    }

    compact(json!({
        "dante": if summary.is_empty() { json!("no recent Dante signal observed") } else { Value::Object(summary) },
        "silent_dante_devices": (!silent_devices.is_empty()).then(|| silent_devices.join(", ")),
        "detail": "get_signal_levels with a device lists every channel's state and level",
    }))
}
