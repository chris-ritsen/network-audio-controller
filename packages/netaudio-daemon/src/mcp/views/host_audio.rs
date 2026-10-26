use super::{local_time, text};
use crate::mcp::service::CallError;
use netaudio_core::host_audio::cards::{self, Bridge, Card, Channel};
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

fn entries(value: &Value) -> impl Iterator<Item = &Value> {
    value.as_array().into_iter().flatten()
}

fn known(value: &Value) -> String {
    if value.is_null() {
        "unknown".to_owned()
    } else {
        text(value)
    }
}

fn joined(value: &Value, separator: &str) -> String {
    entries(value).map(text).collect::<Vec<_>>().join(separator)
}

fn natural(left: &str, right: &str) -> Ordering {
    let mut left = left.chars().peekable();
    let mut right = right.chars().peekable();
    loop {
        match (left.peek(), right.peek()) {
            (Some(first), Some(second)) if first.is_ascii_digit() && second.is_ascii_digit() => {
                let mut first = String::new();
                let mut second = String::new();
                while left.peek().is_some_and(char::is_ascii_digit) {
                    first.push(left.next().expect("peeked character"));
                }
                while right.peek().is_some_and(char::is_ascii_digit) {
                    second.push(right.next().expect("peeked character"));
                }
                let order = first
                    .trim_start_matches('0')
                    .len()
                    .cmp(&second.trim_start_matches('0').len())
                    .then_with(|| {
                        first
                            .trim_start_matches('0')
                            .cmp(second.trim_start_matches('0'))
                    })
                    .then_with(|| first.len().cmp(&second.len()));
                if order != Ordering::Equal {
                    return order;
                }
            }
            _ => {
                let first = left.next();
                let second = right.next();
                let order = first.cmp(&second);
                if order != Ordering::Equal || first.is_none() {
                    return order;
                }
            }
        }
    }
}

fn label(name: &str, port: &Value) -> String {
    let aliases = entries(&port["aliases"])
        .filter_map(Value::as_str)
        .filter(|alias| !alias.starts_with("alsa_pcm:"))
        .collect::<Vec<_>>();
    if aliases.is_empty() {
        name.to_owned()
    } else {
        format!("{name} [{}]", aliases.join(", "))
    }
}

fn hardware(
    name: &str,
    port: &Value,
    cards: &[Card],
    bridges: &BTreeMap<String, Bridge>,
) -> Option<Channel> {
    let aliases = entries(&port["aliases"])
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    cards::hardware_aliases(&aliases, cards).or_else(|| cards::bridge_channel(name, bridges))
}

fn jack_line(jack: &Value) -> String {
    if jack["available"] != true {
        return format!("not available: {}", known(&jack["reason"]));
    }
    let rate = jack["sample_rate"].as_f64();
    let frames = jack["buffer_size"].as_f64();
    let latency = rate
        .zip(frames)
        .filter(|(rate, _)| *rate > 0.0)
        .map(|(rate, frames)| format!(" ({:.1} ms)", frames / rate * 1000.0))
        .unwrap_or_default();
    let clients = jack["ports"]
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(_, port)| port["client"].as_str())
        .collect::<BTreeSet<_>>()
        .len();
    format!(
        "{} kHz, {} frames{latency}, DSP load {}%, {clients} clients, {} xruns since netaudio began watching at {}{}",
        rate.map(|rate| (rate / 1000.0).to_string())
            .unwrap_or_else(|| "unknown".into()),
        known(&jack["buffer_size"]),
        known(&jack["cpu_load"]),
        known(&jack["xruns"]),
        local_time(&jack["xruns_since"]).unwrap_or_else(|| "an unknown time".into()),
        local_time(&jack["last_xrun"])
            .map(|time| format!(", last at {time}"))
            .unwrap_or_default()
    )
}

fn pulse_line(pulse: &Value) -> String {
    if pulse["available"] != true {
        return format!("not available: {}", known(&pulse["reason"]));
    }
    let server = &pulse["server"];
    format!(
        "{} {}; default output {}, default input {}; {} playback and {} recording streams",
        known(&server["name"]),
        known(&server["version"]),
        known(&server["default_sink"]),
        known(&server["default_source"]),
        entries(&pulse["sink_inputs"]).count(),
        entries(&pulse["source_outputs"]).count()
    )
}

fn stream_line(kind: &str, entry: &Value) -> String {
    let application = entry["application"]
        .as_str()
        .or_else(|| entry["binary"].as_str())
        .unwrap_or("unknown application");
    let media = entry["media"]
        .as_str()
        .filter(|media| *media != application && !media.is_empty())
        .map(|media| format!(" “{media}”"))
        .unwrap_or_default();
    let mut states = Vec::new();
    if entry["corked"] == true {
        states.push("paused".to_owned());
    }
    match entry["mute"].as_bool() {
        Some(true) => states.push("muted".to_owned()),
        None => states.push("mute unknown".to_owned()),
        _ => (),
    }
    if entries(&entry["volume_percent"]).next().is_some() {
        states.push(format!("{}%", joined(&entry["volume_percent"], "/")));
    }
    let (direction, device) = if kind == "playback" {
        ("→", &entry["sink"])
    } else {
        ("←", &entry["source"])
    };
    format!(
        "{application}{media} {kind} stream {} {direction} {} ({})",
        known(&entry["index"]),
        known(device),
        states.join(", ")
    )
}

fn device_line(kind: &str, entry: &Value, default: &Value) -> String {
    let marker = if !default.is_null() && entry["name"] == *default {
        " (default)"
    } else {
        ""
    };
    let mut details = vec![known(&entry["state"])];
    if entries(&entry["volume_percent"]).next().is_some() {
        details.push(format!("{}%", joined(&entry["volume_percent"], "/")));
    }
    match entry["mute"].as_bool() {
        Some(true) => details.push("muted".to_owned()),
        None => details.push("mute unknown".to_owned()),
        _ => (),
    }
    for (field, label) in [("jack_client", "JACK client"), ("monitor_of", "monitor of")] {
        if let Some(value) = entry[field].as_str() {
            details.push(format!("{label} {value}"));
        }
    }
    format!(
        "{kind} {}{marker}: {}; {}",
        known(&entry["name"]),
        known(&entry["description"]),
        details.join(", ")
    )
}

fn card_lines(cards: &[Card], mappings: &BTreeMap<String, String>) -> Vec<String> {
    let mut lines = cards
        .iter()
        .map(|card| {
            let mut line = format!(
                "{}: {}",
                card.id,
                card.long_name.as_deref().unwrap_or(&card.name)
            );
            if let Some(product) = &card.usb_product {
                line.push_str(&format!(
                    " (USB {} {product})",
                    card.usb_vendor.as_deref().unwrap_or("unknown vendor")
                ));
            }
            let devices = mappings
                .iter()
                .filter(|(_, linked)| *linked == &card.id)
                .map(|(device, _)| device.as_str())
                .collect::<Vec<_>>();
            if !devices.is_empty() {
                line.push_str(&format!(" is Dante device {}", devices.join(", ")));
            } else if cards::is_dante(card) {
                line.push_str(
                    "; a Dante interface not linked to its Dante device (link_audio_card)",
                );
            }
            line
        })
        .collect::<Vec<_>>();
    for (device, card) in mappings {
        if !cards.iter().any(|entry| &entry.id == card) {
            lines.push(format!(
                "{card}: linked to {device} but not present on this computer"
            ));
        }
    }
    lines
}

fn roles(
    snapshot: &Value,
    cards: &[Card],
    bridges: &BTreeMap<String, Bridge>,
    mappings: &BTreeMap<String, String>,
) -> Value {
    let mut clients = BTreeMap::<String, Vec<(&str, &Value)>>::new();
    for (name, port) in snapshot["jack"]["ports"].as_object().into_iter().flatten() {
        clients
            .entry(known(&port["client"]))
            .or_default()
            .push((name, port));
    }
    let mut roles = serde_json::Map::new();
    for (client, ports) in clients {
        let mut parts = Vec::new();
        let used_cards = ports
            .iter()
            .filter_map(|(name, port)| {
                hardware(name, port, cards, bridges).map(|channel| channel.card)
            })
            .collect::<BTreeSet<_>>();
        if !used_cards.is_empty() {
            let descriptions = used_cards
                .iter()
                .map(|card| {
                    let devices = mappings
                        .iter()
                        .filter(|(_, linked)| *linked == card)
                        .map(|(device, _)| device.as_str())
                        .collect::<Vec<_>>();
                    if devices.is_empty() {
                        card.clone()
                    } else {
                        format!("{card} ({})", devices.join(", "))
                    }
                })
                .collect::<Vec<_>>();
            parts.push(format!("sound card {}", descriptions.join(", ")));
        }
        for (field, kind) in [("sinks", "output"), ("sources", "input")] {
            for entry in
                entries(&snapshot["pulse"][field]).filter(|entry| entry["jack_client"] == client)
            {
                parts.push(format!("PulseAudio {kind} {}", known(&entry["name"])));
            }
        }
        parts.push(format!(
            "{} in, {} out",
            ports
                .iter()
                .filter(|(_, port)| port["direction"] == "input")
                .count(),
            ports
                .iter()
                .filter(|(_, port)| port["direction"] == "output")
                .count()
        ));
        let mut feeds = ports
            .iter()
            .filter(|(_, port)| port["direction"] == "output")
            .flat_map(|(_, port)| entries(&port["connections"]).filter_map(Value::as_str))
            .map(|name| name.split(':').next().unwrap_or(name))
            .collect::<BTreeSet<_>>();
        if !feeds.is_empty() {
            feeds.remove(client.as_str());
            parts.push(format!(
                "feeds {}",
                if feeds.is_empty() {
                    "itself".to_owned()
                } else {
                    feeds.into_iter().collect::<Vec<_>>().join(", ")
                }
            ));
        }
        roles.insert(client, json!(parts.join("; ")));
    }
    Value::Object(roles)
}

fn changes(snapshot: &Value, limit: Option<usize>) -> Vec<String> {
    let mut records = ["jack", "pulse"]
        .into_iter()
        .flat_map(|component| {
            entries(&snapshot[component]["changes"]).map(move |change| (component, change))
        })
        .collect::<Vec<_>>();
    records.sort_by(|(_, left), (_, right)| right["time"].as_str().cmp(&left["time"].as_str()));
    records
        .into_iter()
        .take(limit.unwrap_or(usize::MAX))
        .map(|(component, change)| {
            let time = local_time(&change["time"]).unwrap_or_else(|| "at an unknown time".into());
            let kind = change["kind"].as_str().unwrap_or("unknown change");
            let label = if component == "jack" {
                "JACK"
            } else {
                "PulseAudio"
            };
            if matches!(kind, "connected" | "disconnected") {
                let pairs = entries(&change["pairs"])
                    .map(|pair| format!("{} → {}", known(&pair[0]), known(&pair[1])))
                    .collect::<Vec<_>>();
                return format!("{time} {label} {kind} {}", pairs.join(", "));
            }
            match kind {
                "server_connected" => format!("{time} netaudio connected to {label}"),
                "server_stopped" => format!(
                    "{time} netaudio lost its connection to {label}: {}",
                    known(&change["reason"])
                ),
                _ => {
                    let details = change
                        .as_object()
                        .into_iter()
                        .flatten()
                        .filter(|(key, value)| {
                            !matches!(key.as_str(), "time" | "kind") && !value.is_null()
                        })
                        .map(|(key, value)| format!("{key} {}", text(value)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "{time} {label} {}{}",
                        kind.replace('_', " "),
                        if details.is_empty() {
                            String::new()
                        } else {
                            format!(": {details}")
                        }
                    )
                }
            }
        })
        .collect()
}

pub fn view(snapshot: &Value, arguments: &Value) -> Result<Value, CallError> {
    let section = arguments["section"].as_str().unwrap_or("summary");
    let client = arguments["client"].as_str();
    let jack = &snapshot["jack"];
    let pulse = &snapshot["pulse"];
    let mut ports = jack["ports"]
        .as_object()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    ports.sort_by(|(left, _), (right, _)| natural(left, right));
    if let Some(client) =
        client.filter(|client| !ports.iter().any(|(_, port)| port["client"] == *client))
    {
        let clients = ports
            .iter()
            .map(|(_, port)| known(&port["client"]))
            .collect::<BTreeSet<_>>();
        return Err(CallError::invalid(format!(
            "no JACK client {client:?}; clients: {}",
            clients.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    let cards: Vec<Card> = serde_json::from_value(snapshot["cards"].clone())
        .map_err(|error| CallError::invalid(format!("Invalid sound card snapshot: {error}")))?;
    let bridges: BTreeMap<String, Bridge> = serde_json::from_value(snapshot["bridges"].clone())
        .map_err(|error| CallError::invalid(format!("Invalid sound card bridges: {error}")))?;
    let mappings: BTreeMap<String, String> = serde_json::from_value(snapshot["card_links"].clone())
        .map_err(|error| CallError::invalid(format!("Invalid sound card mappings: {error}")))?;
    let selected = ports
        .iter()
        .filter(|(_, port)| client.is_none_or(|client| port["client"] == client))
        .collect::<Vec<_>>();
    let streams = || {
        ["playback", "recording"]
            .into_iter()
            .flat_map(|kind| {
                entries(
                    &pulse[if kind == "playback" {
                        "sink_inputs"
                    } else {
                        "source_outputs"
                    }],
                )
                .map(move |entry| stream_line(kind, entry))
            })
            .collect::<Vec<_>>()
    };
    let mut view = json!({"host":snapshot["host"]});
    match section {
        "summary" => {
            view["jack"] = json!(jack_line(jack));
            view["pulseaudio"] = json!(pulse_line(pulse));
            view["cards"] = json!(card_lines(&cards, &mappings));
            let mut clients = roles(snapshot, &cards, &bridges, &mappings);
            if let Some(client) = client {
                clients
                    .as_object_mut()
                    .expect("roles object")
                    .retain(|name, _| name == client);
            }
            view["jack_clients"] = clients;
            view["streams"] = json!(streams());
            view["recent_changes"] = json!(changes(snapshot, Some(8)));
            view["more"] = json!(
                "section connections, ports, pulse, cards or changes; trace_signal follows audio through"
            );
        }
        "connections" => {
            view["connections"] = json!(
                selected
                    .iter()
                    .filter(|(_, port)| port["direction"] == "output"
                        && entries(&port["connections"]).next().is_some())
                    .map(|(name, port)| format!(
                        "{} → {}",
                        label(name, port),
                        entries(&port["connections"])
                            .filter_map(Value::as_str)
                            .map(|destination| label(destination, &jack["ports"][destination]))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                    .collect::<Vec<_>>()
            );
            if client.is_some() {
                view["heard_by_client"] = json!(
                    selected
                        .iter()
                        .filter(|(_, port)| port["direction"] == "input"
                            && entries(&port["connections"]).next().is_some())
                        .map(|(name, port)| format!(
                            "{} ← {}",
                            label(name, port),
                            joined(&port["connections"], ", ")
                        ))
                        .collect::<Vec<_>>()
                );
            }
            view["unconnected_ports"] = json!(
                selected
                    .iter()
                    .filter(|(_, port)| entries(&port["connections"]).next().is_none())
                    .count()
            );
        }
        "ports" => {
            view["ports"] = json!(
                selected
                    .iter()
                    .map(|(name, port)| {
                        let mut line = format!(
                            "{}: {} {}",
                            label(name, port),
                            known(&port["type"]),
                            known(&port["direction"])
                        );
                        if let Some(channel) = hardware(name, port, &cards, &bridges) {
                            line.push_str(&format!(
                                ", {} {} {}",
                                channel.card,
                                if channel.direction == cards::Direction::Capture {
                                    "capture"
                                } else {
                                    "playback"
                                },
                                channel.channel
                            ));
                        }
                        if entries(&port["connections"]).next().is_some() {
                            line.push_str(&format!(
                                " {} {}",
                                if port["direction"] == "output" {
                                    "→"
                                } else {
                                    "←"
                                },
                                joined(&port["connections"], ", ")
                            ));
                        }
                        line
                    })
                    .collect::<Vec<_>>()
            );
        }
        "pulse" => {
            view["pulseaudio"] = json!(pulse_line(pulse));
            view["outputs"] = json!(
                entries(&pulse["sinks"])
                    .map(|entry| device_line("output", entry, &pulse["server"]["default_sink"]))
                    .collect::<Vec<_>>()
            );
            view["inputs"] = json!(
                entries(&pulse["sources"])
                    .map(|entry| device_line("input", entry, &pulse["server"]["default_source"]))
                    .collect::<Vec<_>>()
            );
            view["streams"] = json!(streams());
        }
        "cards" => {
            view["cards"] = json!(card_lines(&cards, &mappings));
            view["links"] = json!(mappings);
            view["bridged_by"] = json!(
                bridges
                    .iter()
                    .map(|(client, bridge)| (
                        client,
                        format!(
                            "{} {} on {}",
                            bridge.program,
                            if bridge.direction == cards::Direction::Capture {
                                "capture"
                            } else {
                                "playback"
                            },
                            bridge.card
                        )
                    ))
                    .collect::<BTreeMap<_, _>>()
            );
        }
        "changes" => view["changes"] = json!(changes(snapshot, None)),
        _ => return Err(CallError::invalid("Unknown host audio section")),
    }
    for field in ["cards_error", "card_links_error", "source_observer_error"] {
        if !snapshot[field].is_null() {
            view[field] = snapshot[field].clone();
        }
    }
    if section != "summary" && section != "pulse" {
        for (name, component) in [("jack", jack), ("pulseaudio", pulse)] {
            if component["available"] != true {
                view[name] = json!(format!("not available: {}", known(&component["reason"])));
            }
        }
    }
    Ok(view)
}

pub fn conditions(snapshot: &Value, records: &super::Records) -> Vec<String> {
    let host = snapshot["host"].as_str().unwrap_or("unknown host");
    let mut conditions = Vec::new();
    for (key, label) in [("jack", "JACK"), ("pulse", "PulseAudio")] {
        let component = &snapshot[key];
        if component["seen_running"] == true && component["available"] != true {
            conditions.push(format!(
                "{label} on {host} stopped: {}",
                known(&component["reason"])
            ));
        }
    }
    let jack = &snapshot["jack"];
    if let Some(xruns) = jack["xruns"].as_u64().filter(|count| *count > 0) {
        conditions.push(format!(
            "JACK on {host} reports {xruns} xruns since {}; last at {}",
            local_time(&jack["xruns_since"]).unwrap_or_else(|| "an unknown time".to_owned()),
            local_time(&jack["last_xrun"]).unwrap_or_else(|| "an unknown time".to_owned())
        ));
    }
    let cards: Vec<Card> = match serde_json::from_value(snapshot["cards"].clone()) {
        Ok(cards) => cards,
        Err(error) => {
            conditions.push(format!(
                "Sound card inventory on {host} is invalid: {error}"
            ));
            return conditions;
        }
    };
    let links = snapshot["card_links"].as_object();
    for card in &cards {
        let devices = links
            .into_iter()
            .flatten()
            .filter(|(_, value)| value.as_str() == Some(&card.id))
            .map(|(device, _)| device)
            .collect::<Vec<_>>();
        if devices.is_empty() && cards::is_dante(card) {
            conditions.push(format!("Sound card {} on {host} is a Dante interface without a device mapping; link_audio_card connects its trace", card.id));
        }
        for device in devices {
            let candidates = records
                .values()
                .filter(|record| record["name"] == *device)
                .collect::<Vec<_>>();
            let [record] = candidates.as_slice() else {
                conditions.push(format!(
                    "Sound card {} on {host} maps to absent or ambiguous Dante device {device}",
                    card.id
                ));
                continue;
            };
            if record["online"] != true {
                conditions.push(format!("Sound card {} on {host} maps to Dante device {device}, which is not observed online", card.id));
                continue;
            }
            if jack["available"] != true {
                continue;
            }
            let uses = jack["ports"]
                .as_object()
                .into_iter()
                .flatten()
                .any(|(_, port)| {
                    let aliases = entries(&port["aliases"])
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>();
                    cards::hardware_aliases(&aliases, &cards)
                        .is_some_and(|channel| channel.card == card.id)
                });
            if let (Some(host_rate), Some(device_rate)) = (
                jack["sample_rate"].as_u64(),
                record["sample_rate_hz"].as_u64(),
            ) && uses
                && host_rate != device_rate
            {
                conditions.push(format!("JACK on {host} runs at {host_rate} Hz but {device} (card {}) reports {device_rate} Hz", card.id));
            }
        }
    }
    conditions
}
