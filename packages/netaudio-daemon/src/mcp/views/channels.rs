use super::{Records, first, problem, subscriptions, text, waiting};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

const COLLAPSE_MINIMUM: usize = 4;
const FIND_LIMIT: usize = 60;
const FACTORY_PREFIXES: &[&str] = &["input", "output", "in", "out", "ch", "channel", "rx", "tx"];

pub fn channel_label(channel: &Value) -> Value {
    if channel.is_object() {
        first(&[&channel["friendly_name"], &channel["name"]])
    } else {
        channel.clone()
    }
}

pub fn channel_names(channels: &Value) -> Vec<(&str, Value)> {
    let Some(channels) = channels.as_object() else {
        return Vec::new();
    };
    let mut entries: Vec<_> = channels
        .iter()
        .map(|(number, channel)| (number.as_str(), channel_label(channel)))
        .collect();
    entries.sort_by_key(|(number, _)| number.parse::<u64>().unwrap_or_default());
    entries
}

pub(super) fn factory_named(number: &str, channel: &Value) -> bool {
    if !channel.is_object() {
        return false;
    }

    let label = channel_label(channel);
    let Some(name) = label
        .as_str()
        .map(str::trim)
        .filter(|name| !name.is_empty())
    else {
        return true;
    };
    let factory = first(&[
        &channel["factory_name"],
        if super::truthy(&channel["friendly_name"]) {
            &channel["name"]
        } else {
            &Value::Null
        },
    ]);

    if let Some(factory) = factory.as_str().filter(|name| !name.is_empty()) {
        return label == factory;
    }

    let lowered = name.to_lowercase();
    let suffix = FACTORY_PREFIXES
        .iter()
        .filter_map(|prefix| lowered.strip_prefix(prefix).map(str::trim))
        .find(|suffix| suffix.chars().all(|character| character.is_ascii_digit()))
        .unwrap_or(&lowered);
    number.parse::<u64>().ok().is_some_and(|number| {
        suffix.chars().all(|character| character.is_ascii_digit())
            && suffix.parse::<u64>().ok() == Some(number)
    })
}

pub fn number_ranges(mut numbers: Vec<u64>) -> String {
    numbers.sort_unstable();
    let mut ranges: Vec<(u64, u64)> = Vec::new();

    for number in numbers {
        if let Some((_, end)) = ranges.last_mut()
            && end.checked_add(1) == Some(number)
        {
            *end = number;
            continue;
        }

        ranges.push((number, number));
    }

    ranges
        .into_iter()
        .map(|(start, end)| {
            if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn compact_channel_names(channels: &Value) -> Value {
    let mut view = Map::new();
    let mut factory = Vec::new();

    for (number, name) in channel_names(channels) {
        if let Ok(number_value) = number.parse::<u64>()
            && factory_named(number, &channels[number])
        {
            factory.push(number_value);
        } else {
            view.insert(number.to_owned(), name);
        }
    }

    if !factory.is_empty() {
        view.insert("factory names".to_owned(), json!(number_ranges(factory)));
    }

    Value::Object(view)
}

struct Match {
    device: String,
    direction: &'static str,
    number: u64,
    collapsible: bool,
    presentation: Value,
}

pub fn find_channels(records: &Records, arguments: &Value) -> Value {
    let query = caseless::default_case_fold_str(arguments["query"].as_str().unwrap_or_default());
    let words: Vec<_> = query
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect();
    let wanted = arguments["channel_type"].as_str().unwrap_or("any");
    let mut feeds = BTreeMap::<(String, String), Vec<String>>::new();

    for (key, device) in records {
        let receiver = device["name"].as_str().unwrap_or(key);

        for subscription in subscriptions(device) {
            feeds
                .entry((
                    text(&subscription["tx_device"]),
                    text(&subscription["tx_channel"]),
                ))
                .or_default()
                .push(format!("{receiver}:{}", text(&subscription["rx_channel"])));
        }
    }

    let mut matches = Vec::new();

    for (key, device) in records {
        let name = device["name"].as_str().unwrap_or(key);
        let device_text = caseless::default_case_fold_str(&format!(
            "{name} {}",
            device["product_name"].as_str().unwrap_or_default()
        ));

        for (direction, field) in [("rx", "receivers"), ("tx", "transmitters")] {
            if wanted != "any" && wanted != direction {
                continue;
            }

            let channels = &device["channels"][field];

            for (number, label) in channel_names(channels) {
                let searchable = format!(
                    "{device_text} {}",
                    caseless::default_case_fold_str(&text(&label))
                );
                let Ok(number_value) = number.parse::<u64>() else {
                    continue;
                };

                if words.is_empty() || !words.iter().all(|word| searchable.contains(word)) {
                    continue;
                }

                let mut entry = json!({"device": name, "channel_type": direction, "number": number_value, "name": label});
                let unused = if direction == "rx" {
                    let route = subscriptions(device).find(|route| {
                        route["rx_channel_number"].as_u64() == Some(number_value)
                            || route["rx_channel"] == label
                    });

                    if let Some(route) = route {
                        entry["fed_by"] = json!(format!(
                            "{}:{}",
                            text(&route["tx_device"]),
                            text(&route["tx_channel"])
                        ));

                        if problem(route) {
                            if let Some(wait) = waiting(&text(&route["tx_device"]), records) {
                                entry["waiting"] = json!(wait);
                            } else {
                                entry["problem"] =
                                    first(&[&route["status"]["detail"], &route["status"]["label"]]);
                            }
                        }

                        false
                    } else {
                        entry["fed_by"] = json!("nothing");
                        true
                    }
                } else {
                    let mut destinations = feeds
                        .get(&(name.to_owned(), text(&label)))
                        .cloned()
                        .unwrap_or_default();
                    destinations.sort();
                    let unused = destinations.is_empty();
                    entry["feeds"] = json!(destinations);
                    unused
                };
                matches.push(Match {
                    device: name.to_owned(),
                    direction,
                    number: number_value,
                    collapsible: unused && factory_named(number, &channels[number]),
                    presentation: entry,
                });
            }
        }
    }

    let count = matches.len();
    let mut collapsed = BTreeMap::<(String, &str), Vec<u64>>::new();

    for entry in matches.iter().filter(|entry| entry.collapsible) {
        collapsed
            .entry((entry.device.clone(), entry.direction))
            .or_default()
            .push(entry.number);
    }

    let mut shown = Vec::new();
    let mut emitted = BTreeSet::new();

    for entry in matches {
        let group = (entry.device, entry.direction);

        if entry.collapsible
            && let Some(numbers) = collapsed
                .get(&group)
                .filter(|numbers| numbers.len() >= COLLAPSE_MINIMUM)
        {
            if emitted.insert(group.clone()) {
                shown.push(json!({
                    "device": group.0,
                    "channel_type": group.1,
                    "numbers": number_ranges(numbers.clone()),
                    "name": "factory names, not routed",
                }));
            }
        } else {
            shown.push(entry.presentation);
        }
    }

    let omitted = shown.len().saturating_sub(FIND_LIMIT);
    shown.truncate(FIND_LIMIT);
    json!({"count": count, "channels": shown, "omitted": (omitted != 0).then_some(omitted)})
}
