use super::{Records, channel_names, compact, last_seen, problem, subscriptions, text, waiting};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

pub fn routing_view(records: &Records, arguments: &Value) -> Value {
    let mut routes = BTreeMap::<String, Vec<String>>::new();
    let mut offline = Map::new();

    for (key, device) in records {
        let receiver = device["name"].as_str().unwrap_or(key);
        let selector = arguments["device"].as_str();

        if selector.is_some_and(|selector| selector != receiver && selector != key)
            || (selector.is_none() && device["online"] == false)
        {
            continue;
        }

        let mut names = BTreeMap::<String, Vec<&str>>::new();

        for (number, name) in channel_names(&device["channels"]["receivers"]) {
            names.entry(text(&name)).or_default().push(number);
        }

        for subscription in subscriptions(device) {
            let channel = text(&subscription["rx_channel"]);
            let number = names
                .get(&channel)
                .filter(|numbers| numbers.len() == 1)
                .map(|numbers| numbers[0]);

            if !arguments["rx_channel"].is_null()
                && text(&arguments["rx_channel"]) != channel
                && number != Some(text(&arguments["rx_channel"]).as_str())
            {
                continue;
            }

            let wait = problem(subscription)
                .then(|| waiting(&text(&subscription["tx_device"]), records))
                .flatten();
            let wanted = arguments["status"].as_str().unwrap_or("all");

            if (wanted == "problem" && (!problem(subscription) || wait.is_some()))
                || (wanted == "waiting" && wait.is_none())
                || (wanted == "ok" && subscription["status"]["severity"] != "ok")
            {
                continue;
            }

            let receiver_label =
                number.map_or_else(|| channel.clone(), |number| format!("{number} {channel}"));
            let mut line = format!(
                "{receiver_label} ← {} on {}",
                text(&subscription["tx_channel"]),
                text(&subscription["tx_device"])
            );
            let label = subscription["status"]["label"].as_str().unwrap_or_default();

            if let Some(wait) = wait {
                line.push_str(&format!(" ({wait})"));
            } else if problem(subscription) {
                line.push_str(&format!(": {label}"));

                if let Some(detail) = subscription["status"]["detail"]
                    .as_str()
                    .filter(|detail| !detail.is_empty())
                {
                    line.push_str(&format!(", {detail}"));
                }
            } else if let Some(kind) = label
                .strip_prefix("Subscribed (")
                .and_then(|label| label.strip_suffix(')'))
            {
                if kind != "unicast" {
                    line.push_str(&format!(" ({kind})"));
                }
            } else if !label.is_empty() && !label.starts_with("Subscribed") {
                line.push_str(&format!(" ({label})"));
            }

            routes.entry(receiver.to_owned()).or_default().push(line);
        }

        if routes.contains_key(receiver)
            && let Some(seen) = last_seen(device)
        {
            offline.insert(
                receiver.to_owned(),
                json!(format!("{seen}; these are its routes as they were then")),
            );
        }
    }

    compact(json!({
        "count": routes.values().map(Vec::len).sum::<usize>(),
        "receivers_off_the_network": (!offline.is_empty()).then_some(offline),
        "routes": routes,
    }))
}
