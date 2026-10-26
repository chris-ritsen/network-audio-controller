use super::{Records, compact, events_view, local_time, text};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub fn domains(records: &Records) -> Vec<Value> {
    let mut domains = BTreeMap::<(String, String), (String, BTreeSet<String>)>::new();
    for (key, record) in records {
        if record["online"] != true || record["management_state"] != "managed" {
            continue;
        }
        let Some(identity) = record["ddm_domain_id"]
            .as_str()
            .or(record["ddm_domain_name"].as_str())
        else {
            continue;
        };
        let server = record["ddm_server_profile"].as_str().unwrap_or_default();
        let name = record["ddm_domain_name"].as_str().unwrap_or(identity);
        domains
            .entry((server.to_owned(), identity.to_owned()))
            .or_insert_with(|| (name.to_owned(), BTreeSet::new()))
            .1
            .insert(record["name"].as_str().unwrap_or(key).to_owned());
    }
    domains
        .into_iter()
        .map(|((server, identity), (name, devices))| {
            compact(json!({"name":name,"id":identity,"server":server,"devices":devices}))
        })
        .collect()
}

pub fn recent_changes(page: &Value) -> String {
    let view = events_view(page, &json!({"limit":5}));
    let lines = view["events"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|event| {
            let kind = event["kind"].as_str().unwrap_or("event").replace('_', " ");
            let change = &event["change"];
            let detail = if change.is_object() {
                format!(
                    ", {}: {} to {}",
                    text(&change["what"]),
                    text(&change["from"]),
                    text(&change["to"])
                )
            } else {
                event["current_value"]["title"]
                    .as_str()
                    .or(event["current_value"]["summary"].as_str())
                    .map(|value| format!(", {value}"))
                    .unwrap_or_default()
            };
            format!(
                "{}: {} {kind}{detail}",
                event["time"].as_str().unwrap_or("time not reported"),
                event["device_name"].as_str().unwrap_or("host")
            )
        })
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return "No visible changes in the latest journal page; get_event_journal includes the retained records and observation gaps".to_owned();
    }
    format!(
        "Latest recorded changes: {}. get_event_journal has the retained history and observation gaps",
        lines.join("; ")
    )
}

fn address(value: &Value) -> Option<String> {
    let value = value.as_str()?.replace([':', '-'], "").to_lowercase();
    (value.len() == 12
        && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        && value.bytes().any(|byte| byte != b'0'))
    .then_some(value)
}

pub fn renamed(records: &Records, page: &Value) -> Vec<Value> {
    let mut names = BTreeMap::<String, BTreeSet<String>>::new();
    for record in records.values().filter(|record| record["online"] == true) {
        if let (Some(address), Some(name)) =
            (address(&record["mac_address"]), record["name"].as_str())
        {
            names.entry(address).or_default().insert(name.to_owned());
        }
    }
    let mut aliases = BTreeMap::new();
    for event in page["events"].as_array().into_iter().flatten() {
        if event["kind"] != "device_disappeared" {
            continue;
        }
        let previous = &event["raw"]["previous_observation"];
        let evidence = if previous.is_object() {
            previous
        } else {
            &event["raw"]["current_observation"]
        };
        let Some(name) = event["device_name"].as_str() else {
            continue;
        };
        let Some(current) = address(&evidence["mac_address"])
            .and_then(|address| names.get(&address))
            .filter(|names| names.len() == 1)
            .and_then(|names| names.first())
        else {
            continue;
        };
        if name == current || aliases.contains_key(name) {
            continue;
        }
        let routes = records
            .values()
            .flat_map(|record| super::subscriptions(record))
            .filter(|route| route["tx_device"] == name)
            .count();
        aliases.insert(name.to_owned(), compact(json!({"name":name,"renamed_to":current,"last_observed_as_old_name":local_time(&event["timestamp"]),"routes_using_old_name":routes,"note":(routes>0).then_some("Routes still name this device's previous identity; inspect and update them before reconnecting")})));
    }
    aliases.into_values().collect()
}

pub fn streams(health: &Value) -> Value {
    let devices = health["devices"]
        .as_array()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let measured = devices
        .iter()
        .filter_map(|device| device["late_packets_since_watching"].as_u64())
        .collect::<Vec<_>>();
    let total = measured
        .iter()
        .try_fold(0u64, |total, count| total.checked_add(*count));
    let top = devices
        .iter()
        .filter_map(|device| {
            Some((
                device["worst_latency_ms"].as_f64()?,
                device["latency_budget_ms"]
                    .as_f64()
                    .filter(|budget| *budget > 0.0)?,
                device["device"].as_str()?,
            ))
        })
        .max_by(|left, right| (left.0 / left.1).total_cmp(&(right.0 / right.1)));
    compact(
        json!({"late_packets_since_server_start":(!measured.is_empty()).then_some(total).flatten(),"devices_with_late_packet_observations":measured.len(),"devices_without_late_packet_observations":devices.len()-measured.len(),"worst_latency":top.map(|(latency,budget,name)|format!("{latency} of {budget} ms ({name})")),"problems":health["problems"],"detail":"get_device_diagnostics; missing or stale telemetry does not establish stream health"}),
    )
}
