mod channels;
mod clock;
mod diagnostics;
mod flows;
pub mod host_audio;
mod issues;
mod journal;
pub mod managed;
mod metering;
pub mod overview;
mod routing;
pub mod wireless;

pub use crate::devices::cached::include_remembered;
pub use channels::{channel_label, channel_names, find_channels};
pub use clock::{clock_status, clock_view};
pub use diagnostics::{diagnostic_evidence, diagnostics_summary, diagnostics_view};
pub use flows::{flow_plan_view, flow_result_view, transmit_flows_view};
pub use issues::issues_view;
pub use journal::events_view;
pub use metering::{channel_signals, signal_levels};
pub mod signal_history;
pub use routing::routing_view;

use chrono::{DateTime, Local, NaiveDateTime, Utc};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

pub type Records = BTreeMap<String, Arc<Value>>;

const FULL_FIELD_LIMIT: usize = 16_000;
const SUMMARY_FIELDS: &[&str] = &[
    "encoding",
    "is_locked",
    "kind",
    "latency_ms",
    "manufacturer",
    "model",
    "name",
    "online",
    "rx_count",
    "sample_rate_hz",
    "tx_count",
];

pub fn compact(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .filter(|(key, value)| key != "icon" && !value.is_null() && value != "")
                .map(|(key, value)| (key, compact(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(compact).collect()),
        value => value,
    }
}

pub fn text(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::String(value) => value.clone(),
        value => value.to_string(),
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn first(values: &[&Value]) -> Value {
    values
        .iter()
        .find(|value| truthy(value))
        .copied()
        .cloned()
        .unwrap_or(Value::Null)
}

fn selected(record: &Value, fields: &[&str]) -> Map<String, Value> {
    fields
        .iter()
        .map(|field| ((*field).to_owned(), record[*field].clone()))
        .collect()
}

fn values(value: &Value) -> Box<dyn Iterator<Item = &Value> + '_> {
    match value {
        Value::Object(fields) => Box::new(fields.values()),
        Value::Array(items) => Box::new(items.iter()),
        _ => Box::new(std::iter::empty()),
    }
}

pub(super) fn subscriptions(device: &Value) -> impl Iterator<Item = &Value> {
    values(&device["subscriptions"]).filter(|subscription| {
        truthy(&subscription["tx_device"]) && subscription["status"]["state"] != "none"
    })
}

pub fn local_time(value: &Value) -> Option<String> {
    let timestamp = match value {
        Value::Number(number) => {
            let seconds = number.as_f64()?;
            let whole = seconds.floor();
            DateTime::from_timestamp(
                whole as i64,
                ((seconds - whole) * 1_000_000_000.0).round() as u32,
            )?
        }
        Value::String(value) => DateTime::parse_from_rfc3339(value)
            .map(|value| value.with_timezone(&Utc))
            .or_else(|_| {
                NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|value| value.and_utc())
            })
            .ok()?,
        _ => return None,
    };
    let local = timestamp.with_timezone(&Local);
    let pattern = if local.date_naive() == Local::now().date_naive() {
        "%H:%M"
    } else {
        "%Y-%m-%d %H:%M"
    };
    Some(local.format(pattern).to_string())
}

pub(crate) fn last_seen(device: &Value) -> Option<String> {
    (device["online"] == false).then(|| {
        local_time(&device["last_seen"]).map_or_else(
            || "last seen at an unknown time".to_owned(),
            |seen| format!("last seen {seen}"),
        )
    })
}

pub fn resolve<'a>(records: &'a Records, selector: &str) -> anyhow::Result<Option<&'a Value>> {
    Ok(crate::state::resolve_record(records, selector, None)?.map(AsRef::as_ref))
}

fn waiting(source: &str, records: &Records) -> Option<String> {
    match resolve(records, source) {
        Ok(None) => Some(format!(
            "waiting for {source}, which has not been seen since the server started"
        )),
        Ok(Some(record)) => last_seen(record).map(|seen| format!("waiting for {source}, {seen}")),
        Err(_) => None,
    }
}

fn problem(subscription: &Value) -> bool {
    matches!(
        subscription["status"]["severity"].as_str(),
        Some("warning" | "error")
    )
}

fn subscription_problems(device: &Value, records: &Records) -> (Vec<String>, Vec<String>) {
    let mut problems = Vec::new();
    let mut pending = BTreeMap::<String, Vec<Value>>::new();
    let mut pending_order = Vec::new();

    for subscription in subscriptions(device).filter(|subscription| problem(subscription)) {
        let source = text(&subscription["tx_device"]);

        if let Some(wait) = waiting(&source, records) {
            let channel = if subscription["rx_channel_number"].is_number() {
                &subscription["rx_channel_number"]
            } else {
                &subscription["rx_channel"]
            };
            if !pending.contains_key(&wait) {
                pending_order.push(wait.clone());
            }

            pending.entry(wait).or_default().push(channel.clone());
            continue;
        }

        let receiver = if subscription["rx_channel_number"].is_null() {
            text(&subscription["rx_channel"])
        } else {
            format!(
                "rx {} {}",
                text(&subscription["rx_channel_number"]),
                text(&subscription["rx_channel"])
            )
        };
        problems.push(format!(
            "{receiver} ← {} on {source}: {}",
            text(&subscription["tx_channel"]),
            text(&first(&[
                &subscription["status"]["detail"],
                &subscription["status"]["label"],
            ])),
        ));
    }

    let pending = pending_order
        .into_iter()
        .map(|wait| {
            let channels = &pending[&wait];
            let numbers = channels.iter().filter_map(Value::as_u64).collect();
            let mut names = channels
                .iter()
                .filter(|channel| channel.as_u64().is_none())
                .map(text)
                .collect::<Vec<_>>();
            let ranges = channels::number_ranges(numbers);

            if !ranges.is_empty() {
                names.insert(0, ranges);
            }

            format!("rx {} {wait}", names.join(", "))
        })
        .collect();
    (problems, pending)
}

pub fn device_summary(device: &Value, management_known: bool, records: &Records) -> Value {
    let mut summary = Value::Object(selected(device, SUMMARY_FIELDS));
    let model = first(&[
        &device["model"],
        &device["dante_model"],
        &device["model_id"],
    ]);
    summary["model"] = if model.as_str().is_some_and(|model| model.starts_with('_')) {
        Value::Null
    } else {
        model
    };
    summary["product"] = device["product_name"].clone();
    summary["management"] = json!(match device["management_state"].as_str() {
        Some("managed") => "managed",
        Some("unenrolled") => "unmanaged",
        _ if management_known => "unmanaged",
        _ => "unknown",
    });

    if device["server_name"] != device["inventory_id"] {
        summary["server_name"] = device["server_name"].clone();
    }

    if device["online"] == false {
        summary["last_seen"] = json!(local_time(&device["last_seen"]));
    }

    summary["subscription_count"] = json!(subscriptions(device).count());
    let (problems, pending) = subscription_problems(device, records);
    summary["subscription_problems"] = json!(problems);
    summary["routes_waiting"] = if pending.is_empty() {
        Value::Null
    } else {
        json!(pending)
    };
    compact(summary)
}

pub fn device_list(records: &Records, management_known: bool) -> Value {
    let mut entries = Vec::new();
    let mut names = BTreeMap::<String, usize>::new();

    for (key, device) in records {
        let summary = device_summary(device, management_known, records);
        let name = summary["name"].as_str().unwrap_or(key);
        *names.entry(name.to_owned()).or_default() += 1;
        let product = first(&[&summary["product"], &summary["model"]]);
        let entry = if device["online"] == false {
            json!({"product": product, "last_seen": first(&[&summary["last_seen"], &json!("unknown")])})
        } else {
            let counts =
                (!summary["rx_count"].is_null() || !summary["tx_count"].is_null()).then(|| {
                    format!(
                        "{} rx, {} tx",
                        text(&summary["rx_count"]),
                        text(&summary["tx_count"])
                    )
                });
            json!({
                "product": product,
                "online": summary["online"],
                "management": summary["management"],
                "domain": if summary["management"] == "managed" { &device["ddm_domain_name"] } else { &Value::Null },
                "sample_rate_hz": summary["sample_rate_hz"],
                "latency_ms": summary["latency_ms"],
                "channels": counts,
                "routes": first(&[&summary["subscription_count"]]),
                "route_problems": first(&[&summary["subscription_problems"]]),
                "locked": (summary["is_locked"] == true).then_some(true),
            })
        };
        entries.push((name.to_owned(), key, compact(entry)));
    }

    Value::Object(
        entries
            .into_iter()
            .map(|(name, key, entry)| (if names[&name] == 1 { name } else { key.clone() }, entry))
            .collect(),
    )
}

pub fn record_fields(record: &Value, fields: &[String], known: &BTreeSet<String>) -> Value {
    let mut view = Map::new();
    let mut pending = Vec::new();
    let mut unknown = Map::new();

    for field in fields {
        if let Some(value) = record.get(field) {
            view.insert(field.clone(), value.clone());
        } else if known.contains(field) {
            pending.push(field);
        } else {
            let mut matches: Vec<_> = record
                .as_object()
                .into_iter()
                .flatten()
                .map(|(name, _)| (strsim::normalized_levenshtein(field, name), name))
                .filter(|(score, _)| *score >= 0.5)
                .collect();
            matches.sort_by(|(left, left_name), (right, right_name)| {
                right
                    .total_cmp(left)
                    .then_with(|| left_name.cmp(right_name))
            });
            unknown.insert(
                field.clone(),
                json!(
                    matches
                        .into_iter()
                        .take(3)
                        .map(|(_, name)| name)
                        .collect::<Vec<_>>()
                ),
            );
        }
    }

    if !pending.is_empty() {
        view.insert("not_known_yet".to_owned(), json!(pending));
    }

    if !unknown.is_empty() {
        view.insert("unknown_fields".to_owned(), Value::Object(unknown));
    }

    Value::Object(view)
}

pub fn device_view(
    device: &Value,
    sections: &[String],
    management_known: bool,
    records: &Records,
) -> Value {
    let has = |section: &str| sections.iter().any(|candidate| candidate == section);

    if has("full") {
        let mut record = compact(device.clone());
        let mut omitted = Map::new();

        if let Some(fields) = record.as_object_mut() {
            fields.retain(|key, value| {
                let length = value.to_string().len();

                if length > FULL_FIELD_LIMIT {
                    omitted.insert(key.clone(), json!(length));
                    false
                } else {
                    true
                }
            });
        }

        if !omitted.is_empty() {
            record["omitted_fields"] = json!({
                "bytes": omitted,
                "read_with": "get_device_record with these field names",
            });
        }

        return record;
    }

    let mut view = if has("summary") {
        device_summary(device, management_known, records)
    } else {
        json!({})
    };

    if has("summary") {
        view["clock"] = clock_view(device);

        if has("subscriptions")
            && let Some(fields) = view.as_object_mut()
        {
            fields.remove("subscription_problems");
        }
    }

    if has("channels") {
        view["channels"] = json!({
            "rx": channels::compact_channel_names(&device["channels"]["receivers"]),
            "tx": channels::compact_channel_names(&device["channels"]["transmitters"]),
        });
    }

    if has("subscriptions") {
        view["subscriptions"] = subscriptions(device).map(|subscription| {
            let wait = waiting(&text(&subscription["tx_device"]), records);
            let mut view = json!({
                "rx_channel": {"number": subscription["rx_channel_number"], "name": subscription["rx_channel"]},
                "status": wait.as_ref().map_or_else(|| subscription["status"]["label"].clone(), |wait| json!(wait)),
                "tx_channel": subscription["tx_channel"],
                "tx_device": subscription["tx_device"],
            });

            if problem(subscription) && wait.is_none() {
                view["problem"] = subscription["status"]["detail"].clone();
            }

            view
        }).collect();
    }

    if has("network") {
        let mut network = Value::Object(selected(
            device,
            &[
                "interfaces",
                "link_speed_mbps",
                "mac_address",
                "network_redundancy",
            ],
        ));
        let redundancy = &device["network_redundancy"];

        if redundancy.is_object() {
            let choices: Vec<_> = values(&redundancy["available_modes"])
                .filter_map(|choice| choice.get("label"))
                .collect();
            network["network_redundancy"] = json!({
                "mode": redundancy["current_mode"],
                "configured_mode": redundancy["configured_mode"],
                "supported": redundancy["advertised_support"],
                "read_only": redundancy["read_only"],
                "choices": (!choices.is_empty()).then_some(choices),
                "current": redundancy["state_fresh"],
            });
        }

        view["network"] = network;
    }

    if has("availability") {
        for field in [
            "operation_availability",
            "performance_operation_availability",
        ] {
            view[field] = availability(&device[field]);
        }
    }

    if has("flows") {
        view["flows"] = flows::flows_view(device, records);
    }

    if let Some(seen) = last_seen(device) {
        view["off_the_network"] = json!(format!("{seen}; everything below is how it was then"));
    }

    compact(view)
}

fn availability(value: &Value) -> Value {
    let Some(operations) = value.as_object() else {
        return json!({});
    };
    let view = operations.iter().map(|(operation, state)| {
        let result = if !state.is_object() {
            state.clone()
        } else if truthy(&state["writable"]) {
            json!("writable")
        } else {
            let reasons: Vec<_> = values(&state["reasons"]).map(text).collect();
            let unconfirmed = operation != "redundancy"
                && !reasons.is_empty()
                && reasons.iter().all(|reason| {
                    [
                        "capability_unknown",
                        "lock_state_unknown",
                        "update_mode_unknown",
                    ]
                    .contains(&reason.as_str())
                });
            let prefix = if unconfirmed {
                "unconfirmed, a write will be tried"
            } else {
                "refused"
            };
            let reasons = if reasons.is_empty() {
                "no reason given".to_owned()
            } else {
                reasons.join(", ")
            };
            json!(format!("{prefix}: {reasons}"))
        };
        (operation.clone(), result)
    });
    Value::Object(view.collect())
}
pub mod panels;
