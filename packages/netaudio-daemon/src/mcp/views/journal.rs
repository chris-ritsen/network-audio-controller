use super::{compact, local_time};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

const DEFAULT_EVENT_LIMIT: usize = 20;
const EVENT_FIELDS: &[&str] = &[
    "channel_identity",
    "device_name",
    "flow_identity",
    "interface_identity",
    "kind",
    "sequence",
    "severity",
];

struct Group<'a> {
    latest: &'a Value,
    count: usize,
    first_observed: &'a Value,
}

pub fn events_view(payload: &Value, arguments: &Value) -> Value {
    let debug = arguments["detail"] == "debug";
    let collapse = arguments["collapse_repeats"] != false;
    let limit = arguments["limit"]
        .as_u64()
        .map(|value| value as usize)
        .unwrap_or(DEFAULT_EVENT_LIMIT);
    let mut groups = Vec::<Group<'_>>::new();
    let mut identities = BTreeMap::new();
    let mut observations = 0;

    for event in payload["events"].as_array().into_iter().flatten() {
        if !debug && hidden_in_compact(event) {
            continue;
        }

        observations += 1;
        let identity = identity(event);

        if collapse && let Some(index) = identities.get(&identity).copied() {
            let group: &mut Group<'_> = &mut groups[index];
            group.count += 1;
            group.first_observed = &event["timestamp"];
            continue;
        }

        identities.insert(identity, groups.len());
        groups.push(Group {
            latest: event,
            count: 1,
            first_observed: &event["timestamp"],
        });
    }

    let views: Vec<_> = groups
        .iter()
        .take(limit)
        .map(|group| {
            if debug {
                return group.latest.clone();
            }

            let event = group.latest;
            let mut view: Map<_, _> = EVENT_FIELDS
                .iter()
                .map(|field| ((*field).to_owned(), event[*field].clone()))
                .collect();
            view.insert("time".to_owned(), json!(local_time(&event["timestamp"])));

            match event["kind"].as_str() {
                Some("setting_changed" | "channel_renamed" | "route_changed") => {
                    let before = &event["previous_value"];
                    let after = &event["current_value"];
                    let field = after
                        .as_object()
                        .and_then(|value| value.keys().next())
                        .or_else(|| before.as_object().and_then(|value| value.keys().next()));
                    if let Some(field) = field {
                        view.insert(
                            "change".to_owned(),
                            json!({"what": field, "from": before[field], "to": after[field]}),
                        );
                    }
                }
                Some("configuration_operation" | "preset_run") => {
                    view.extend([
                        ("operation".to_owned(), event["operation_name"].clone()),
                        ("requested".to_owned(), event["requested_values"].clone()),
                        ("result".to_owned(), event["final_operation_state"].clone()),
                        ("phase".to_owned(), event["lifecycle_phase"].clone()),
                        ("message".to_owned(), event["raw"]["message"].clone()),
                    ]);
                }
                _ => {
                    view.insert("previous_value".to_owned(), event["previous_value"].clone());
                    view.insert("current_value".to_owned(), event["current_value"].clone());
                }
            }

            if group.count > 1 {
                view.insert("observation_count".to_owned(), json!(group.count));
                view.insert(
                    "first_observed_at".to_owned(),
                    json!(local_time(group.first_observed)),
                );
            }

            compact(Value::Object(view))
        })
        .collect();
    let next = if groups.len() > views.len() {
        views
            .last()
            .and_then(|view| view["sequence"].as_u64())
            .map(|value| json!(value))
    } else {
        payload.get("next_before_sequence").cloned()
    };
    let gaps: Vec<_> = payload["observation_gaps"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|gap| {
            if debug {
                return gap.clone();
            }

            json!({
                "from": local_time(&gap["stopped_at"]),
                "to": local_time(&gap["started_at"]),
                "reason": gap["reason"],
            })
        })
        .collect();
    compact(json!({
        "events": views,
        "matched": groups.len(),
        "observation_count": observations,
        "returned": views.len(),
        "next_before_sequence": next,
        "total": payload["count"],
        "recorded_since": if debug { payload["recorded_since"].clone() } else { json!(local_time(&payload["recorded_since"])) },
        "not_recorded": (!gaps.is_empty()).then_some(gaps),
        "retention": "kept across restarts; recorded gaps identify intervals without guaranteed observations",
    }))
}

fn identity(event: &Value) -> String {
    match event["kind"].as_str() {
        Some("configuration_operation" | "preset_run") if !event["operation_id"].is_null() => {
            json!([event["kind"], event["operation_id"]]).to_string()
        }
        Some("setting_changed" | "channel_renamed" | "route_changed") => {
            json!([event["kind"], event["sequence"]]).to_string()
        }
        _ => json!([
            event["kind"],
            event["device_identity"],
            event["flow_identity"],
            event["interface_identity"],
            event["channel_identity"],
            event["current_value"]["issue_id"],
            event["previous_value"]["issue_id"],
            event["raw"]["category"],
            event["raw"]["counter"],
        ])
        .to_string(),
    }
}

fn hidden_in_compact(event: &Value) -> bool {
    if event["kind"] == "configuration_operation" {
        return event["operation_name"]
            .as_str()
            .is_some_and(|name| name.starts_with("validate_"))
            || event["final_operation_state"] == "planned";
    }

    let issue = if event["kind"] == "issue_resolved" {
        &event["previous_value"]
    } else {
        &event["current_value"]
    };
    issue["kind"] == "subscription_failure"
        && issue["raw_source_fields"]["status"]["status"] == "UNRESOLVED"
}
