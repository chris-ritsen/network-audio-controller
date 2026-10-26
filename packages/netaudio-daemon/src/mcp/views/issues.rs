use super::{Records, channels, compact, last_seen, local_time, resolve, subscriptions, waiting};
use caseless::Caseless;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_LIMIT: usize = 50;
const DEBUG_FIELDS: &[&str] = &[
    "first_seen",
    "issue_id",
    "kind",
    "last_seen",
    "observation_state",
    "occurrence_count",
    "resolved_at",
    "severity",
    "state",
    "summary",
    "title",
];

#[derive(Default)]
struct Group<'a> {
    issues: Vec<&'a Value>,
    channels: BTreeSet<String>,
    flows: BTreeSet<String>,
    interfaces: BTreeSet<String>,
    sources: BTreeMap<String, BTreeSet<String>>,
}

pub fn issues_view(payload: &Value, arguments: &Value, records: &Records) -> Value {
    let limit = arguments["limit"]
        .as_u64()
        .map(|value| value as usize)
        .unwrap_or(DEFAULT_LIMIT);
    let offset = arguments["offset"].as_u64().unwrap_or_default() as usize;
    let state = arguments["state"].as_str().unwrap_or("open");
    let issues: Vec<_> = payload["issues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|issue| state == "all" || issue["state"] == state)
        .filter(|issue| {
            arguments["device"].as_str().is_none_or(|device| {
                ["device_identity", "device_name", "server_name"]
                    .iter()
                    .any(|field| {
                        issue["scope"][*field].as_str().is_some_and(|identity| {
                            identity
                                .chars()
                                .default_case_fold()
                                .eq(device.chars().default_case_fold())
                                || identity.chars().default_case_fold().eq(format!(
                                    "{device}.local."
                                )
                                .chars()
                                .default_case_fold())
                        })
                    })
            })
        })
        .collect();

    if arguments["detail"] == "debug" {
        let selected: Vec<_> = issues
            .iter()
            .skip(offset)
            .take(limit)
            .map(|issue| {
                let mut view = json!({});
                for field in DEBUG_FIELDS {
                    view[*field] = issue[*field].clone();
                }
                view["device"] = json!(device_name(issue));
                view["channel"] = issue["scope"]["channel_identity"].clone();
                view["flow"] = issue["scope"]["flow_identity"].clone();
                view["interface"] = issue["scope"]["interface_identity"].clone();
                view
            })
            .collect();
        return compact(json!({
            "active_count": payload["active_count"],
            "matched": issues.len(),
            "returned": selected.len(),
            "next_offset": (offset.saturating_add(selected.len()) < issues.len())
                .then_some(offset.saturating_add(selected.len())),
            "total": payload["count"],
            "issues": selected,
        }));
    }

    let mut groups = BTreeMap::<[String; 6], Group<'_>>::new();
    for issue in issues {
        let name = device_name(issue);
        let record = resolve(records, &name).ok().flatten();
        if arguments["device"].is_null() && record.is_some_and(|record| record["online"] == false) {
            continue;
        }

        let channel = issue["scope"]["channel_identity"].as_str();
        let source = record.and_then(|record| route_source(record, channel));
        if issue["state"] == "open"
            && source.is_some_and(|source| waiting(source, records).is_some())
        {
            continue;
        }

        let key = [
            issue["kind"].as_str().unwrap_or_default().to_owned(),
            issue["severity"].as_str().unwrap_or_default().to_owned(),
            name,
            issue["summary"].as_str().unwrap_or_default().to_owned(),
            issue["state"].as_str().unwrap_or_default().to_owned(),
            issue["observation_state"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        ];
        let group = groups.entry(key).or_default();
        group.issues.push(issue);
        for (field, target) in [
            ("channel_identity", &mut group.channels),
            ("flow_identity", &mut group.flows),
            ("interface_identity", &mut group.interfaces),
        ] {
            if let Some(identity) = issue["scope"][field].as_str() {
                target.insert(identity.to_owned());
            }
        }
        if let (Some(source), Some(channel)) = (source, channel) {
            group
                .sources
                .entry(source.to_owned())
                .or_default()
                .insert(channel.to_owned());
        }
    }

    let mut problems: Vec<_> = groups
        .into_iter()
        .map(|(key, group)| {
            let [kind, severity, name, summary, lifecycle, observation] = key;
            let record = resolve(records, &name).ok().flatten();
            let sources = group.sources.into_iter()
                .map(|(source, channels)| format!("{source}: {}", identities(&channels)))
                .collect::<Vec<_>>()
                .join("; ");
            let first_seen = group_time(&group.issues, "first_seen", false);
            let last_observed = group_time(&group.issues, "last_seen", true);
            let resolved_at = group_time(&group.issues, "resolved_at", true);
            json!({
                "kind": kind,
                "severity": severity,
                "device": name,
                "summary": summary,
                "state": (state == "all").then_some(lifecycle),
                "observation_state": observation,
                "issue_count": group.issues.len(),
                "first_seen": first_seen,
                "last_seen": last_observed,
                "resolved_at": resolved_at,
                "channels": identities(&group.channels),
                "flows": identities(&group.flows),
                "interfaces": identities(&group.interfaces),
                "from": sources,
                "device_state": record.and_then(last_seen).map(|seen| format!("{seen}; this is its last report")),
            })
        })
        .collect();
    problems.sort_by_key(|problem| {
        (
            severity_order(&problem["severity"]),
            std::cmp::Reverse(problem["issue_count"].as_u64().unwrap_or_default()),
            problem["kind"].as_str().unwrap_or_default().to_owned(),
            problem["device"].as_str().unwrap_or_default().to_owned(),
        )
    });
    let count: u64 = problems
        .iter()
        .filter_map(|problem| problem["issue_count"].as_u64())
        .sum();
    let total = problems.len();
    let selected: Vec<_> = problems.into_iter().skip(offset).take(limit).collect();
    compact(json!({
        "problems": selected,
        "problem_count": count,
        "next_offset": (offset.saturating_add(selected.len()) < total)
            .then_some(offset.saturating_add(selected.len())),
        "detail": "detail=debug lists each issue separately with its identifier",
    }))
}

fn group_time(issues: &[&Value], field: &str, latest: bool) -> Option<String> {
    let timestamps = issues.iter().filter_map(|issue| {
        let text = issue[field].as_str()?;
        chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|time| (time, text))
    });
    let selected = if latest {
        timestamps.max_by_key(|(time, _)| *time)
    } else {
        timestamps.min_by_key(|(time, _)| *time)
    };
    selected.and_then(|(_, text)| local_time(&json!(text)))
}

fn device_name(issue: &Value) -> String {
    ["device_name", "server_name", "device_identity"]
        .iter()
        .find_map(|field| {
            issue["scope"][*field]
                .as_str()
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_default()
        .to_owned()
}

fn route_source<'a>(record: &'a Value, channel: Option<&str>) -> Option<&'a str> {
    let number = channel?.strip_prefix("rx:")?.parse::<u64>().ok()?;
    subscriptions(record)
        .find(|subscription| subscription["rx_channel_number"] == number)
        .and_then(|subscription| subscription["tx_device"].as_str())
}

fn identities(identities: &BTreeSet<String>) -> String {
    let mut groups = BTreeMap::<&str, Vec<u64>>::new();
    let mut others = Vec::new();
    for identity in identities {
        if let Some((direction, number)) = identity.split_once(':')
            && matches!(direction, "rx" | "tx")
            && let Ok(number) = number.parse()
        {
            groups.entry(direction).or_default().push(number);
        } else {
            others.push(identity.clone());
        }
    }
    let mut parts: Vec<_> = groups
        .into_iter()
        .map(|(direction, numbers)| format!("{direction} {}", channels::number_ranges(numbers)))
        .collect();
    parts.extend(others);
    parts.join("; ")
}

fn severity_order(severity: &Value) -> u8 {
    match severity.as_str() {
        Some("critical") => 0,
        Some("error") => 1,
        Some("warning") => 2,
        Some("info") => 3,
        _ => 4,
    }
}
