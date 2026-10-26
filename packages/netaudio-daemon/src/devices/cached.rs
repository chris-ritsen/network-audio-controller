use crate::monitoring::journal::remembered;
use caseless::Caseless;
use chrono::DateTime;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub fn include_remembered(
    records: &mut BTreeMap<String, Arc<Value>>,
    remembered: remembered::Devices,
) {
    let mut names: BTreeSet<_> = records
        .values()
        .filter_map(|record| record["name"].as_str())
        .map(fold)
        .collect();
    let mut remembered: Vec<_> = remembered.into_iter().collect();
    remembered.sort_by_cached_key(|(identity, device)| {
        (
            std::cmp::Reverse(
                device
                    .seen_at
                    .as_deref()
                    .and_then(|time| DateTime::parse_from_rfc3339(time).ok()),
            ),
            identity.clone(),
        )
    });

    for (identity, device) in remembered {
        let Some(name) = device.record["name"]
            .as_str()
            .filter(|name| !name.is_empty())
        else {
            continue;
        };
        if records.contains_key(&identity) || names.contains(&fold(name)) {
            continue;
        }

        let mut record = device.record.as_ref().clone();
        record["online"] = json!(false);
        if record.get("cmc_process_state").is_none() {
            for (field, value) in
                serde_json::to_value(netaudio_core::conmon_identity::Identity::default())
                    .expect("monitoring identity is serializable")
                    .as_object()
                    .expect("monitoring identity is an object")
            {
                record[field] = value.clone();
            }
        }
        record["association_state"] = json!("none");
        record["association_generation"] = Value::Null;
        record["remembered_last_seen"] = json!(device.seen_at);
        record["last_seen"] = json!(device.seen_at);
        names.insert(fold(name));
        records.insert(format!("remembered:{identity}"), Arc::new(record));
    }
}

fn fold(value: &str) -> String {
    value.chars().default_case_fold().collect()
}
