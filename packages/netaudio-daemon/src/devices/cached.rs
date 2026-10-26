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
    let mut addresses: BTreeSet<_> = records
        .values()
        .filter_map(|record| record["mac_address"].as_str())
        .filter_map(canonical_address)
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
        let address = device.record["mac_address"]
            .as_str()
            .and_then(canonical_address);
        if records.contains_key(&identity)
            || names.contains(&fold(name))
            || address
                .as_ref()
                .is_some_and(|address| addresses.contains(address))
        {
            continue;
        }

        let mut record = device.record.as_ref().clone();
        record["online"] = json!(false);
        record["remembered_last_seen"] = json!(device.seen_at);
        record["last_seen"] = json!(device.seen_at);
        names.insert(fold(name));
        if let Some(address) = address {
            addresses.insert(address);
        }
        records.insert(format!("remembered:{identity}"), Arc::new(record));
    }
}

fn fold(value: &str) -> String {
    value.chars().default_case_fold().collect()
}

fn canonical_address(value: &str) -> Option<String> {
    const ADDRESS_HEX_DIGITS: usize = 12;
    let value = value.replace([':', '-'], "").to_lowercase();
    (value.len() == ADDRESS_HEX_DIGITS
        && value.bytes().all(|digit| digit.is_ascii_hexdigit())
        && value.bytes().any(|digit| digit != b'0'))
    .then_some(value)
}
