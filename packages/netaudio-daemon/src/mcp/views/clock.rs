use super::{Records, compact, first, local_time, text, truthy};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

pub fn clock_view(device: &Value) -> Value {
    let clocking = &device["ddm_clocking_state"];
    let preferences = &device["ddm_clock_preferences"];
    let mut role = device["clock_role"].clone();

    if role.is_null() && clocking.get("grand_leader").is_some() {
        role = json!(if truthy(&clocking["grand_leader"]) {
            "Leader"
        } else {
            "Follower"
        });
    }

    let preferred = if device["preferred_leader"].is_null() {
        &preferences["leader"]
    } else {
        &device["preferred_leader"]
    };
    let status = &device["clock_status"];
    let mut synchronization = status["synchronization"].clone();

    if (synchronization.is_null() || synchronization == "unknown") && clocking["locked"] == "LOCKED"
    {
        synchronization = json!("synchronized");
    }

    compact(json!({
        "clock_observed_at": local_time(&device["clock_observed_at"]),
        "ddm_frequency_offset": if device["management_state"] == "managed" { &clocking["frequency_offset"] } else { &Value::Null },
        "frequency_offset_parts_per_billion": device["clock_frequency_offset_parts_per_billion"],
        "synchronization": synchronization,
        "clock_state": status["clock_state"],
        "clock_source": status["clock_source"],
        "mute_state": status["mute_state"],
        "leader_evidence": truthy(&device["ptpv1_master_uuid"]).then_some("reported"),
        "locked": if truthy(&status["clock_state"]) { &Value::Null } else { &clocking["locked"] },
        "mute_status": if truthy(&status["mute_state"]) { &Value::Null } else { &clocking["mute_status"] },
        "preferred_leader": preferred,
        "role": role,
        "role_source": if !truthy(&role) { None } else if device["clock_role"].is_null() { Some("ddm") } else { Some("direct") },
    }))
}

fn domain(device: &Value) -> String {
    if device["management_state"] != "managed" {
        return "unmanaged".to_owned();
    }

    let parts: Vec<_> = device["inventory_id"]
        .as_str()
        .unwrap_or_default()
        .split(':')
        .collect();
    let fallback = |index| {
        if parts.first() == Some(&"ddm") {
            parts
                .get(index)
                .map(|value| json!(value))
                .unwrap_or_default()
        } else {
            Value::Null
        }
    };
    let server = first(&[&device["ddm_server_profile"], &fallback(1)]);
    let mut domain = first(&[&device["ddm_domain_name"], &device["ddm_domain_id"]]);

    if domain.is_null() && parts.get(2) != Some(&"unenrolled") {
        domain = fallback(2);
    }

    if truthy(&domain) {
        format!("ddm:{}:{}", text(&server), text(&domain))
    } else {
        format!("ddm:{}", text(&server))
    }
}

#[derive(Default)]
struct Domain {
    devices: Vec<Value>,
    leaders: Vec<String>,
}

pub fn clock_status(records: &Records, arguments: &Value) -> Value {
    let identities: BTreeMap<_, _> = records
        .iter()
        .filter_map(|(key, device)| {
            device["ptpv1_device_uuid"]
                .as_str()
                .map(|identity| (identity, device["name"].as_str().unwrap_or(key)))
        })
        .collect();
    let mut domains = BTreeMap::<String, Domain>::new();
    let mut devices: Vec<_> = records
        .iter()
        .filter(|(_, device)| device["online"] != false)
        .collect();
    devices.sort_by_key(|(key, device)| device["name"].as_str().unwrap_or(key));

    for (key, device) in devices {
        let name = device["name"].as_str().unwrap_or(key);
        let clock = clock_view(device);
        let leader = device["ptpv1_master_uuid"]
            .as_str()
            .and_then(|identity| identities.get(identity));
        let entry = compact(json!({
            "name": name,
            "role": clock["role"],
            "leader": leader,
            "preferred": clock["preferred_leader"],
            "synchronization": clock["synchronization"],
            "locked": clock["locked"],
            "offset_ppb": clock["frequency_offset_parts_per_billion"],
            "ddm_frequency_offset": clock["ddm_frequency_offset"],
            "leader_evidence": clock["leader_evidence"],
        }));
        let group = domains.entry(domain(device)).or_default();
        group.devices.push(entry);

        if clock["role"] == "Leader" {
            group.leaders.push(name.to_owned());
        }
    }

    let mut presentation = Map::new();

    for (name, mut group) in domains {
        for entry in &mut group.devices {
            if entry["role"] == "Leader" || entry.get("leader").is_some() {
                continue;
            }

            if group.leaders.len() == 1 {
                entry["leader"] = json!(group.leaders[0]);
                entry["leader_evidence"] = json!("domain");
            } else {
                entry["leader_evidence"] = json!("unknown");
            }
        }

        let preferred: Vec<_> = group
            .devices
            .iter()
            .filter(|device| truthy(&device["preferred"]))
            .map(|device| text(&device["name"]))
            .collect();
        let mut view = json!({"devices": group.devices, "leaders": group.leaders});

        if preferred.len() > 1 {
            let winners: Vec<_> = preferred
                .iter()
                .filter(|name| group.leaders.contains(name))
                .collect();
            let election = if winners.len() == 1 {
                format!("; {} won the election, so the others follow it", winners[0])
            } else {
                String::new()
            };
            view["note"] = json!(format!(
                "{} devices are set as preferred leader ({}){election}",
                preferred.len(),
                preferred.join(", ")
            ));
        }

        if group.leaders.len() > 1 {
            view["problem"] = json!(format!(
                "more than one leader in one clock domain: {}",
                group.leaders.join(", ")
            ));
        } else if group.leaders.is_empty() && !group.devices.is_empty() {
            view["problem"] = json!("no device in this domain reports being the leader");
        }

        presentation.insert(name, view);
    }

    let mut view = json!({"domains": presentation});

    if arguments["detail"] == "debug" {
        view["notes"] = json!([
            "offset_ppb comes from the device's own clock status. ddm_frequency_offset is Dante Domain Manager's figure, reported only for devices it manages, and has not been shown to be the same quantity; do not compare the two.",
            "leader_evidence reported means the device named its leader; domain means it was inferred from the only leader in its domain; unknown means the device did not say.",
        ]);
        view["raw_clock_records"] = Value::Object(
            records
                .iter()
                .map(|(key, device)| {
                    (
                        key.clone(),
                        compact(Value::Object(super::selected(
                            device,
                            &[
                                "clock_status",
                                "ptpv1_device_uuid",
                                "ptpv1_master_uuid",
                                "ptpv1_grandmaster_uuid",
                            ],
                        ))),
                    )
                })
                .collect(),
        );
    }

    view
}
