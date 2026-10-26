use super::model::{EvidenceClass, Finding, Issue, Kind, Scope};
use crate::monitoring::journal::model::{Severity, device_identity};
use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::Arc;

#[derive(Serialize)]
pub struct Interface<'a> {
    pub identity: String,
    pub address: Option<&'a str>,
    pub netmask: Option<&'a str>,
    pub gateway: Option<&'a str>,
    pub active: &'a Value,
    pub configured: &'a Value,
    pub raw: &'a Value,
}

pub fn interfaces(record: &Value) -> Vec<Interface<'_>> {
    record["interfaces"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter(|(_, raw)| raw.is_object())
        .map(|(index, raw)| {
            let active = raw
                .get("active")
                .filter(|value| value.is_object())
                .unwrap_or(raw);
            let configured = raw
                .get("configured")
                .filter(|value| value.is_object())
                .unwrap_or(raw);
            let identity = raw
                .get("interface")
                .or_else(|| raw.get("identity"))
                .filter(|value| !value.is_null())
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string())
                })
                .unwrap_or_else(|| (index + 1).to_string());
            Interface {
                identity,
                address: active["ip_address"]
                    .as_str()
                    .or_else(|| active["address"].as_str()),
                netmask: active["netmask"]
                    .as_str()
                    .or_else(|| active["subnet_mask"].as_str()),
                gateway: configured["gateway"].as_str(),
                active,
                configured,
                raw,
            }
        })
        .collect()
}

fn network(interface: &Interface<'_>) -> Option<(u32, u32)> {
    let address: Ipv4Addr = interface.address?.parse().ok()?;
    let mask: Ipv4Addr = interface.netmask?.parse().ok()?;
    let mask = u32::from(mask);
    let prefix = mask.leading_ones();
    let expected = u32::MAX.checked_shl(u32::BITS - prefix).unwrap_or_default();
    (mask == expected).then_some((u32::from(address) & mask, mask))
}

pub fn subnet_observable(issue: &Issue, record: &Value) -> bool {
    let current = interfaces(record);
    let evidence = &issue.raw_source_fields;
    if let (Some(left), Some(right)) = (
        evidence["left"]["identity"].as_str(),
        evidence["right"]["identity"].as_str(),
    ) {
        return [left, right].into_iter().all(|identity| {
            current
                .iter()
                .any(|interface| interface.identity == identity && network(interface).is_some())
        });
    }

    current
        .iter()
        .find(|interface| {
            Some(interface.identity.as_str()) == issue.scope.interface_identity.as_deref()
        })
        .is_some_and(|interface| {
            network(interface).is_some()
                && interface
                    .gateway
                    .is_some_and(|gateway| gateway.parse::<Ipv4Addr>().is_ok())
        })
}

pub fn address_observable(record: &Value, identity: &str) -> bool {
    interfaces(record)
        .iter()
        .find(|interface| interface.identity == identity)
        .and_then(|interface| interface.address)
        .and_then(|address| address.parse::<Ipv4Addr>().ok())
        .is_some_and(|address| !address.is_unspecified())
}

pub fn local(record: &Value, timestamp: &str) -> Result<Vec<Issue>> {
    let interfaces = interfaces(record);
    let networks = interfaces
        .iter()
        .filter_map(|interface| network(interface).map(|network| (interface, network)))
        .collect::<Vec<_>>();
    let mut issues = Vec::new();

    for (interface, (address, mask)) in &networks {
        let Some(gateway) = interface.gateway.filter(|gateway| *gateway != "0.0.0.0") else {
            continue;
        };
        let reachable = gateway
            .parse::<Ipv4Addr>()
            .is_ok_and(|gateway| u32::from(gateway) & mask == *address);
        if !reachable {
            let mut scope = Scope::device(record)?;
            scope.interface_identity = Some(interface.identity.clone());
            let finding = Finding {
                kind: Kind::SubnetConflict,
                severity: Severity::Error,
                title: "Gateway is outside the interface subnet".to_owned(),
                summary: format!(
                    "Interface {} reports gateway {gateway} outside {}/{}",
                    interface.identity,
                    Ipv4Addr::from(*address),
                    mask.leading_ones(),
                ),
                raw: json!(interface),
                source: "interface_status",
                evidence_class: EvidenceClass::DerivedState,
                remediation: "Review the configured address, netmask and gateway; no change is sent automatically.",
            };
            issues.push(Issue::candidate(scope, timestamp, finding)?);
        }
    }

    for (index, (left, (left_address, left_mask))) in networks.iter().enumerate() {
        for (right, (right_address, right_mask)) in networks.iter().skip(index + 1) {
            let common_mask = left_mask & right_mask;
            if left_address & common_mask != right_address & common_mask {
                continue;
            }

            let mut scope = Scope::device(record)?;
            scope.interface_identity = Some(format!("{}+{}", left.identity, right.identity));
            let finding = Finding {
                kind: Kind::SubnetConflict,
                severity: Severity::Warning,
                title: "Device interfaces use overlapping subnets".to_owned(),
                summary: format!(
                    "Interfaces {} and {} overlap",
                    left.identity, right.identity
                ),
                raw: json!({"left": left, "right": right}),
                source: "interface_status",
                evidence_class: EvidenceClass::DerivedState,
                remediation: "Confirm the intended switched or redundant topology and assign distinct subnets when required.",
            };
            issues.push(Issue::candidate(scope, timestamp, finding)?);
        }
    }

    Ok(issues)
}

pub fn cross(records: &BTreeMap<String, Arc<Value>>, timestamp: &str) -> Result<Vec<Issue>> {
    let mut addresses = BTreeMap::<Ipv4Addr, Vec<(&Value, Interface<'_>)>>::new();
    let mut groups = BTreeMap::<String, Vec<(&Value, u64)>>::new();

    for record in records.values().filter(|record| record["online"] != false) {
        for interface in interfaces(record) {
            if let Some(address) = interface
                .address
                .and_then(|value| value.parse::<Ipv4Addr>().ok())
                .filter(|address| !address.is_unspecified())
            {
                addresses
                    .entry(address)
                    .or_default()
                    .push((record, interface));
            }
        }

        if let (Some(group), Some(value)) = (
            clock_group(record),
            record["sample_rate_pullup_raw_value"].as_u64(),
        ) {
            groups.entry(group).or_default().push((record, value));
        }
    }

    let mut issues = Vec::new();
    for (address, members) in addresses {
        let identities = members
            .iter()
            .map(|(record, _)| device_identity(record))
            .collect::<Result<BTreeSet<_>>>()?;
        if identities.len() < 2 {
            continue;
        }
        let peers = members
            .iter()
            .map(|(record, interface)| {
                Ok(json!({"device_identity": device_identity(record)?, "interface": interface}))
            })
            .collect::<Result<Vec<_>>>()?;

        for (record, interface) in &members {
            let mut scope = Scope::device(record)?;
            scope.interface_identity = Some(interface.identity.clone());
            let finding = Finding {
                kind: Kind::AddressConflict,
                severity: Severity::Error,
                title: "IPv4 address is used by multiple devices".to_owned(),
                summary: format!(
                    "Address {address} is reported by {} interfaces",
                    members.len()
                ),
                raw: json!({"address": address.to_string(), "reporting_interfaces": peers}),
                source: "interface_status",
                evidence_class: EvidenceClass::DerivedState,
                remediation: "Resolve the duplicate address in DHCP or static configuration before changing routes.",
            };
            issues.push(Issue::candidate(scope, timestamp, finding)?);
        }
    }

    for (group, members) in groups {
        let values = members
            .iter()
            .map(|(_, value)| *value)
            .collect::<BTreeSet<_>>();
        if values.len() < 2 {
            continue;
        }
        let reported = members
            .iter()
            .map(|(record, value)| Ok((device_identity(record)?, *value)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        for (record, value) in members {
            let finding = Finding {
                kind: Kind::PullupMismatch,
                severity: Severity::Warning,
                title: "Sample-rate pull-up differs across devices".to_owned(),
                summary: format!(
                    "This device reports raw value {value}; its observed clock group reports {values:?}"
                ),
                raw: json!({"reported_values": reported, "clock_group": group}),
                source: "sample_rate_pullup_status",
                evidence_class: EvidenceClass::DerivedState,
                remediation: "Inspect the reported clock group and pull-up settings before making changes.",
            };
            issues.push(Issue::candidate(
                Scope::device(record)?,
                timestamp,
                finding,
            )?);
        }
    }

    Ok(issues)
}

pub fn clock_group(record: &Value) -> Option<String> {
    if record["management_state"] == "managed" {
        let server = record["ddm_server_profile"]
            .as_str()
            .filter(|value| !value.is_empty())?;
        let domain = record["ddm_domain_id"]
            .as_str()
            .filter(|value| !value.is_empty())?;
        return Some(json!(["ddm", server, domain]).to_string());
    }

    if record["clock_status"]["observation_state"] != "fresh" {
        return None;
    }

    let leader = record["ptpv1_master_uuid"].as_str()?;
    let canonical = leader.replace([':', '-'], "").to_lowercase();
    if canonical.is_empty() || canonical.bytes().all(|byte| byte == b'0') {
        return None;
    }

    let subdomain = record
        .get("clock_subdomain")
        .filter(|value| !value.is_null())?;
    Some(json!(["ptpv1", canonical, subdomain]).to_string())
}
