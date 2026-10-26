use super::model::{EvidenceClass, Finding, Issue, Kind, Scope};
use crate::monitoring::journal::model::Severity;
use anyhow::Result;
use serde_json::{Map, Value, json};

const CONFIGURATION_PAIRS: &[(&str, &str)] = &[
    ("requested_sample_rate", "sample_rate_hz"),
    ("requested_encoding", "encoding"),
    (
        "requested_sample_rate_pullup_raw_value",
        "sample_rate_pullup_raw_value",
    ),
    ("aes67_configured", "aes67_current"),
];
const NETWORK_CONFIGURATION_FIELDS: &[&str] =
    &["mode", "ip_address", "netmask", "gateway", "dns_server"];

pub fn detect(record: &Value, timestamp: &str) -> Result<Vec<Issue>> {
    let mut issues = Vec::new();
    let availability = record["availability_state"]
        .as_str()
        .unwrap_or_default()
        .to_lowercase();
    let states = [
        (
            record["is_licensed"] == false || record["license_valid"] == false,
            Kind::LicensingFailure,
            Severity::Error,
            "Device licensing failed",
            "The device explicitly reports an invalid or unavailable license".to_owned(),
            json!({"is_licensed": record["is_licensed"], "license_valid": record["license_valid"], "ddm_status": record["ddm_status"]}),
            "device_license_status",
            "Review the reported license state and vendor licensing workflow.",
        ),
        (
            record["safe_mode"] == true || matches!(availability.as_str(), "safe" | "safe_mode"),
            Kind::SafeState,
            Severity::Error,
            "Device is in a safe state",
            format!("Availability state is {}", record["availability_state"]),
            json!({"safe_mode": record["safe_mode"], "availability_state": record["availability_state"]}),
            "device_status",
            "Review device diagnostics and vendor recovery guidance before making configuration changes.",
        ),
        (
            record["upgrade_required"] == true || availability == "upgrade_required",
            Kind::UpgradeRequired,
            Severity::Warning,
            "Device upgrade is required",
            "The device explicitly reports that an upgrade is required".to_owned(),
            json!({"upgrade_required": record["upgrade_required"], "availability_state": record["availability_state"]}),
            "device_status",
            "Review the vendor-supported upgrade path; NetAudio does not start upgrades automatically.",
        ),
        (
            record["interface_reboot_required"] == true
                || record["dante_redundancy"]["reboot_required"] == true,
            Kind::RebootRequired,
            Severity::Warning,
            "Configuration requires a reboot",
            "Reported network configuration has not become effective".to_owned(),
            json!({"interface_reboot_required": record["interface_reboot_required"], "redundancy": record["dante_redundancy"]}),
            "interface_status",
            "Schedule and authorize a device reboot after reviewing the pending configuration.",
        ),
    ];

    for (bad, kind, severity, title, summary, raw, source, remediation) in states {
        if bad {
            issues.push(Issue::candidate(
                Scope::device(record)?,
                timestamp,
                Finding {
                    kind,
                    severity,
                    title: title.to_owned(),
                    summary,
                    raw,
                    source,
                    evidence_class: EvidenceClass::DirectObservation,
                    remediation,
                },
            )?);
        }
    }

    for (field, kind, title) in [
        (
            "configuration_partition_status",
            Kind::ConfigurationPartitionFault,
            "Configuration partition fault",
        ),
        (
            "persistence_status",
            Kind::PersistenceFault,
            "Configuration persistence fault",
        ),
    ] {
        let value = &record[field];
        if value == false
            || value.as_str().is_some_and(|value| {
                matches!(value.to_lowercase().as_str(), "error" | "failed" | "fault")
            })
        {
            let finding = Finding {
                kind,
                severity: Severity::Error,
                title: title.to_owned(),
                summary: format!("{field} reports {value}"),
                raw: json!({field: value}),
                source: "device_configuration_status",
                evidence_class: EvidenceClass::DirectObservation,
                remediation: "Inspect device diagnostics and preserve the current configuration before recovery work.",
            };
            issues.push(Issue::candidate(
                Scope::device(record)?,
                timestamp,
                finding,
            )?);
        }
    }

    telemetry(record, timestamp, &mut issues)?;
    divergence(record, timestamp, &mut issues)?;
    Ok(issues)
}

fn telemetry(record: &Value, timestamp: &str, issues: &mut Vec<Issue>) -> Result<()> {
    let mut stale = Vec::new();

    for field in [
        "receiver_flow_connection_health",
        "network_interface_traffic",
    ] {
        let value = &record[field];
        if value["fresh"] == false {
            stale.push(json!({"field": field, "value": value}));
        }

        for path in value["paths"].as_array().into_iter().flatten() {
            for name in ["latency", "late_packets"] {
                let series = &path[name];
                if series["current"].is_object() && series["fresh"] == false {
                    stale.push(json!({
                        "field": format!("Telemetry index {} {name}", path["telemetry_index"]),
                        "value": series,
                    }));
                }
            }
        }
    }

    if !stale.is_empty() {
        let finding = Finding {
            kind: Kind::TelemetryStale,
            severity: Severity::Warning,
            title: "Monitoring telemetry is stale".to_owned(),
            summary: stale
                .iter()
                .filter_map(|entry| entry["field"].as_str())
                .collect::<Vec<_>>()
                .join(", "),
            raw: json!({"stale_streams": stale}),
            source: "telemetry_freshness",
            evidence_class: EvidenceClass::DirectObservation,
            remediation: "Inspect the monitoring path and device availability; absence is not treated as silence or health.",
        };
        issues.push(Issue::candidate(
            Scope::device(record)?,
            timestamp,
            finding,
        )?);
    }

    if let Some(failed) = record["failed_queries"]
        .as_array()
        .filter(|failed| !failed.is_empty())
    {
        let finding = Finding {
            kind: Kind::TelemetryMissing,
            severity: Severity::Warning,
            title: "Required telemetry is missing".to_owned(),
            summary: format!("{} query or observation source(s) failed", failed.len()),
            raw: json!({"failed_queries": failed, "device_error": record["error"]}),
            source: "observation_failures",
            evidence_class: EvidenceClass::DirectObservation,
            remediation: "Restore the failed observation path before drawing conclusions from missing fields.",
        };
        issues.push(Issue::candidate(
            Scope::device(record)?,
            timestamp,
            finding,
        )?);
    }

    Ok(())
}

fn divergence(record: &Value, timestamp: &str, issues: &mut Vec<Issue>) -> Result<()> {
    let mut differences = Map::new();

    for &(requested, effective) in CONFIGURATION_PAIRS {
        let requested_value = &record[requested];
        let effective_value = &record[effective];
        if matches!(requested, "requested_sample_rate" | "requested_encoding")
            && requested_value == 0
        {
            continue;
        }

        if !requested_value.is_null()
            && !effective_value.is_null()
            && requested_value != effective_value
        {
            differences.insert(
                effective.to_owned(),
                json!({"requested": requested_value, "effective": effective_value}),
            );
        }
    }

    for interface in super::network::interfaces(record) {
        let mut changed = Map::new();
        for field in NETWORK_CONFIGURATION_FIELDS {
            if let (Some(configured), Some(active)) = (
                interface.configured.get(*field),
                interface.active.get(*field),
            ) && !configured.is_null()
                && !active.is_null()
                && configured != active
            {
                changed.insert(
                    (*field).to_owned(),
                    json!({"requested": configured, "effective": active}),
                );
            }
        }

        if !changed.is_empty() {
            differences.insert(
                format!("interface:{}", interface.identity),
                Value::Object(changed),
            );
        }
    }

    if !differences.is_empty() {
        let finding = Finding {
            kind: Kind::ConfigurationDivergence,
            severity: Severity::Warning,
            title: "Requested and effective configuration differ".to_owned(),
            summary: format!(
                "{} requested setting group(s) have not become effective",
                differences.len()
            ),
            raw: json!({"differences": differences}),
            source: "configuration_readback",
            evidence_class: EvidenceClass::DerivedState,
            remediation: "Review pending reboot state and fresh readback before retrying any mutation.",
        };
        issues.push(Issue::candidate(
            Scope::device(record)?,
            timestamp,
            finding,
        )?);
    }

    Ok(())
}

pub fn configuration_pairs() -> &'static [(&'static str, &'static str)] {
    CONFIGURATION_PAIRS
}
