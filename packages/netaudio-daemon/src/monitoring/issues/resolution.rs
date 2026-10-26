use super::model::{Issue, Kind};
use super::{clock, configuration, network, receivers};
use crate::monitoring::journal::model::Thresholds;
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub fn depends_on(issue: &Issue, identity: &str) -> bool {
    if issue.scope.device_identity == identity {
        return true;
    }
    match issue.kind {
        Kind::AddressConflict => issue.raw_source_fields["reporting_interfaces"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|record| record["device_identity"] == identity),
        Kind::PullupMismatch => issue.raw_source_fields["reported_values"]
            .get(identity)
            .is_some(),
        _ => false,
    }
}

pub fn recovered(
    issue: &Issue,
    records: &BTreeMap<String, Arc<Value>>,
    clock: &clock::Recovery,
    thresholds: Thresholds,
    timestamp: &str,
) -> Result<bool> {
    let Some(record) = records
        .get(&issue.scope.device_identity)
        .filter(|record| record["online"] != false)
    else {
        return Ok(false);
    };
    if matches!(issue.kind, Kind::AddressConflict | Kind::PullupMismatch) {
        let dependencies = match issue.kind {
            Kind::AddressConflict => issue.raw_source_fields["reporting_interfaces"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|item| item["device_identity"].as_str())
                .collect::<Vec<_>>(),
            _ => issue.raw_source_fields["reported_values"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(identity, _)| identity.as_str())
                .collect(),
        };
        return Ok(!dependencies.is_empty()
            && dependencies.into_iter().all(|identity| {
                records.get(identity).is_some_and(|record| {
                    record["online"] != false
                        && if issue.kind == Kind::AddressConflict {
                            issue.raw_source_fields["reporting_interfaces"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter(|member| member["device_identity"] == identity)
                                .all(|member| {
                                    member["interface"]["identity"].as_str().is_some_and(
                                        |interface| network::address_observable(record, interface),
                                    )
                                })
                        } else {
                            record["sample_rate_pullup_raw_value"].as_u64().is_some()
                                && network::clock_group(record).is_some()
                        }
                })
            }));
    }

    Ok(match issue.kind {
        Kind::BluetoothLinkLost | Kind::VideoSignalFailure => {
            clock::panel_recovered(issue, record, timestamp)?
        }
        Kind::SubscriptionFailure
        | Kind::ReceiverHealthDegraded
        | Kind::ReceiverLatencyPressure
        | Kind::ReceiverLatePackets => receivers::recovered(issue, record, thresholds),
        Kind::ClockSynchronization | Kind::ClockMuted => {
            if issue.evidence_source == "device_clock_status" {
                clock.confirmed(issue) && clock::fresh_clock(record, timestamp).is_some()
            } else {
                let managed = &record["ddm_clocking_state"];
                (managed["locked"] == true
                    || managed["locked"].as_str().is_some_and(|value| {
                        matches!(value.to_lowercase().as_str(), "locked" | "true" | "ok")
                    }))
                    && managed["follower_without_leader"] != true
            }
        }
        Kind::ClockFrequencyVariation => {
            let source = issue
                .scope
                .interface_identity
                .as_deref()
                .unwrap_or_default();
            let clock = &record["clock_observations"];
            clock["warning_enabled"] == true
                && clock["variation"][source]["observable"] == true
                && clock["variation"][source]["recovered"] == true
        }
        Kind::TelemetryMissing => record["failed_queries"]
            .as_array()
            .is_some_and(Vec::is_empty),
        Kind::TelemetryStale => issue.raw_source_fields["stale_streams"]
            .as_array()
            .is_some_and(|streams| {
                !streams.is_empty()
                    && streams.iter().all(|stream| match stream["field"].as_str() {
                        Some("receiver_flow_connection_health" | "network_interface_traffic") => {
                            record[stream["field"].as_str().unwrap_or_default()]["fresh"] == true
                        }
                        Some(field) => record["receiver_flow_connection_health"]["paths"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|path| {
                                ["latency", "late_packets"].iter().any(|metric| {
                                    field
                                        == format!(
                                            "Telemetry index {} {metric}",
                                            path["telemetry_index"]
                                        )
                                        && path[*metric]["fresh"] == true
                                        && path[*metric]["current"].is_object()
                                })
                            }),
                        None => false,
                    })
            }),
        Kind::SubnetConflict => network::subnet_observable(issue, record),
        Kind::LicensingFailure => ["is_licensed", "license_valid"]
            .iter()
            .any(|field| record[*field] == true),
        Kind::SafeState => record["safe_mode"] == false,
        Kind::UpgradeRequired => record["upgrade_required"] == false,
        Kind::RebootRequired => {
            record["interface_reboot_required"] == false
                || record["dante_redundancy"]["reboot_required"] == false
        }
        Kind::ConfigurationPartitionFault => record["configuration_partition_status"] == true,
        Kind::PersistenceFault => record["persistence_status"] == true,
        Kind::ConfigurationDivergence => {
            let differences = issue.raw_source_fields["differences"].as_object();
            differences.is_some_and(|differences| {
                !differences.is_empty()
                    && differences.keys().all(|effective| {
                        if let Some((requested, _)) = configuration::configuration_pairs()
                            .iter()
                            .find(|(_, field)| *field == effective)
                        {
                            !record[*requested].is_null()
                                && !record[effective].is_null()
                                && record[*requested] == record[effective]
                        } else if let Some(identity) = effective.strip_prefix("interface:") {
                            network::interfaces(record)
                                .iter()
                                .find(|interface| interface.identity == identity)
                                .is_some_and(|interface| {
                                    differences[effective].as_object().is_some_and(|fields| {
                                        fields.keys().all(|field| {
                                            !interface.configured[field].is_null()
                                                && !interface.active[field].is_null()
                                                && interface.configured[field]
                                                    == interface.active[field]
                                        })
                                    })
                                })
                        } else {
                            false
                        }
                    })
            })
        }
        Kind::AddressConflict | Kind::PullupMismatch => false,
    })
}
