use super::Observation;
use crate::monitoring::journal::model::{EventKind, Severity};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub fn observe(observation: &mut Observation<'_>) -> Result<()> {
    if observation.current["online"] == false {
        return Ok(());
    }

    let before_health = &observation.previous["receiver_flow_connection_health"];
    let after_health = &observation.current["receiver_flow_connection_health"];
    let before_flows = flow_map(before_health);
    let after_flows = flow_map(after_health);

    for (identity, current) in after_flows {
        let Some(previous) = before_flows.get(&identity) else {
            continue;
        };
        let after = &current["late_packets"]["current"];
        let before = &previous["late_packets"]["current"];

        if current["late_packets"]["fresh"] != true
            || after["evidence"]["comparison_key"] != current["evidence"]["comparison_key"]
            || after["epoch"] != before["epoch"]
        {
            continue;
        }

        if let Some(mut event) = counter(
            observation,
            before,
            after,
            "raw",
            EventKind::LatePacketCountIncreased,
            EventKind::LatePacketCounterReset,
            "heartbeat_receiver_flow_late_packets",
        )? {
            event.flow_identity = Some(identity);

            if let Some(records) = after_health.get("source_records") {
                event
                    .raw
                    .insert("source_records".to_owned(), records.clone());
            }

            observation.events.push(event);
        }
    }

    let before = &observation.previous["network_interface_traffic"]["interfaces"];
    let after = &observation.current["network_interface_traffic"]["interfaces"];

    for (index, current) in after.as_array().into_iter().flatten().enumerate() {
        let Some(previous) = before.get(index) else {
            continue;
        };

        for field in ["transmit_error_count", "receive_error_count"] {
            if let Some(mut event) = counter(
                observation,
                previous,
                current,
                field,
                EventKind::InterfaceErrorCounterIncreased,
                EventKind::InterfaceErrorCounterReset,
                "heartbeat_interface_statistics",
            )? {
                event.interface_identity = Some(format!("interface:{}", index + 1));
                observation.events.push(event);
            }
        }
    }

    Ok(())
}

fn counter(
    observation: &Observation<'_>,
    previous: &Value,
    current: &Value,
    field: &str,
    increased: EventKind,
    reset: EventKind,
    source: &str,
) -> Result<Option<crate::monitoring::journal::model::JournalEvent>> {
    let (Some(before), Some(after)) = (previous[field].as_u64(), current[field].as_u64()) else {
        return Ok(None);
    };

    if before == after {
        return Ok(None);
    }

    let resetting = after < before;
    let mut event = observation.event(
        if resetting { reset } else { increased },
        if resetting {
            Severity::Info
        } else {
            Severity::Warning
        },
        json!(before),
        json!(after),
        source,
    )?;
    event.raw.extend([
        ("counter".to_owned(), json!(field)),
        ("previous_measurement".to_owned(), previous.clone()),
        ("current_measurement".to_owned(), current.clone()),
        ("delta".to_owned(), json!(after.checked_sub(before))),
    ]);
    Ok(Some(event))
}

pub(crate) fn flow_map(health: &Value) -> BTreeMap<String, &Value> {
    health["paths"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|flow| {
            if flow["attribution_status"] != "resolved" {
                return None;
            }

            let receiver = flow["audio_receiver_flow_id"]
                .as_u64()
                .filter(|value| *value != 0)?;
            let network = flow["network_interface_index"].as_u64()?;
            let epoch = flow["attribution_epoch"].as_u64()?;
            Some((
                format!("audio-receiver:{receiver}/network:{network}/attribution:{epoch}"),
                flow,
            ))
        })
        .collect()
}
