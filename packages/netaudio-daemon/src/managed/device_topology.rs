use super::{
    control::FreshTarget,
    controller::Exchange,
    device_session::DeviceSession,
    inventory::freshness,
    registry::{Registry, Source},
    status::observations::Received,
};
use crate::{
    devices::{operations::ControlError, sample_rate::Snapshot},
    monitoring::{requests::StatusQuery, status::StatusKind},
};
use netaudio_core::{inventory::InventoryState, parser::ChannelCount};
use serde_json::{Map, Value, json};
use std::sync::Arc;

impl Registry {
    pub(super) async fn controller_topology(
        &self,
        source: &Arc<Source>,
        fresh: &FreshTarget,
        session: &mut DeviceSession,
        rate: u32,
    ) -> Result<Value, ControlError> {
        let result = self
            .observe_controller_topology(source, fresh, session, rate)
            .await;
        if let Err(error) = &result {
            self.invalidate_controller_status(
                source,
                &fresh.target,
                StatusKind::Topology,
                &error.to_string(),
            )?;
        }
        result
    }

    async fn observe_controller_topology(
        &self,
        source: &Arc<Source>,
        fresh: &FreshTarget,
        session: &mut DeviceSession,
        rate: u32,
    ) -> Result<Value, ControlError> {
        let protocol = netaudio_core::protocol::arc_protocol(None, true)
            .map_err(|error| unavailable(error.to_string()))?
            .ok_or_else(|| unavailable("Managed ARC protocol is unavailable".into()))?
            .protocol_id;
        let first = query(session, json!({"command": "channel_count"})).await?;
        let counts = counts(&first)?;
        let receivers = if counts.rx_count == 0 {
            json!({"records": [], "queried": false, "reason": "zero_channel_count"})
        } else {
            inventory(session, "rx_channels", protocol).await?
        };
        let transmitters = if counts.tx_count == 0 {
            json!({"records": [], "queried": false, "reason": "zero_channel_count"})
        } else {
            inventory(session, "tx_channels", protocol).await?
        };
        let flows = if counts.tx_count == 0 {
            json!({"flows": [], "queried": false, "reason": "zero_channel_count"})
        } else {
            inventory(session, "tx_flows", protocol).await?
        };
        let last = query(session, json!({"command": "channel_count"})).await?;
        let verified = self::counts(&last)?;
        if !same_topology(&counts, &verified) {
            return Err(unavailable(
                "Channel counts changed during managed topology observation".into(),
            ));
        }
        self.controller_status(source, fresh, session, StatusQuery::SampleRate)
            .await?;
        let record = self.status_record(fresh)?;
        if record["sample_rate_hz"] != rate {
            return Err(unavailable(
                "Sample rate changed during managed topology observation".into(),
            ));
        }
        let receiver_records = records(&receivers, "records")?;
        let transmitter_records = records(&transmitters, "records")?;
        for channel in receiver_records.iter().chain(transmitter_records) {
            if channel["media_type"] == "audio"
                && let Some(observed) = channel["sample_rate"].as_u64()
                && observed != u64::from(rate)
            {
                return Err(unavailable(
                    "Channel inventory disagrees with the observed sample rate".into(),
                ));
            }
        }
        let subscriptions = receiver_records
            .iter()
            .filter(|channel| {
                ["source_device_name", "source_channel_name"].iter().any(|field| {
                    channel[field].as_str().is_some_and(|value| !value.is_empty())
                })
            })
            .map(|channel| json!({
                "receiver_channel_number": channel["channel_number"],
                "receiver_channel_name": channel["local_channel_name"].as_str().unwrap_or_default(),
                "transmitter_channel_name": channel["source_channel_name"].as_str().unwrap_or_default(),
                "transmitter_device_name": channel["source_device_name"].as_str().unwrap_or_default(),
            }))
            .collect();
        let snapshot = Snapshot {
            capacity: json!({
                "sample_rate_hertz": rate,
                "receive_channel_count": counts.rx_count,
                "transmit_channel_count": counts.tx_count,
            }),
            receiver_numbers: numbers(receiver_records)?,
            transmitter_numbers: numbers(transmitter_records)?,
            subscriptions,
            flows: records(&flows, "flows")?,
            protocol,
        }
        .resolve(&record, rate)?;
        let fields = Map::from_iter([(
            "managed_topology".into(),
            json!({
                "snapshot": snapshot,
                "channel_counts": counts,
                "receivers": receivers, "transmitters": transmitters,
                "transmitter_flows": flows, "protocol_id": protocol,
                "acquisition_started_at": first.received_at,
                "acquisition_completed_at": last.received_at,
            }),
        )]);
        let observation = Received::inventory(
            &first,
            &last,
            StatusKind::Topology,
            fields,
            freshness(&source.configuration),
        )
        .map_err(|error| unavailable(error.to_string()))?;
        self.accept_controller_status(source, &fresh.target, observation)?;
        Ok(snapshot)
    }
}

pub(super) async fn query(
    session: &mut DeviceSession,
    command: Value,
) -> Result<Exchange, ControlError> {
    session
        .command(command)
        .await
        .map_err(|error| unavailable(error.to_string()))
}

pub(super) fn counts(exchange: &Exchange) -> Result<ChannelCount, ControlError> {
    exchange
        .packet
        .as_deref()
        .and_then(netaudio_core::parser::parse_channel_count)
        .ok_or_else(|| {
            unavailable("Managed channel count response is unavailable or malformed".into())
        })
}

pub(super) async fn inventory(
    session: &mut DeviceSession,
    kind: &str,
    protocol: u16,
) -> Result<Value, ControlError> {
    let mut inventory = netaudio_core::inventory::Inventory::new(kind, protocol, 256)
        .map_err(|error| unavailable(error.into()))?;
    loop {
        let command = match inventory.state() {
            InventoryState::Pending { next_command, .. }
            | InventoryState::Partial { next_command, .. } => next_command,
            InventoryState::Complete { inventory, .. } => return Ok(Value::Object(inventory)),
        };
        let exchange = query(session, Value::Object(command)).await?;
        let packet = exchange
            .packet
            .as_deref()
            .ok_or_else(|| unavailable(format!("Managed {kind} returned no device packet")))?;
        inventory
            .accept(packet)
            .map_err(|error| unavailable(format!("Managed {kind}: {error}")))?;
    }
}

fn records<'a>(inventory: &'a Value, field: &str) -> Result<&'a [Value], ControlError> {
    inventory[field]
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| unavailable(format!("Managed inventory has no {field}")))
}

fn numbers(records: &[Value]) -> Result<Vec<u16>, ControlError> {
    records
        .iter()
        .map(|record| {
            record["channel_number"]
                .as_u64()
                .and_then(|number| u16::try_from(number).ok())
                .ok_or_else(|| {
                    unavailable("Managed inventory has an invalid channel number".into())
                })
        })
        .collect()
}

fn unavailable(error: String) -> ControlError {
    ControlError::ObservationUnavailable(error)
}

pub(super) fn same_topology(first: &ChannelCount, last: &ChannelCount) -> bool {
    first.rx_count == last.rx_count
        && first.tx_count == last.tx_count
        && first.transmit_flow_authoring_capability_word
            == last.transmit_flow_authoring_capability_word
        && first.maximum_transmit_flow_channel_slots == last.maximum_transmit_flow_channel_slots
        && first.maximum_receive_flow_channel_slots == last.maximum_receive_flow_channel_slots
        && first.maximum_transmit_flows == last.maximum_transmit_flows
        && first.maximum_receive_flows == last.maximum_receive_flows
}
