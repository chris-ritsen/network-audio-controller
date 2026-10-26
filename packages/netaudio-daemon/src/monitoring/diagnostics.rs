use super::multicast::ReceivedPacket;
use anyhow::{Result, bail};
use netaudio_core::heartbeat_clock::{self, ClockObservationRequest, ClockObservations};
use netaudio_core::heartbeat_connection_health::{
    self, ConnectionHealthUpdate, HeartbeatConnectionHealthRecords, Topology, UpdateRequest,
};
use netaudio_core::heartbeat_interface_traffic::parse_heartbeat_interface_traffic_packet;
use netaudio_core::observation::Series;
use serde_json::{Map, Value, json};
use std::time::Duration;
use tokio::time::Instant;

pub const HISTORY_LIMIT: usize = 300;
pub const FRESHNESS: Duration = Duration::from_secs(5);
pub const PRESENCE_LIFETIME: Duration = Duration::from_secs(15);

#[derive(Default)]
pub struct Diagnostics {
    pub identity: Option<String>,
    clock: ClockObservations,
    clock_observed: bool,
    receiver: Option<ConnectionHealthUpdate>,
    traffic: Option<Value>,
    traffic_observed: Option<Instant>,
    pub last_seen: Option<Instant>,
}

impl Diagnostics {
    pub fn observe_presence(&mut self, identity: &str, observed: Instant) -> bool {
        let changed = self
            .identity
            .as_deref()
            .is_some_and(|previous| previous != identity);

        if changed {
            *self = Self::default();
        }

        self.identity = Some(identity.to_owned());
        self.last_seen = Some(observed);
        changed
    }

    pub fn heartbeat(
        &mut self,
        packet: &ReceivedPacket,
        identity: &str,
        origin: Instant,
        topology: Topology,
    ) -> Result<()> {
        self.observe_presence(identity, packet.received_monotonic);
        let timestamp = packet
            .received_at
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let observed_monotonic = packet
            .received_monotonic
            .duration_since(origin)
            .as_secs_f64();
        self.observe_clock(ClockObservationRequest {
            packet: Some(packet.payload.to_vec()),
            conmon_status: None,
            ..clock_request(timestamp.clone(), observed_monotonic)
        })?;

        if let Some(records) =
            heartbeat_connection_health::parse_heartbeat_connection_health_packet(&packet.payload)
        {
            let receiver = self
                .receiver
                .get_or_insert_with(ConnectionHealthUpdate::default);
            heartbeat_connection_health::update_in_place(
                receiver,
                UpdateRequest {
                    records,
                    previous: None,
                    topology,
                    observed_at: timestamp.clone(),
                    observed_monotonic,
                    freshness_seconds: FRESHNESS.as_secs_f64(),
                    history_limit: HISTORY_LIMIT,
                    refresh_only: false,
                    reset: false,
                },
            )
            .map_err(anyhow::Error::msg)?;
        }

        if let Some(record) = parse_heartbeat_interface_traffic_packet(&packet.payload)
            .and_then(|records| records.into_iter().last())
            && self
                .traffic
                .as_ref()
                .is_none_or(|previous| previous["sequence"] != record.sequence)
        {
            self.traffic = Some(json!({
                "device_extended_unique_identifier": identity,
                "sequence": record.sequence,
                "interface_entry_count": record.interface_entry_count,
                "interface_entry_width": record.interface_entry_width,
                "packet_source": packet.source.ip().to_string(),
                "observed_at": timestamp,
                "observation_timestamp_source": "local_receive_time",
                "freshness_seconds": FRESHNESS.as_secs_f64(),
                "fresh": true,
                "transport_source": "heartbeat_0x8000",
                "total_transmit_rate_bits_per_second": record.interfaces.iter().map(|interface| interface.transmit_rate_bits_per_second).sum::<u64>(),
                "total_receive_rate_bits_per_second": record.interfaces.iter().map(|interface| interface.receive_rate_bits_per_second).sum::<u64>(),
                "total_transmit_error_count": record.interfaces.iter().map(|interface| u64::from(interface.transmit_error_count)).sum::<u64>(),
                "total_receive_error_count": record.interfaces.iter().map(|interface| u64::from(interface.receive_error_count)).sum::<u64>(),
                "interfaces": record.interfaces,
            }));
            self.traffic_observed = Some(packet.received_monotonic);
        }

        Ok(())
    }

    pub fn clock_status(
        &mut self,
        status: &Value,
        packet: &ReceivedPacket,
        origin: Instant,
    ) -> Result<()> {
        if status["clock_frequency_offset_parts_per_billion"]
            .as_i64()
            .is_none()
        {
            return Ok(());
        }

        self.observe_clock(ClockObservationRequest {
            conmon_status: Some(status.clone()),
            ..clock_request(
                packet
                    .received_at
                    .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                packet
                    .received_monotonic
                    .duration_since(origin)
                    .as_secs_f64(),
            )
        })
    }

    fn observe_clock(&mut self, request: ClockObservationRequest) -> Result<()> {
        let changed = heartbeat_clock::observe_in_place(&mut self.clock, request)
            .map_err(anyhow::Error::msg)?;
        self.clock_observed |= changed
            || self.clock.heartbeat.current.is_some()
            || self.clock.conmon.current.is_some();
        Ok(())
    }

    pub fn refresh(
        &mut self,
        now: Instant,
        origin: Instant,
        reset: bool,
        warning: Option<bool>,
    ) -> Result<()> {
        let monotonic = now.duration_since(origin).as_secs_f64();
        self.observe_clock(ClockObservationRequest {
            reset,
            warning_enabled: warning,
            ..clock_request(
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                monotonic,
            )
        })?;

        if let Some(receiver) = &mut self.receiver {
            let identity = self
                .identity
                .clone()
                .ok_or_else(|| anyhow::anyhow!("receiver observations have no identity"))?;
            heartbeat_connection_health::update_in_place(
                receiver,
                UpdateRequest {
                    records: HeartbeatConnectionHealthRecords {
                        device_extended_unique_identifier: identity,
                        latency_records: Vec::new(),
                        late_packet_records: Vec::new(),
                        diagnostics: Vec::new(),
                    },
                    previous: None,
                    topology: Topology::default(),
                    observed_at: String::new(),
                    observed_monotonic: monotonic,
                    freshness_seconds: FRESHNESS.as_secs_f64(),
                    history_limit: HISTORY_LIMIT,
                    refresh_only: false,
                    reset,
                },
            )
            .map_err(anyhow::Error::msg)?;
        }

        if self
            .traffic_observed
            .is_some_and(|observed| now >= observed + FRESHNESS)
            && let Some(traffic) = &mut self.traffic
        {
            traffic["fresh"] = json!(false);
        }

        Ok(())
    }

    pub fn next_expiry(&self, origin: Instant, now: Instant) -> Option<Instant> {
        let mut deadlines = Vec::new();
        let series = [&self.clock.heartbeat, &self.clock.conmon]
            .into_iter()
            .chain(
                self.receiver
                    .iter()
                    .flat_map(|receiver| receiver.paths.iter())
                    .flat_map(|path| [&path.latency, &path.late_packets]),
            );

        for series in series.filter(|series| series.fresh) {
            if let Some(current) = &series.current {
                deadlines
                    .push(origin + Duration::from_secs_f64(current.observed_monotonic) + FRESHNESS);
            }
        }

        if let Some(observed) = self.traffic_observed
            && self
                .traffic
                .as_ref()
                .is_some_and(|traffic| traffic["fresh"] == true)
        {
            deadlines.push(observed + FRESHNESS);
        }

        if self.identity.is_some()
            && let Some(observed) = self.last_seen
        {
            let deadline = observed + PRESENCE_LIFETIME;

            if deadline > now {
                deadlines.push(deadline);
            }
        }

        deadlines.into_iter().min()
    }

    pub fn fields(&self) -> Result<Map<String, Value>> {
        let mut fields = Map::new();
        fields.insert("clock_observations".to_owned(), self.clock_snapshot(false)?);
        fields.insert(
            "receiver_flow_connection_health".to_owned(),
            self.receiver_snapshot(false)?,
        );
        fields.insert(
            "network_interface_traffic".to_owned(),
            self.traffic.clone().unwrap_or(Value::Null),
        );

        if self.clock_observed || self.identity.is_some() {
            let current = [&self.clock.heartbeat, &self.clock.conmon]
                .into_iter()
                .filter_map(|series| series.current.as_ref())
                .max_by(|left, right| left.observed_monotonic.total_cmp(&right.observed_monotonic));
            fields.insert(
                "clock_frequency_offset_parts_per_billion".to_owned(),
                current.map_or(Value::Null, |current| json!(current.raw)),
            );
        }

        Ok(fields)
    }

    pub fn clock_snapshot(&self, history: bool) -> Result<Value> {
        if !self.clock_observed {
            return Ok(Value::Null);
        }

        if history {
            return Ok(serde_json::to_value(&self.clock)?);
        }

        Ok(serde_json::to_value(ClockObservations {
            warning_enabled: self.clock.warning_enabled,
            variation: self.clock.variation.clone(),
            heartbeat: self.clock.heartbeat.summary(),
            conmon: self.clock.conmon.summary(),
            diagnostics: self.clock.diagnostics.clone(),
        })?)
    }

    pub fn receiver_snapshot(&self, history: bool) -> Result<Value> {
        match &self.receiver {
            Some(receiver) if history => Ok(serde_json::to_value(receiver)?),
            Some(receiver) => Ok(serde_json::to_value(receiver.summary())?),
            None => Ok(Value::Null),
        }
    }
}

fn clock_request(observed_at: String, observed_monotonic: f64) -> ClockObservationRequest {
    ClockObservationRequest {
        warning_enabled: None,
        previous: None,
        packet: None,
        conmon_status: None,
        observed_at,
        observed_monotonic,
        freshness_seconds: FRESHNESS.as_secs_f64(),
        history_limit: HISTORY_LIMIT,
        reset: false,
    }
}

pub fn points(snapshot: &mut Value) -> Result<()> {
    if let Some(paths) = snapshot
        .get_mut("receiver")
        .and_then(|receiver| receiver.get_mut("paths"))
        .and_then(Value::as_array_mut)
    {
        for path in paths {
            if let Some(late_packets) = path["late_packets"].as_object_mut() {
                late_packets.remove("history");
            }

            series_points(&mut path["latency"])?;
        }
    }

    if let Some(clock) = snapshot.get_mut("clock").and_then(Value::as_object_mut) {
        for name in ["heartbeat", "conmon"] {
            if let Some(series) = clock.get_mut(name) {
                series_points(series)?;
            }
        }
    }

    Ok(())
}

fn series_points(value: &mut Value) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }

    let series: Series = serde_json::from_value(value.clone())?;
    let points: Vec<_> = series
        .history
        .iter()
        .filter(|sample| sample.display_epoch == series.display_epoch)
        .filter_map(|sample| {
            sample.value.map(|value| {
                json!([
                    (sample.observed_monotonic * 10.0).round_ties_even() / 10.0,
                    value,
                ])
            })
        })
        .collect();
    let Some(fields) = value.as_object_mut() else {
        bail!("diagnostic series is not an object");
    };
    fields.remove("history");

    if points
        .iter()
        .any(|point| point[1].as_f64().is_some_and(|value| value != 0.0))
    {
        fields.insert("points".to_owned(), json!(points));
    }

    Ok(())
}
