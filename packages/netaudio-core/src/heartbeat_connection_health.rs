use crate::bytes::{read_u16, read_u32};
use crate::heartbeat::{
    parse_heartbeat_device_extended_unique_identifier, parse_heartbeat_records,
};
use crate::observation::{Diagnostic, Observation, Series};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HeartbeatFlowLatencyEntry {
    pub telemetry_index: u16,
    pub latency_sample_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HeartbeatFlowLatencyRecord {
    pub raw_record: Vec<u8>,
    pub sequence: u16,
    pub sample_rate_hertz: u32,
    pub entries: Vec<HeartbeatFlowLatencyEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HeartbeatLatePacketEntry {
    pub telemetry_index: u16,
    pub late_packet_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HeartbeatLatePacketRecord {
    pub raw_record: Vec<u8>,
    pub sequence: u16,
    pub entries: Vec<HeartbeatLatePacketEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HeartbeatConnectionHealthRecords {
    pub device_extended_unique_identifier: String,
    pub latency_records: Vec<HeartbeatFlowLatencyRecord>,
    pub late_packet_records: Vec<HeartbeatLatePacketRecord>,
    pub diagnostics: Vec<Diagnostic>,
}

fn geometry(record: &[u8], fixed: usize) -> Option<(usize, u16, u16, usize)> {
    let payload = crate::heartbeat::monitoring_payload(record, fixed)?;
    let count = read_u16(record, payload)?;
    let first = read_u16(record, payload + 2)?;
    let offset = usize::from(read_u16(record, payload + 4)?);
    let end = offset.checked_add(usize::from(count).checked_mul(4)?)?;
    if count == 0
        || offset < payload + fixed
        || end > record.len()
        || u32::from(first) + u32::from(count) > 65536
    {
        return None;
    }
    Some((payload, count, first, offset))
}

pub fn parse_heartbeat_connection_health_packet(
    data: &[u8],
) -> Option<HeartbeatConnectionHealthRecords> {
    let mut result = HeartbeatConnectionHealthRecords {
        device_extended_unique_identifier: parse_heartbeat_device_extended_unique_identifier(data)?,
        latency_records: vec![],
        late_packet_records: vec![],
        diagnostics: vec![],
    };
    for record in parse_heartbeat_records(data)? {
        let fixed = match record.record_type {
            0x8003 => 12,
            0x8004 => 8,
            _ => continue,
        };
        let Some((payload, count, first, offset)) = geometry(record.bytes, fixed) else {
            result.diagnostics.push(Diagnostic {
                kind: "malformed_record".into(),
                evidence: json!({"record_type": record.record_type, "raw_record": record.bytes}),
            });
            continue;
        };
        let sequence = read_u16(record.bytes, 8)?;
        if record.record_type == 0x8003 {
            result.latency_records.push(HeartbeatFlowLatencyRecord {
                raw_record: record.bytes.to_vec(),
                sequence,
                sample_rate_hertz: read_u32(record.bytes, payload + 8)?,
                entries: (0..count)
                    .map(|i| {
                        Some(HeartbeatFlowLatencyEntry {
                            telemetry_index: first.checked_add(i)?,
                            latency_sample_count: read_u32(
                                record.bytes,
                                offset + usize::from(i) * 4,
                            )?,
                        })
                    })
                    .collect::<Option<_>>()?,
            });
        } else {
            result.late_packet_records.push(HeartbeatLatePacketRecord {
                raw_record: record.bytes.to_vec(),
                sequence,
                entries: (0..count)
                    .map(|i| {
                        Some(HeartbeatLatePacketEntry {
                            telemetry_index: first.checked_add(i)?,
                            late_packet_count: read_u32(record.bytes, offset + usize::from(i) * 4)?,
                        })
                    })
                    .collect::<Option<_>>()?,
            });
        }
    }
    (!result.latency_records.is_empty()
        || !result.late_packet_records.is_empty()
        || !result.diagnostics.is_empty())
    .then_some(result)
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Topology {
    pub inventory_family: Option<String>,
    pub capacity: Option<Value>,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub flows: Vec<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReceiverPath {
    pub attribution_epoch: u64,
    pub telemetry_index: u16,
    pub network_interface_index: Option<u16>,
    pub audio_receiver_flow_id: Option<u16>,
    pub media_type: Option<String>,
    pub global_flow_id: Option<u16>,
    pub attribution_status: String,
    pub attribution_reason: Option<String>,
    pub evidence: Value,
    pub latency: Series,
    pub late_packets: Series,
}

impl ReceiverPath {
    fn summary(&self) -> Self {
        Self {
            attribution_epoch: self.attribution_epoch,
            telemetry_index: self.telemetry_index,
            network_interface_index: self.network_interface_index,
            audio_receiver_flow_id: self.audio_receiver_flow_id,
            media_type: self.media_type.clone(),
            global_flow_id: self.global_flow_id,
            attribution_status: self.attribution_status.clone(),
            attribution_reason: self.attribution_reason.clone(),
            evidence: self.evidence.clone(),
            latency: self.latency.summary(),
            late_packets: self.late_packets.summary(),
        }
    }

    fn new(index: u16) -> Self {
        Self {
            attribution_epoch: 0,
            telemetry_index: index,
            network_interface_index: None,
            audio_receiver_flow_id: None,
            media_type: None,
            global_flow_id: None,
            attribution_status: "unresolved".into(),
            attribution_reason: None,
            evidence: Value::Null,
            latency: Series::default(),
            late_packets: Series::default(),
        }
    }
    fn attribute(&mut self, topology: &Topology) {
        let capacity = topology.capacity.as_ref().unwrap_or(&Value::Null);
        let f = capacity["base_receive_flow_capacity"].as_u64().unwrap_or(0);
        let n = capacity["network_interface_count"].as_u64().unwrap_or(0);
        self.network_interface_index = None;
        self.audio_receiver_flow_id = None;
        self.media_type = None;
        self.global_flow_id = None;
        self.attribution_status = "unresolved".into();
        self.attribution_reason = Some("Audio capacity or network topology is unavailable".into());
        self.evidence = json!({"capacity": capacity, "inventory_complete": topology.complete});
        if f == 0
            || n == 0
            || f > 65535
            || n > 65535
            || capacity["resource_extension_offset"].as_u64() != Some(0)
            || !topology.complete
            || topology.flows.iter().any(|flow| {
                flow["media_type_code"].as_u64() != Some(3)
                    && topology.inventory_family.as_deref() != Some("legacy")
            })
        {
            return;
        }
        let index = u64::from(self.telemetry_index);
        if index >= f * n {
            self.attribution_reason = Some("Telemetry index exceeds advertised topology".into());
            return;
        }
        let local_id = (index % f + 1) as u16;
        self.network_interface_index = Some((index / f) as u16);
        self.audio_receiver_flow_id = Some(local_id);
        self.media_type = Some("audio".into());
        let matches: Vec<_> = topology
            .flows
            .iter()
            .filter(|flow| {
                let id = if topology.inventory_family.as_deref() == Some("legacy") {
                    &flow["flow_number"]
                } else {
                    &flow["media_local_flow_id"]
                };
                id.as_u64() == Some(u64::from(local_id))
            })
            .collect();
        self.attribution_reason = Some("Audio receiver flow is not uniquely resolved".into());
        if matches.len() == 1 {
            self.global_flow_id = matches[0]["global_flow_id"]
                .as_u64()
                .or_else(|| matches[0]["flow_number"].as_u64())
                .and_then(|v| u16::try_from(v).ok());
            self.evidence["flow"] = matches[0].clone();
            self.evidence["source"] = matches[0]["source"].clone();
            self.evidence["configured_latency_nanoseconds"] =
                matches[0]["latency_nanoseconds"].clone();
            self.attribution_status = "resolved".into();
            self.attribution_reason = None;
        }
        self.evidence["network_interface_index"] = json!(self.network_interface_index);
        self.evidence["audio_receiver_flow_id"] = json!(local_id);
        let flow = self.evidence["flow"].clone();
        self.evidence["comparison_key"] = json!({"capacity": f, "networks": n,
            "network": self.network_interface_index, "local_id": local_id,
            "global_id": self.global_flow_id, "source": flow["source"], "flow_name": flow["flow_name"],
            "endpoint": flow["endpoint_descriptor_hexadecimal"],
            "mapping": flow["receiver_mapping_descriptor_hexadecimal"],
            "budget": flow["latency_nanoseconds"]});
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConnectionHealthUpdate {
    pub device_extended_unique_identifier: String,
    pub complete: bool,
    pub fresh: bool,
    pub retention_limit: usize,
    pub paths: Vec<ReceiverPath>,
    pub diagnostics: Vec<Diagnostic>,
}

impl ConnectionHealthUpdate {
    pub fn summary(&self) -> Self {
        Self {
            device_extended_unique_identifier: self.device_extended_unique_identifier.clone(),
            complete: self.complete,
            fresh: self.fresh,
            retention_limit: self.retention_limit,
            paths: self.paths.iter().map(ReceiverPath::summary).collect(),
            diagnostics: self.diagnostics.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct UpdateRequest {
    pub records: HeartbeatConnectionHealthRecords,
    pub previous: Option<ConnectionHealthUpdate>,
    pub topology: Topology,
    pub observed_at: String,
    pub observed_monotonic: f64,
    pub freshness_seconds: f64,
    pub history_limit: usize,
    #[serde(default)]
    pub refresh_only: bool,
    #[serde(default)]
    pub reset: bool,
}

pub fn plan_update(request: UpdateRequest) -> Result<Option<ConnectionHealthUpdate>, String> {
    let mut request = request;
    let mut state = request.previous.take().unwrap_or_default();
    let changed = update_in_place(&mut state, request)?;
    Ok(changed.then_some(state))
}

pub fn update_in_place(
    state: &mut ConnectionHealthUpdate,
    request: UpdateRequest,
) -> Result<bool, String> {
    crate::observation::validate_limits(
        request.observed_monotonic,
        request.freshness_seconds,
        request.history_limit,
    )?;
    let identity = &request.records.device_extended_unique_identifier;
    if identity.len() != 16
        || !identity.bytes().all(|c| c.is_ascii_hexdigit())
        || identity.bytes().all(|c| c == b'0')
    {
        return Err("Invalid monitoring identity".into());
    }
    if request.previous.is_some() {
        return Err("A retained receiver tracker cannot replace its history".into());
    }
    if !state.device_extended_unique_identifier.is_empty()
        && state.device_extended_unique_identifier != *identity
    {
        return Err("Monitoring identity changed".into());
    }
    state.device_extended_unique_identifier = identity.clone();
    state.retention_limit = request.history_limit;
    let mut changed = false;
    let diagnostic_count = state.diagnostics.len();
    let mut observations = Vec::new();
    for record in request.records.latency_records {
        let raw_record = Arc::new(record.raw_record);
        for entry in record.entries {
            observations.push((
                entry.telemetry_index,
                false,
                Observation::new(
                    i64::from(entry.latency_sample_count),
                    Some(record.sample_rate_hertz),
                    Some(record.sequence),
                    "receiver_latency",
                    &request.observed_at,
                    request.observed_monotonic,
                    raw_record.clone(),
                ),
            ));
        }
    }
    for record in request.records.late_packet_records {
        let raw_record = Arc::new(record.raw_record);
        for entry in record.entries {
            observations.push((
                entry.telemetry_index,
                true,
                Observation::new(
                    i64::from(entry.late_packet_count),
                    None,
                    Some(record.sequence),
                    "late_packets",
                    &request.observed_at,
                    request.observed_monotonic,
                    raw_record.clone(),
                ),
            ));
        }
    }
    for diagnostic in request.records.diagnostics {
        if state.diagnostics.last() != Some(&diagnostic) {
            state.diagnostics.push(diagnostic);
        }
    }
    for (index, late, mut observation) in observations {
        let position = state
            .paths
            .iter()
            .position(|p| p.telemetry_index == index)
            .unwrap_or_else(|| {
                state.paths.push(ReceiverPath::new(index));
                state.paths.len() - 1
            });
        let path = &mut state.paths[position];
        let mut attributed = ReceiverPath::new(index);
        attributed.attribution_epoch = path.attribution_epoch;
        attributed.attribute(&request.topology);
        if !path.evidence.is_null()
            && path.evidence["comparison_key"] != attributed.evidence["comparison_key"]
        {
            attributed.attribution_epoch += 1;
        }
        observation.evidence = attributed.evidence.clone();
        let series = if late {
            &mut path.late_packets
        } else {
            &mut path.latency
        };
        let accepted = series.accept(
            observation,
            request.freshness_seconds,
            request.history_limit,
            &mut state.diagnostics,
        );
        if accepted {
            attributed.latency = std::mem::take(&mut path.latency);
            attributed.late_packets = std::mem::take(&mut path.late_packets);
            *path = attributed;
        }
        changed |= accepted;
    }
    for path in &mut state.paths {
        for series in [&mut path.latency, &mut path.late_packets] {
            changed |= series.refresh(request.observed_monotonic, request.freshness_seconds);
            if request.reset {
                series.reset();
                changed = true;
            }
        }
    }
    state.fresh = state
        .paths
        .iter()
        .any(|p| p.latency.fresh || p.late_packets.fresh);
    state.paths.sort_by_key(|p| p.telemetry_index);
    changed |= state.diagnostics.len() != diagnostic_count;
    if state.diagnostics.len() > request.history_limit {
        state
            .diagnostics
            .drain(..state.diagnostics.len() - request.history_limit);
    }
    Ok(changed || request.refresh_only)
}
