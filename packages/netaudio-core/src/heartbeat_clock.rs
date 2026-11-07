use serde::{Deserialize, Serialize};

use crate::bytes::{read_u16, read_u32};
use crate::heartbeat::parse_heartbeat_records;

const CLOCK_FREQUENCY_OFFSET_RECORD_TYPE: u16 = 0x8001;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatClockFrequencyOffsetRecord {
    pub raw_record: Vec<u8>,
    pub record_length: u16,
    pub extension_length: u16,
    pub payload_length: u16,
    pub sequence: u16,
    pub unknown_word_at_offset_10: u16,
    pub clock_frequency_offset_parts_per_billion: i32,
    pub trailing_payload: Vec<u8>,
}

fn parse_clock_frequency_offset_record(
    record: &[u8],
) -> Option<HeartbeatClockFrequencyOffsetRecord> {
    let payload = crate::heartbeat::monitoring_payload(record, 4)?;
    let record_length = read_u16(record, 0)?;
    let extension_length = read_u16(record, 4)?;
    let payload_length = read_u16(record, 6)?;
    Some(HeartbeatClockFrequencyOffsetRecord {
        raw_record: record.to_vec(),
        record_length,
        extension_length,
        payload_length,
        sequence: read_u16(record, 8)?,
        unknown_word_at_offset_10: if extension_length >= 4 {
            read_u16(record, 10)?
        } else {
            0
        },
        clock_frequency_offset_parts_per_billion: read_u32(record, payload)? as i32,
        trailing_payload: record.get(payload + 4..)?.to_vec(),
    })
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockObservations {
    #[serde(default)]
    pub warning_enabled: bool,
    #[serde(default)]
    pub variation: std::collections::BTreeMap<String, ClockVariation>,
    pub heartbeat: crate::observation::Series,
    pub conmon: crate::observation::Series,
    pub diagnostics: Vec<crate::observation::Diagnostic>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockVariation {
    pub observable: bool,
    pub active: bool,
    pub recovered: bool,
    pub window_size: usize,
    pub deviation_ppb: Option<f64>,
    pub threshold_ppb: u32,
    pub recovery_since: Option<f64>,
    pub last_epoch: Option<u64>,
    pub last_observed_monotonic: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockObservationRequest {
    pub warning_enabled: Option<bool>,
    pub previous: Option<ClockObservations>,
    pub packet: Option<Vec<u8>>,
    pub conmon_status: Option<serde_json::Value>,
    pub observed_at: String,
    pub observed_monotonic: f64,
    pub freshness_seconds: f64,
    pub history_limit: usize,
    #[serde(default)]
    pub reset: bool,
}

pub fn observe(request: ClockObservationRequest) -> Result<Option<ClockObservations>, String> {
    let mut request = request;
    let mut state = request.previous.take().unwrap_or_default();
    let changed = observe_in_place(&mut state, request)?;
    Ok(changed.then_some(state))
}

pub fn observe_in_place(
    state: &mut ClockObservations,
    request: ClockObservationRequest,
) -> Result<bool, String> {
    use crate::observation::{Diagnostic, Observation};
    use serde_json::json;
    crate::observation::validate_limits(
        request.observed_monotonic,
        request.freshness_seconds,
        request.history_limit,
    )?;
    if request.previous.is_some() {
        return Err("A retained clock tracker cannot replace its history".into());
    }
    // Validate all fallible inputs before changing retained state.
    if let Some(packet) = &request.packet {
        parse_heartbeat_records(packet).ok_or("Malformed heartbeat envelope")?;
    }
    if let Some(status) = &request.conmon_status {
        status["clock_frequency_offset_parts_per_billion"]
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .ok_or("Missing clock offset")?;
    }
    let mut changed = false;
    if let Some(enabled) = request.warning_enabled {
        changed |= state.warning_enabled != enabled;
        state.warning_enabled = enabled;
    }
    let diagnostics = state.diagnostics.len();
    if let Some(packet) = request.packet {
        let records = parse_heartbeat_records(&packet).ok_or("Malformed heartbeat envelope")?;
        for record in records
            .into_iter()
            .filter(|r| r.record_type == CLOCK_FREQUENCY_OFFSET_RECORD_TYPE)
        {
            let Some(parsed) = parse_clock_frequency_offset_record(record.bytes) else {
                state.diagnostics.push(Diagnostic {
                    kind: "malformed_record".into(),
                    evidence: json!({"raw_record": record.bytes}),
                });
                continue;
            };
            let mut sample = Observation::new(
                i64::from(parsed.clock_frequency_offset_parts_per_billion),
                None,
                Some(parsed.sequence),
                "heartbeat_clock",
                &request.observed_at,
                request.observed_monotonic,
                parsed.raw_record,
            );
            sample.clock_state_evidence = state
                .conmon
                .current
                .as_ref()
                .and_then(|s| s.clock_state_evidence.clone());
            changed |= state.heartbeat.accept(
                sample,
                request.freshness_seconds,
                request.history_limit,
                &mut state.diagnostics,
            );
        }
    }
    if let Some(status) = request.conmon_status {
        let raw = status["clock_frequency_offset_parts_per_billion"]
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .ok_or("Missing clock offset")?;
        let mut sample = Observation::new(
            i64::from(raw),
            None,
            None,
            "conmon_clock",
            &request.observed_at,
            request.observed_monotonic,
            vec![],
        );
        #[derive(Serialize)]
        struct ClockEvidence<'a> {
            status: &'a serde_json::Value,
            observed_at: &'a str,
        }
        sample.clock_state_evidence = Some(
            serde_json::value::to_raw_value(&ClockEvidence {
                status: &status,
                observed_at: &request.observed_at,
            })
            .map_err(|error| error.to_string())?
            .into(),
        );
        changed |= state.conmon.accept(
            sample,
            request.freshness_seconds,
            request.history_limit,
            &mut state.diagnostics,
        );
    }
    for series in [&mut state.heartbeat, &mut state.conmon] {
        changed |= series.refresh(request.observed_monotonic, request.freshness_seconds);
        if request.reset {
            series.reset();
            changed = true;
        }
    }
    for (name, series) in [("heartbeat", &state.heartbeat), ("conmon", &state.conmon)] {
        let policy = state.variation.entry(name.into()).or_default();
        policy.threshold_ppb = 10000;
        let epoch = series.current.as_ref().map(|s| s.epoch);
        let window: Vec<_> = series
            .history
            .iter()
            .rev()
            .take_while(|s| Some(s.epoch) == epoch && s.display_epoch == series.display_epoch)
            .take(20)
            .collect();
        let observable = series.fresh && window.len() >= 11;
        changed |= policy.observable != observable;
        policy.observable = observable;
        policy.window_size = window.len();
        policy.deviation_ppb = if observable {
            let mean = window.iter().map(|s| s.raw as f64).sum::<f64>() / window.len() as f64;
            Some(
                (window
                    .iter()
                    .map(|s| (s.raw as f64 - mean).powi(2))
                    .sum::<f64>()
                    / window.len() as f64)
                    .sqrt(),
            )
        } else {
            None
        };
        if !observable || policy.last_epoch != epoch {
            policy.recovery_since = None;
            policy.recovered = false;
        }
        policy.last_epoch = epoch;
        if state.warning_enabled && observable {
            let now = series.current.as_ref().unwrap().observed_monotonic;
            if policy.deviation_ppb.unwrap() > f64::from(policy.threshold_ppb) {
                policy.active = true;
                policy.recovery_since = None;
                policy.recovered = false;
            } else if policy.last_observed_monotonic != Some(now) {
                let start = policy.recovery_since.get_or_insert(now);
                if now - *start >= 5.0 {
                    policy.active = false;
                    policy.recovered = true;
                }
            }
            policy.last_observed_monotonic = Some(now);
        }
    }
    changed |= state.diagnostics.len() != diagnostics;
    if state.diagnostics.len() > request.history_limit {
        state
            .diagnostics
            .drain(..state.diagnostics.len() - request.history_limit);
    }
    Ok(changed)
}

pub fn parse_heartbeat_clock_frequency_offset_packet(
    data: &[u8],
) -> Option<Vec<HeartbeatClockFrequencyOffsetRecord>> {
    let records = parse_heartbeat_records(data)?;
    Some(
        records
            .into_iter()
            .filter(|record| record.record_type == CLOCK_FREQUENCY_OFFSET_RECORD_TYPE)
            .filter_map(|record| parse_clock_frequency_offset_record(record.bytes))
            .collect(),
    )
}
