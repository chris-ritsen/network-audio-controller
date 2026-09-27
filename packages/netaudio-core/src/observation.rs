use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Diagnostic {
    pub kind: String,
    pub evidence: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Observation {
    pub raw: i64,
    pub sample_rate_hertz: Option<u32>,
    pub sequence: Option<u16>,
    pub sequence_gap: u16,
    pub source: String,
    pub record_type: u16,
    pub value_unit: String,
    pub observed_at: String,
    pub observed_monotonic: f64,
    pub timestamp_provenance: String,
    pub epoch: u64,
    pub display_epoch: u64,
    pub evidence: Value,
    pub clock_state_evidence: Option<Value>,
    pub raw_record: Vec<u8>,
    pub value: Option<f64>,
    pub latency_microseconds: Option<u64>,
}

impl Observation {
    pub fn new(
        raw: i64,
        rate: Option<u32>,
        sequence: Option<u16>,
        source: &str,
        at: &str,
        monotonic: f64,
        bytes: Vec<u8>,
    ) -> Self {
        let value = match source {
            "receiver_latency" => rate
                .filter(|r| *r > 0)
                .map(|r| raw as f64 * 1_000_000_000.0 / f64::from(r)),
            "heartbeat_clock" | "conmon_clock" => Some(raw as f64 / 1000.0),
            _ => Some(raw as f64),
        };
        Self {
            raw,
            sample_rate_hertz: rate,
            sequence,
            sequence_gap: 0,
            source: source.into(),
            observed_at: at.into(),
            record_type: match source {
                "receiver_latency" => 0x8003,
                "late_packets" => 0x8004,
                "heartbeat_clock" => 0x8001,
                _ => 0x0020,
            },
            value_unit: match source {
                "receiver_latency" => "nanoseconds",
                "late_packets" => "count",
                _ => "ppm",
            }
            .into(),
            observed_monotonic: monotonic,
            timestamp_provenance: "local_receive_time".into(),
            epoch: 0,
            display_epoch: 0,
            evidence: Value::Null,
            clock_state_evidence: None,
            raw_record: bytes,
            value,
            latency_microseconds: rate.filter(|r| *r > 0).and_then(|r| {
                u64::try_from(raw)
                    .ok()
                    .map(|s| s * 1_000_000 / u64::from(r))
            }),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Series {
    pub fresh: bool,
    pub history: Vec<Observation>,
    pub current: Option<Observation>,
    pub delta: Option<i64>,
    pub baseline: Option<i64>,
    pub increase_since_baseline: Option<i64>,
    pub display_epoch: u64,
    pub statistics: Value,
    pub histogram: Value,
}

pub fn validate_limits(now: f64, freshness: f64, limit: usize) -> Result<(), String> {
    if !now.is_finite()
        || !freshness.is_finite()
        || freshness <= 0.0
        || !(1..=10000).contains(&limit)
    {
        return Err("Invalid monitoring time or retention limit".into());
    }
    Ok(())
}

impl Series {
    /// Current state without walking or cloning retained observations.
    pub fn summary(&self) -> Self {
        Self {
            fresh: self.fresh,
            history: Vec::new(),
            current: self.current.clone(),
            delta: self.delta,
            baseline: self.baseline,
            increase_since_baseline: self.increase_since_baseline,
            display_epoch: self.display_epoch,
            statistics: self.statistics.clone(),
            histogram: self.histogram.clone(),
        }
    }

    pub fn accept(
        &mut self,
        mut sample: Observation,
        freshness: f64,
        limit: usize,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> bool {
        sample.display_epoch = self.display_epoch;
        let mut delta = None;
        if let Some(previous) = &self.current {
            let elapsed = sample.observed_monotonic - previous.observed_monotonic;
            if elapsed < 0.0 {
                return false;
            }
            let evidence_key = |e: &Value| {
                e.get("comparison_key")
                    .cloned()
                    .unwrap_or_else(|| e.clone())
            };
            let continuity = elapsed < freshness
                && evidence_key(&previous.evidence) == evidence_key(&sample.evidence)
                && previous.source == sample.source;
            sample.epoch = previous.epoch + u64::from(!continuity);
            if elapsed < freshness {
                if let (Some(old), Some(new)) = (previous.sequence, sample.sequence) {
                    let distance = new.wrapping_sub(old);
                    if distance == 0 {
                        if previous.raw != sample.raw
                            || previous.sample_rate_hertz != sample.sample_rate_hertz
                        {
                            let diagnostic = Diagnostic {
                                kind: "conflicting_duplicate".into(),
                                evidence: json!({"previous": previous, "received": sample}),
                            };
                            if diagnostics.last() != Some(&diagnostic) {
                                diagnostics.push(diagnostic);
                            }
                        }
                        return false;
                    }
                    if distance >= 32768 {
                        return false;
                    }
                    sample.sequence_gap = distance - 1;
                }
            }
            if continuity && sample.source == "late_packets" {
                if sample.raw >= previous.raw {
                    delta = Some(sample.raw - previous.raw);
                } else {
                    sample.epoch += 1;
                }
            }
            if sample.epoch != previous.epoch {
                self.baseline = None;
            }
        }
        self.delta = delta;
        if sample.source == "late_packets" {
            let baseline = *self.baseline.get_or_insert(sample.raw);
            self.increase_since_baseline = Some(sample.raw - baseline);
        }
        self.history.push(sample.clone());
        if self.history.len() > limit {
            self.history.drain(..self.history.len() - limit);
        }
        self.current = Some(sample);
        self.fresh = true;
        self.summarize();
        true
    }

    pub fn refresh(&mut self, now: f64, freshness: f64) -> bool {
        let fresh = self
            .current
            .as_ref()
            .is_some_and(|s| now >= s.observed_monotonic && now - s.observed_monotonic < freshness);
        let changed = fresh != self.fresh;
        self.fresh = fresh;
        changed
    }

    pub fn reset(&mut self) {
        self.display_epoch += 1;
        self.baseline = self.current.as_ref().map(|s| s.raw);
        self.increase_since_baseline = self.baseline.map(|_| 0);
        self.summarize();
    }

    fn summarize(&mut self) {
        let Some(current) = &self.current else {
            return;
        };
        let window: Vec<_> = self
            .history
            .iter()
            .filter(|s| s.epoch == current.epoch && s.display_epoch == self.display_epoch)
            .collect();
        let values: Vec<_> = window.iter().filter_map(|s| s.value).collect();
        let mean = (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64);
        let deviation = mean.map(|m| {
            (values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / values.len() as f64).sqrt()
        });
        let peak = window
            .iter()
            .filter(|s| s.value.is_some())
            .max_by(|a, b| a.value.partial_cmp(&b.value).unwrap());
        self.statistics = json!({"count": window.len(), "finite_count": values.len(), "mean": mean,
            "minimum": values.iter().copied().reduce(f64::min), "maximum": values.iter().copied().reduce(f64::max),
            "population_standard_deviation": deviation, "peak_observed_at": peak.map(|s| &s.observed_at),
            "window_start": window.first().map(|s| &s.observed_at), "window_end": window.last().map(|s| &s.observed_at),
            "epoch": current.epoch});
        self.histogram = Value::Null;
        if current.source == "receiver_latency" {
            if let Some(budget) = current.evidence["configured_latency_nanoseconds"]
                .as_u64()
                .filter(|b| *b > 0)
            {
                let mut counts = vec![0u64; 20];
                let mut overflow = 0;
                for sample in window {
                    if sample.raw <= 0 {
                        continue;
                    }
                    let Some(rate) = sample.sample_rate_hertz.filter(|r| *r > 0) else {
                        continue;
                    };
                    let bin = (sample.raw as u128 * 1_000_000_000 * 20)
                        / (u128::from(rate) * u128::from(budget));
                    if bin >= 20 {
                        overflow += 1;
                    } else {
                        counts[bin as usize] += 1;
                    }
                }
                self.histogram = json!({"counts": counts, "overflow": overflow, "budget_nanoseconds": budget,
                    "boundary_fractions": (0..=20).collect::<Vec<_>>(), "boundary_denominator": 20,
                    "semantics": "reported_maxima", "zero_excluded": true});
            }
        } else if current.source.ends_with("_clock") {
            let mut counts = vec![0u64; 400];
            let mut underflow = 0;
            let mut overflow = 0;
            for sample in window {
                if sample.raw < -200000 {
                    underflow += 1;
                } else if sample.raw >= 200000 {
                    overflow += 1;
                } else {
                    counts[((sample.raw + 200000) / 1000) as usize] += 1;
                }
            }
            self.histogram = json!({"counts": counts, "underflow": underflow, "overflow": overflow,
                "start_ppb": -200000, "bin_width_ppb": 1000});
        }
    }
}
