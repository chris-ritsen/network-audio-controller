use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAXIMUM_TARGETS: usize = 32;
pub const MAXIMUM_SOURCES: usize = 2048;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "HostMeterRequest"))]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub targets: BTreeMap<String, Vec<String>>,
    #[serde(default = "default_seconds")]
    pub seconds: f64,
}

fn default_seconds() -> f64 {
    0.5
}

impl Request {
    pub fn validate(&mut self) -> Result<(), String> {
        if self.targets.len() > MAXIMUM_TARGETS {
            return Err(format!(
                "Targets must contain at most {MAXIMUM_TARGETS} entries"
            ));
        }
        if !self.seconds.is_finite() {
            return Err("Measurement duration must be a finite number".into());
        }
        let mut total = 0;
        for (name, sources) in &mut self.targets {
            if name.is_empty() || name.contains('\0') {
                return Err("Measurement target names must be nonempty text".into());
            }
            if sources
                .iter()
                .any(|source| source.is_empty() || source.contains('\0'))
            {
                return Err("Measurement source names must be nonempty text".into());
            }
            sources.sort();
            sources.dedup();
            total += sources.len();
        }
        if total > MAXIMUM_SOURCES {
            return Err(format!(
                "Measurement source count exceeds {MAXIMUM_SOURCES}; divide the request into smaller groups"
            ));
        }
        self.seconds = self.seconds.clamp(0.05, 5.0);
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "HostMeterReading"))]
#[serde(untagged, deny_unknown_fields)]
pub enum Reading {
    Level {
        peak_dbfs: Option<f64>,
        rms_dbfs: Option<f64>,
        frames: u64,
        sample_rate: u32,
        measured_at: String,
        duration_seconds: f64,
    },
    Disconnected {
        connected: bool,
    },
    Unavailable {
        error: String,
    },
}

pub type Readings = BTreeMap<String, Reading>;

pub fn decibels(amplitude: f64) -> Option<f64> {
    (amplitude > 0.0 && amplitude.is_finite())
        .then(|| (20.0 * amplitude.log10() * 10.0).round_ties_even() / 10.0)
}
