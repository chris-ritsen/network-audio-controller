use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", schemars(rename = "HostRecordingConfiguration"))]
pub struct Configuration {
    pub generation: u64,
    pub sample_rate: u32,
    pub xruns: u64,
    pub sources: BTreeMap<String, u64>,
}

impl Configuration {
    pub fn validate(&self) -> Result<(), String> {
        if self.sample_rate == 0 || self.sources.is_empty() {
            return Err("JACK recording requires a sample rate and source ports".into());
        }
        if self.sources.len() > 2048 {
            return Err("JACK recording exceeds the 2048 source memory budget".into());
        }
        if self.sources.iter().any(|(name, identity)| {
            name.is_empty() || name.len() > 1024 || name.contains('\0') || *identity == 0
        }) {
            return Err("JACK recording has an invalid source identity".into());
        }
        Ok(())
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "schema", schemars(rename = "HostRecordingRecord"))]
pub enum Record {
    Requested {
        configuration: Configuration,
        observed_at: String,
    },
    Started {
        configuration: Configuration,
        observed_at: String,
    },
    Frame(Frame),
    Finished {
        observed_at: String,
        error: Option<String>,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", schemars(rename = "HostRecordingFrame"))]
pub struct Frame {
    pub second: i64,
    pub started_at: String,
    pub ended_at: String,
    pub frames: u64,
    pub sample_rate: u32,
    pub complete: bool,
    pub levels: BTreeMap<String, Level>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", schemars(rename = "HostRecordingLevel"))]
pub struct Level {
    pub peak_amplitude: Option<f64>,
    pub squared_amplitude_sum: Option<f64>,
    pub peak_dbfs: Option<f64>,
    pub rms_dbfs: Option<f64>,
    pub error: Option<String>,
}
