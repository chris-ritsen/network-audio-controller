use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Reading {
    Level {
        #[serde(deserialize_with = "Option::deserialize")]
        peak_dbfs: Option<f64>,
        #[serde(deserialize_with = "Option::deserialize")]
        rms_dbfs: Option<f64>,
        frames: Option<u64>,
        sample_rate: Option<u32>,
        measured_at: Option<String>,
        duration_seconds: Option<f64>,
        recorded_second: Option<i64>,
    },
    Disconnected {
        connected: bool,
    },
    Unavailable {
        error: String,
    },
}

pub type Readings = BTreeMap<String, Reading>;
