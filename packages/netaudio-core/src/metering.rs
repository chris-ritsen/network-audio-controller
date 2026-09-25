use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MeteringValue {
    pub dbfs: Option<f64>,
    pub state: &'static str,
}

impl MeteringValue {
    fn decode(value: u8, offset: f64) -> Self {
        let dbfs = match value {
            1..=253 => Some(-((f64::from(value) - offset) / 2.0)),
            _ => None,
        };
        let state = match value {
            0 => "clipping",
            1..=253 if dbfs.is_some_and(|level| level >= -61.0) => "signal_present",
            1..=253 => "below_threshold",
            254 => "muted",
            255 => "unknown",
        };

        Self { dbfs, state }
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MeteringScales {
    pub detailed: Vec<MeteringValue>,
    pub signal_presence: Vec<MeteringValue>,
}

pub fn metering_scale() -> MeteringScales {
    // Passive midrange calibration is supported by the AVIO measurements in
    // github.com/chris-ritsen/network-audio-controller/issues/54#issuecomment-5833037369.
    // It is an estimate: the reported extremes deviate from the linear scale.
    MeteringScales {
        detailed: (u8::MIN..=u8::MAX)
            .map(|value| MeteringValue::decode(value, 1.0))
            .collect(),
        signal_presence: (u8::MIN..=u8::MAX)
            .map(|value| MeteringValue::decode(value, 0.0))
            .collect(),
    }
}
