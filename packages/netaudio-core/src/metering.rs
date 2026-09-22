use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct MeteringValue {
    pub dbfs: Option<f64>,
    pub state: &'static str,
}

impl From<u8> for MeteringValue {
    fn from(value: u8) -> Self {
        let dbfs = match value {
            1..=253 => Some(-((f64::from(value) - 1.0) / 2.0)),
            _ => None,
        };
        let state = match value {
            0 => "clipping",
            1..=123 => "signal_present",
            124..=253 => "below_threshold",
            254 => "muted",
            255 => "unknown",
        };

        Self { dbfs, state }
    }
}

pub fn metering_scale() -> Vec<MeteringValue> {
    (u8::MIN..=u8::MAX).map(MeteringValue::from).collect()
}
