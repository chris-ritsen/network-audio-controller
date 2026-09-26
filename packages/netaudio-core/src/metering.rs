use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MeteringValue {
    pub raw: u8,
    pub source: &'static str,
    pub dbfs: Option<f64>,
    pub state: &'static str,
    pub display_state: &'static str,
}

impl MeteringValue {
    fn decode(value: u8, passive: bool) -> Self {
        let finite = (1..=if passive { 252 } else { 253 }).contains(&value);
        let dbfs = finite.then(|| -((f64::from(value) - if passive { 0.0 } else { 1.0 }) / 2.0));
        let state = if passive && value >= 253 {
            "mute_or_floor"
        } else {
            match value {
                0 => "clipping",
                1..=253 if dbfs.is_some_and(|level| level >= -60.0) => "signal_present",
                1..=253 => "below_threshold",
                254 => "muted",
                255 => "framing_marker",
            }
        };

        Self {
            raw: value,
            source: if passive {
                "signal_presence"
            } else {
                "detailed"
            },
            dbfs,
            state,
            display_state: match state {
                "mute_or_floor" => "muted",
                "framing_marker" => "unknown",
                _ => state,
            },
        }
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MeteringScales {
    pub detailed: Vec<MeteringValue>,
    pub signal_presence: Vec<MeteringValue>,
}

pub fn metering_scale() -> MeteringScales {
    MeteringScales {
        detailed: (u8::MIN..=u8::MAX)
            .map(|value| MeteringValue::decode(value, false))
            .collect(),
        signal_presence: (u8::MIN..=u8::MAX)
            .map(|value| MeteringValue::decode(value, true))
            .collect(),
    }
}
