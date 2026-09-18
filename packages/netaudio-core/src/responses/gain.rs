use super::*;

pub const GAIN_LEVELS: [u32; 5] = [1, 2, 3, 4, 5];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GainStatus {
    pub channel_levels: Vec<u32>,
    pub device_type: String,
    pub supported_levels: Vec<u32>,
}

fn gain_device_type(parameter: &CodecParameterStatus) -> Option<&'static str> {
    match (parameter.parameter_type, parameter.mode) {
        (1, 2) => Some("input"),
        (2, 1) => Some("output"),
        _ => None,
    }
}

pub fn gain_status_from_codec_status(status: &CodecStatus) -> Option<GainStatus> {
    let mut matches = status.parameters.iter().filter_map(|parameter| {
        gain_device_type(parameter).map(|device_type| (device_type, parameter))
    });
    let (device_type, parameter) = matches.next()?;
    if matches.next().is_some()
        || status
            .parameters
            .iter()
            .any(|p| matches!(p.parameter_type, 1 | 2) && gain_device_type(p).is_none())
        || parameter.values.len() > 2
        || parameter.values.is_empty()
        || parameter
            .values
            .iter()
            .any(|value| !GAIN_LEVELS.contains(value))
    {
        return None;
    }
    Some(GainStatus {
        channel_levels: parameter.values.clone(),
        device_type: device_type.to_owned(),
        supported_levels: GAIN_LEVELS.to_vec(),
    })
}

pub fn parse_gain_status(data: &[u8]) -> Option<GainStatus> {
    gain_status_from_codec_status(&parse_codec_status(data)?)
}
