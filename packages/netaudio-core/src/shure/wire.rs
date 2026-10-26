use super::{Result, invalid};
use serde::{Deserialize, Serialize};

const MAXIMUM_FRAME_BYTES: usize = 65_536;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Ad4d,
    P10t,
}

impl Protocol {
    pub fn for_model(value: &str) -> Option<Self> {
        let value = value.to_ascii_uppercase();
        if value.starts_with("AD4D") || value.starts_with("AD4Q") {
            Some(Self::Ad4d)
        } else if value.starts_with("P10T") {
            Some(Self::P10t)
        } else {
            None
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Ad4d => "ad4d",
            Self::P10t => "p10t",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Report {
    pub channel: Option<u8>,
    pub key: String,
    pub value: String,
}

impl Report {
    pub fn index(&self) -> Option<&str> {
        indexed(&self.key)
            .then(|| self.value.split_whitespace().next())
            .flatten()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Report(Protocol, Report),
    Sample(Vec<Report>),
    Rejected(Protocol),
}

#[derive(Default)]
pub struct Decoder {
    buffer: Vec<u8>,
}

impl Decoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        for byte in bytes {
            if self.buffer.is_empty() && byte.is_ascii_whitespace() {
                continue;
            }
            if self.buffer.is_empty() && *byte != b'<' {
                return Err(invalid(
                    "Shure response does not begin with a frame delimiter",
                ));
            }
            if self.buffer.len() >= MAXIMUM_FRAME_BYTES {
                return Err(invalid("Shure response exceeds the frame size limit"));
            }
            if *byte == b'<' && !self.buffer.is_empty() {
                return Err(invalid("Shure response contains a nested frame delimiter"));
            }
            self.buffer.push(*byte);
            if *byte == b'>' {
                let frame = std::mem::take(&mut self.buffer);
                let text = std::str::from_utf8(&frame)
                    .map_err(|_| invalid("Shure response is not UTF-8"))?;
                messages.push(parse(&text[1..text.len() - 1])?);
            }
        }
        Ok(messages)
    }
}

fn field(text: &str) -> (&str, &str) {
    let text = text.trim_start();
    text.find(char::is_whitespace).map_or((text, ""), |index| {
        (&text[..index], text[index..].trim_start())
    })
}

fn parse(text: &str) -> Result<Message> {
    let (kind, remaining) = field(text);
    let (first, remaining) = field(remaining);
    let (channel, key, value) =
        if first.bytes().all(|byte| byte.is_ascii_digit()) && !first.is_empty() {
            let channel = first
                .parse::<u8>()
                .map_err(|_| invalid("Invalid Shure channel number"))?;
            let (key, value) = field(remaining);
            (Some(channel), key, value.trim())
        } else {
            (None, first, remaining.trim())
        };
    if kind == "SAMPLE" {
        if key != "ALL" {
            return Err(invalid("Unsupported Shure sample format"));
        }
        return sample(
            channel.ok_or_else(|| invalid("Shure sample has no channel"))?,
            value,
        )
        .map(Message::Sample);
    }
    let protocol = match kind {
        "REP" => Protocol::Ad4d,
        "REPORT" => Protocol::P10t,
        _ => return Err(invalid("Unsupported Shure response format")),
    };
    if key == "ERR" {
        return Ok(Message::Rejected(protocol));
    }
    if key.is_empty()
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid("Invalid Shure property key"));
    }
    let value = if value.starts_with('{') {
        value
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
            .ok_or_else(|| invalid("Unclosed Shure property value"))?
    } else {
        value
    };
    Ok(Message::Report(
        protocol,
        Report {
            channel,
            key: key.to_owned(),
            value: value.trim().to_owned(),
        },
    ))
}

fn sample(channel: u8, value: &str) -> Result<Vec<Report>> {
    let mut fields = value.split_whitespace();
    let mut reports = Vec::new();
    for key in [
        "CHAN_QUALITY",
        "AUDIO_LED_BITMAP",
        "AUDIO_LEVEL_PEAK",
        "AUDIO_LEVEL_RMS",
    ] {
        let value = fields
            .next()
            .ok_or_else(|| invalid("Incomplete Shure meter sample"))?;
        reports.push(Report {
            channel: Some(channel),
            key: key.to_owned(),
            value: value.to_owned(),
        });
    }
    let mut group = 1;
    while let Some(status) = fields.next() {
        if group > 8 || status.len() > 16 || !status.is_ascii() {
            return Err(invalid("Unsupported Shure antenna sample dimensions"));
        }
        let suffix = if group == 1 {
            String::new()
        } else {
            group.to_string()
        };
        reports.push(Report {
            channel: Some(channel),
            key: format!("ANTENNA_STATUS{suffix}"),
            value: status.to_owned(),
        });
        for antenna in 1..=status.len() {
            for key in ["RSSI_LED_BITMAP", "RSSI"] {
                let value = fields
                    .next()
                    .ok_or_else(|| invalid("Incomplete Shure antenna sample"))?;
                reports.push(Report {
                    channel: Some(channel),
                    key: format!("{key}{suffix}"),
                    value: format!("{antenna} {value}"),
                });
            }
        }
        group += 1;
    }
    Ok(reports)
}

pub fn command(value: &str) -> Result<Vec<u8>> {
    if value.is_empty()
        || value.len() > MAXIMUM_FRAME_BYTES - 6
        || value
            .chars()
            .any(|character| character.is_control() || matches!(character, '<' | '>'))
    {
        return Err(invalid(
            "Shure command contains invalid characters or exceeds its size limit",
        ));
    }
    Ok(format!("< {value} >\r\n").into_bytes())
}

pub fn indexed(key: &str) -> bool {
    matches!(
        key,
        "RSSI" | "RSSI2" | "RSSI_LED_BITMAP" | "RSSI_LED_BITMAP2" | "NET_SETTINGS"
    ) || key.starts_with("SLOT_")
}

pub fn sampled(key: &str) -> bool {
    matches!(
        key,
        "AUDIO_LEVEL_PEAK"
            | "AUDIO_LEVEL_RMS"
            | "AUDIO_IN_LVL_L"
            | "AUDIO_IN_LVL_R"
            | "CHAN_QUALITY"
            | "AUDIO_LED_BITMAP"
            | "ANTENNA_STATUS"
            | "ANTENNA_STATUS2"
            | "RSSI"
            | "RSSI2"
            | "RSSI_LED_BITMAP"
            | "RSSI_LED_BITMAP2"
    )
}

pub fn meter_value(key: &str, value: &str) -> Option<(Option<f32>, bool)> {
    let value = value.trim().parse::<i32>().ok()? as f32 - 120.0;
    match key {
        "AUDIO_LEVEL_PEAK" | "AUDIO_LEVEL_RMS" => Some((Some(value), value >= -80.0)),
        _ => None,
    }
}
