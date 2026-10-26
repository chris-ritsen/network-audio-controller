use super::{Device, Protocol, Report, Result, command, indexed, invalid};
use serde::Serialize;
use serde_json::{Value, json};
use std::net::Ipv4Addr;

#[derive(Clone, Debug, Serialize)]
pub struct Change {
    pub mac: String,
    pub device: String,
    pub device_type: Protocol,
    pub channel: Option<u8>,
    pub channel_name: Option<String>,
    pub key: String,
    pub value: String,
    pub current: Option<String>,
    pub current_described: Value,
    pub requested_described: Value,
}

impl Change {
    pub fn prepare(device: &Device, arguments: &Value) -> Result<Self> {
        let setting = arguments
            .get("setting")
            .or_else(|| arguments.get("key"))
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("A wireless setting is required"))?;
        let key = setting_key(device.protocol, setting)?;
        let channel = if device_key(&key) {
            None
        } else {
            channel_number(device, &arguments["channel"])?
        };
        if channel.is_none() && !device_key(&key) && key != "FLASH" {
            return Err(invalid("This wireless setting requires a channel"));
        }
        let mut value = formatted(device.protocol, &key, &arguments["value"])?;
        if key.starts_with("SLOT_") {
            let slot = integer(&arguments["slot"])
                .filter(|value| (1..=8).contains(value))
                .ok_or_else(|| invalid("Transmitter slot must be from 1 through 8"))?;
            value = format!("{slot} {value}");
        }
        let index = indexed(&key)
            .then(|| value.split_whitespace().next())
            .flatten();
        let current = device.value(channel, &key, index);
        let change = Self {
            mac: device.mac.clone(),
            device: device.name().to_owned(),
            device_type: device.protocol,
            channel,
            channel_name: channel.and_then(|channel| {
                device
                    .channels
                    .get(&channel)?
                    .get("CHAN_NAME")?
                    .as_str()
                    .map(str::to_owned)
            }),
            current_described: current.as_deref().map_or(Value::Null, |current| {
                described(device.protocol, &key, current)
            }),
            requested_described: described(device.protocol, &key, &value),
            key,
            value,
            current,
        };
        change.command()?;
        Ok(change)
    }

    pub fn command(&self) -> Result<Vec<u8>> {
        let target = self.channel.map_or_else(
            || self.key.clone(),
            |channel| format!("{channel} {}", self.key),
        );
        command(&format!("SET {target} {}", self.value))
    }

    pub fn matches(&self, report: &Report) -> bool {
        report.channel == self.channel
            && report.key == self.key
            && (!indexed(&self.key) || report.index() == self.value.split_whitespace().next())
    }

    pub fn verified(&self, report: &Report) -> bool {
        if !self.matches(report) {
            return false;
        }
        let comparable = |value: &str| {
            value
                .replace(['{', '}'], " ")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase()
        };
        if self.key == "AUDIO_MUTE" && self.value == "TOGGLE" {
            return matches!(
                (self.current.as_deref(), report.value.as_str()),
                (Some("ON"), "OFF") | (Some("OFF"), "ON")
            );
        }
        if self.key == "NET_SETTINGS" && self.value.split_whitespace().nth(1) == Some("AUTO") {
            return self
                .value
                .split_whitespace()
                .take(2)
                .eq(report.value.split_whitespace().take(2));
        }
        comparable(&self.value) == comparable(&report.value)
    }
}

fn device_key(key: &str) -> bool {
    matches!(key, "DEVICE_ID" | "DEVICE_NAME" | "NET_SETTINGS")
}

pub fn setting_key(protocol: Protocol, setting: &str) -> Result<String> {
    let normalized = setting.trim().to_ascii_lowercase().replace([' ', '-'], "_");
    let key = match (protocol, normalized.as_str()) {
        (_, "name" | "channel_name") => "CHAN_NAME",
        (_, "frequency") => "FREQUENCY",
        (Protocol::Ad4d, "gain") => "AUDIO_GAIN",
        (Protocol::Ad4d, "mute") => "AUDIO_MUTE",
        (Protocol::Ad4d, "group_channel") => "GROUP_CHANNEL",
        (Protocol::Ad4d, "identify" | "flash") => "FLASH",
        (Protocol::Ad4d, "device_name") => "DEVICE_ID",
        (Protocol::Ad4d, "frequency2") => "FREQUENCY2",
        (Protocol::Ad4d, "group_channel2") => "GROUP_CHANNEL2",
        (Protocol::Ad4d, "network" | "net_settings") => "NET_SETTINGS",
        (Protocol::Ad4d, "slot_device_id") => "SLOT_TX_DEVICE_ID",
        (Protocol::P10t, "input_level" | "audio_level") => "AUDIO_IN_LVL",
        (Protocol::P10t, "rf_power") => "RF_TX_LVL",
        (Protocol::P10t, "rf_mute" | "mute") => "RF_MUTE",
        (Protocol::P10t, "group_channel") => "GROUP_CHAN",
        (Protocol::P10t, "transmit_mode") => "AUDIO_TX_MODE",
        (Protocol::P10t, "line_level") => "AUDIO_IN_LINE_LVL",
        (Protocol::P10t, "device_name") => "DEVICE_NAME",
        _ => "",
    };
    let key = if key.is_empty() {
        setting.trim().to_ascii_uppercase()
    } else {
        key.to_owned()
    };
    if key == "METER_RATE" {
        return Err(invalid(
            "Meter rate is owned by the wireless monitoring service",
        ));
    }
    let permitted = match protocol {
        Protocol::Ad4d => matches!(
            key.as_str(),
            "DEVICE_ID"
                | "NET_SETTINGS"
                | "CHAN_NAME"
                | "AUDIO_GAIN"
                | "AUDIO_MUTE"
                | "FREQUENCY"
                | "FREQUENCY2"
                | "GROUP_CHANNEL"
                | "GROUP_CHANNEL2"
                | "FLASH"
                | "SLOT_INPUT_PAD"
                | "SLOT_OFFSET"
                | "SLOT_POLARITY"
                | "SLOT_RF_OUTPUT"
                | "SLOT_RF_POWER_MODE"
                | "SLOT_TX_DEVICE_ID"
        ),
        Protocol::P10t => matches!(
            key.as_str(),
            "DEVICE_NAME"
                | "CHAN_NAME"
                | "AUDIO_IN_LVL"
                | "GROUP_CHAN"
                | "FREQUENCY"
                | "RF_TX_LVL"
                | "RF_MUTE"
                | "AUDIO_TX_MODE"
                | "AUDIO_IN_LINE_LVL"
        ),
    };
    if !permitted {
        return Err(invalid(format!(
            "{setting} is not writable with the {} protocol",
            protocol.name()
        )));
    }
    Ok(key)
}

fn integer(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.trim().parse().ok())
}

fn channel_number(device: &Device, value: &Value) -> Result<Option<u8>> {
    if value.is_null() || value == "" {
        return Ok(None);
    }
    if let Some(number) = integer(value).and_then(|value| u8::try_from(value).ok()) {
        return device
            .channels
            .contains_key(&number)
            .then_some(Some(number))
            .ok_or_else(|| invalid("The wireless device has no such channel"));
    }
    let wanted = value
        .as_str()
        .ok_or_else(|| invalid("Channel must be a number or name"))?
        .trim();
    let matches: Vec<_> = device
        .channels
        .iter()
        .filter(|(_, properties)| {
            properties
                .get("CHAN_NAME")
                .and_then(Value::as_str)
                .is_some_and(|name| name.eq_ignore_ascii_case(wanted))
        })
        .map(|(number, _)| *number)
        .collect();
    match matches.as_slice() {
        [number] => Ok(Some(*number)),
        _ => Err(invalid(
            "Channel name does not identify one wireless channel",
        )),
    }
}

fn formatted(protocol: Protocol, key: &str, value: &Value) -> Result<String> {
    if key == "NET_SETTINGS" {
        return network(value);
    }
    let text = match value {
        Value::String(text) => text.trim().to_owned(),
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => if *value { "on" } else { "off" }.to_owned(),
        Value::Null if key == "FLASH" => String::new(),
        _ => {
            return Err(invalid(
                "Wireless setting requires a string, number or boolean value",
            ));
        }
    };
    let folded = text.to_ascii_lowercase();
    let boolean = match folded.as_str() {
        "on" | "true" | "yes" | "1" | "mute" | "muted" | "enable" | "enabled" => Some(true),
        "off" | "false" | "no" | "0" | "unmute" | "unmuted" | "disable" | "disabled" => Some(false),
        _ => None,
    };
    let result = match key {
        "AUDIO_MUTE" if folded == "toggle" => "TOGGLE".to_owned(),
        "AUDIO_MUTE" => match boolean {
            Some(true) => "ON",
            Some(false) => "OFF",
            None => return Err(invalid("Mute takes on or off")),
        }
        .to_owned(),
        "RF_MUTE" => match boolean {
            Some(true) => "1",
            Some(false) => "0",
            None => return Err(invalid("RF mute takes on or off")),
        }
        .to_owned(),
        "FLASH" => "ON".to_owned(),
        "SLOT_INPUT_PAD" => match (boolean, folded.as_str()) {
            (Some(true), _) | (_, "-12") => "0",
            (Some(false), _) => "12",
            _ => return Err(invalid("Input pad takes on or off")),
        }
        .to_owned(),
        "SLOT_OFFSET" => {
            let offset = text
                .parse::<i32>()
                .ok()
                .filter(|value| (-12..=21).contains(value))
                .ok_or_else(|| invalid("Offset takes whole decibels from -12 to 21"))?;
            format!("{:03}", offset + 12)
        }
        "SLOT_POLARITY" => choice(&text, &["POSITIVE", "NEGATIVE"])?,
        "SLOT_RF_OUTPUT" => choice(&text, &["RF_ON", "RF_MUTE"])?,
        "SLOT_RF_POWER_MODE" => choice(&text, &["LOW", "NORMAL", "HIGH"])?,
        "AUDIO_TX_MODE" => match folded.as_str() {
            "mono" | "1" => "1",
            "point_to_point" | "point to point" | "ptp" | "2" => "2",
            "stereo" | "3" => "3",
            _ => {
                return Err(invalid(
                    "Transmit mode takes mono, point to point or stereo",
                ));
            }
        }
        .to_owned(),
        "AUDIO_IN_LINE_LVL" => match folded.as_str() {
            "line" | "1" | "on" => "1",
            "aux" | "0" | "off" => "0",
            _ => return Err(invalid("Input level takes line or aux")),
        }
        .to_owned(),
        "RF_TX_LVL" => choice(&text, &["10", "50", "100"])?,
        "FREQUENCY" | "FREQUENCY2" => {
            let number = text
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite() && *number >= 0.0)
                .ok_or_else(|| invalid("Frequency takes a finite positive MHz or kHz value"))?;
            let kilohertz = if number < 10_000.0 {
                number * 1000.0
            } else {
                number
            }
            .round_ties_even();
            if kilohertz > 999_999.0 {
                return Err(invalid("Frequency exceeds the six digit kHz field"));
            }
            format!("{:06}", kilohertz as u32)
        }
        "AUDIO_GAIN" if protocol == Protocol::Ad4d => {
            let gain = text
                .parse::<u8>()
                .ok()
                .filter(|gain| *gain <= 60)
                .ok_or_else(|| {
                    invalid("Gain takes a setting from 0 through 60, where 18 is 0 dB")
                })?;
            format!("{gain:03}")
        }
        _ if protocol == Protocol::Ad4d
            && matches!(
                key,
                "CHAN_NAME"
                    | "DEVICE_ID"
                    | "GROUP_CHANNEL"
                    | "GROUP_CHANNEL2"
                    | "SLOT_TX_DEVICE_ID"
            ) =>
        {
            if text.contains(['{', '}']) {
                return Err(invalid("This wireless setting cannot contain braces"));
            }
            format!("{{{text}}}")
        }
        _ => text,
    };
    command(&format!("SET {key} {result}"))?;
    Ok(result)
}

fn choice(value: &str, choices: &[&str]) -> Result<String> {
    let value = value.to_ascii_uppercase();
    if choices.contains(&value.as_str()) {
        Ok(value)
    } else {
        Err(invalid(format!(
            "Value must be one of {}",
            choices.join(", ")
        )))
    }
}

fn network(value: &Value) -> Result<String> {
    if !value.is_object() {
        return Err(invalid("Network settings require an object"));
    }
    let interface = choice(
        value["interface"].as_str().unwrap_or(""),
        &["SC", "D1", "D2"],
    )?;
    let mode = choice(value["mode"].as_str().unwrap_or(""), &["AUTO", "MANUAL"])?;
    if mode == "AUTO" {
        return Ok(format!("{interface} AUTO na na na"));
    }
    let mut parts = vec![interface, mode];
    for field in ["address", "subnet_mask", "gateway"] {
        let address = value[field]
            .as_str()
            .ok_or_else(|| invalid(format!("{field} must be an IPv4 address")))?;
        address
            .parse::<Ipv4Addr>()
            .map_err(|_| invalid(format!("{field} must be an IPv4 address")))?;
        parts.push(address.to_owned());
    }
    Ok(parts.join(" "))
}

pub fn described(protocol: Protocol, key: &str, value: &str) -> Value {
    let text = value.trim().trim_matches(['{', '}']).trim();
    match (protocol, key, text.parse::<i64>().ok()) {
        (Protocol::Ad4d, "AUDIO_GAIN", Some(value)) => {
            json!(format!("{value} ({:+} dB)", value - 18))
        }
        (_, "FREQUENCY", Some(value)) => json!(format!("{:.3} MHz", value as f64 / 1000.0)),
        (_, "RF_MUTE", Some(1)) => json!("on"),
        (_, "RF_MUTE", Some(0)) => json!("off"),
        _ => json!(text),
    }
}
