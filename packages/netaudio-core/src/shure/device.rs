use super::{Protocol, Report, Result, indexed, invalid};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Device {
    pub ip: String,
    pub mac: String,
    pub protocol: Protocol,
    pub online: bool,
    pub last_seen: Option<f64>,
    pub properties: Map<String, Value>,
    pub channels: BTreeMap<u8, Map<String, Value>>,
}

impl Device {
    pub fn new(ip: String, mac: String, protocol: Protocol) -> Self {
        Self {
            ip,
            mac,
            protocol,
            online: true,
            last_seen: None,
            properties: Map::new(),
            channels: BTreeMap::new(),
        }
    }

    pub fn observe(&mut self, report: &Report, timestamp: f64) -> Result<bool> {
        if !timestamp.is_finite() || timestamp < 0.0 {
            return Err(invalid("Shure observation timestamp is invalid"));
        }
        let target = match report.channel {
            Some(channel) => self.channels.entry(channel).or_default(),
            None => &mut self.properties,
        };
        let (target, key, value) = if indexed(&report.key) {
            let (index, value) = report
                .value
                .split_once(char::is_whitespace)
                .ok_or_else(|| invalid("Indexed Shure property has no value"))?;
            let target = target
                .entry(report.key.clone())
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .ok_or_else(|| invalid("Shure property changed its indexing format"))?;
            (
                target,
                index,
                value
                    .trim()
                    .trim_start_matches('{')
                    .trim_end_matches('}')
                    .trim(),
            )
        } else {
            (target, report.key.as_str(), report.value.as_str())
        };
        let changed = target.get(key).and_then(Value::as_str) != Some(value);
        target.insert(key.to_owned(), json!(value));
        let changed = changed || !self.online;
        self.online = true;
        self.last_seen = Some(timestamp);
        Ok(changed)
    }

    pub fn value(&self, channel: Option<u8>, key: &str, index: Option<&str>) -> Option<String> {
        let properties = match channel {
            Some(channel) => self.channels.get(&channel)?,
            None => &self.properties,
        };
        let value = properties.get(key)?;
        match index {
            Some(index) => value
                .get(index)?
                .as_str()
                .map(|value| format!("{index} {value}")),
            None => value.as_str().map(str::to_owned),
        }
    }

    pub fn name(&self) -> &str {
        self.properties
            .get(match self.protocol {
                Protocol::Ad4d => "DEVICE_ID",
                Protocol::P10t => "DEVICE_NAME",
            })
            .and_then(Value::as_str)
            .unwrap_or("")
    }

    pub fn presentation(&self) -> Value {
        let mut result = json!({
            "ip": self.ip, "mac": self.mac, "device_type": self.protocol.name(), "name": self.name(),
            "online": self.online, "properties": self.properties,
        });
        let fields = result.as_object_mut().expect("constructed object");
        if let Some(timestamp) = self.last_seen {
            fields.insert("last_seen".to_owned(), json!(timestamp));
        }
        for (key, field) in [
            ("MODEL", "model"),
            ("FW_VER", "firmware_version"),
            ("RF_BAND", "rf_band"),
            ("TRANSMISSION_MODE", "transmission_mode"),
            ("QUADVERSITY_MODE", "quadversity_mode"),
            ("ENCRYPTION_MODE", "encryption_mode"),
        ] {
            if let Some(value) = self.properties.get(key) {
                fields.insert(field.to_owned(), value.clone());
            }
        }
        let channels: Map<String, Value> = self
            .channels
            .iter()
            .map(|(number, properties)| (number.to_string(), channel(self.protocol, properties)))
            .collect();
        fields.insert("channels".to_owned(), Value::Object(channels));
        result
    }
}

fn number(properties: &Map<String, Value>, key: &str) -> Option<i64> {
    properties.get(key)?.as_str()?.parse().ok()
}

fn text_fields(
    target: &mut Map<String, Value>,
    properties: &Map<String, Value>,
    fields: &[(&str, &str)],
) {
    for (key, field) in fields {
        if let Some(value) = properties.get(*key) {
            target.insert((*field).to_owned(), value.clone());
        }
    }
}

fn number_fields(
    target: &mut Map<String, Value>,
    properties: &Map<String, Value>,
    fields: &[(&str, &str)],
) {
    for (key, field) in fields {
        if let Some(value) = number(properties, key) {
            target.insert((*field).to_owned(), json!(value));
        }
    }
}

fn channel(protocol: Protocol, properties: &Map<String, Value>) -> Value {
    let mut result = Map::new();
    result.insert(
        "name".to_owned(),
        properties.get("CHAN_NAME").cloned().unwrap_or(Value::Null),
    );
    result.insert("properties".to_owned(), json!(properties));
    number_fields(&mut result, properties, &[("FREQUENCY", "frequency")]);
    match protocol {
        Protocol::Ad4d => {
            number_fields(
                &mut result,
                properties,
                &[
                    ("AUDIO_GAIN", "audio_gain"),
                    ("AUDIO_LEVEL_PEAK", "audio_level_peak"),
                    ("AUDIO_LEVEL_RMS", "audio_level_rms"),
                    ("CHAN_QUALITY", "signal_quality"),
                ],
            );
            text_fields(
                &mut result,
                properties,
                &[
                    ("ANTENNA_STATUS", "antenna_status"),
                    ("ENCRYPTION_STATUS", "encryption_status"),
                    ("INTERFERENCE_STATUS", "interference_status"),
                    ("GROUP_CHANNEL", "group_channel"),
                ],
            );
            for (key, field) in [("AUDIO_MUTE", "audio_mute"), ("FD_MODE", "fd_mode")] {
                if let Some(value @ ("ON" | "OFF")) = properties.get(key).and_then(Value::as_str) {
                    result.insert(field.to_owned(), json!(value == "ON"));
                }
            }
            if let Some(model) = properties.get("TX_MODEL").and_then(Value::as_str) {
                if let Some(antenna) = properties.get("ANTENNA_STATUS").and_then(Value::as_str) {
                    result.insert(
                        "active".to_owned(),
                        json!(model != "UNKNOWN" && antenna != "XX"),
                    );
                }
                if model != "UNKNOWN" {
                    let mut transmitter = Map::new();
                    text_fields(
                        &mut transmitter,
                        properties,
                        &[
                            ("TX_MODEL", "model"),
                            ("TX_DEVICE_ID", "device_id"),
                            ("TX_MUTE_MODE_STATUS", "mute_status"),
                        ],
                    );
                    if let Some(minutes) =
                        number(properties, "TX_BATT_MINS").filter(|value| *value < 65530)
                    {
                        transmitter.insert("battery_minutes".to_owned(), json!(minutes));
                        transmitter.insert(
                            "battery_hours".to_owned(),
                            json!((minutes as f64 / 6.0).round() / 10.0),
                        );
                    }
                    if let Some(value @ ("LION" | "ALKA" | "NIMH")) =
                        properties.get("TX_BATT_TYPE").and_then(Value::as_str)
                    {
                        transmitter.insert("battery_type".to_owned(), json!(value));
                    }
                    number_fields(
                        &mut transmitter,
                        properties,
                        &[
                            ("TX_BATT_CHARGE_PERCENT", "battery_charge_percent"),
                            ("TX_BATT_BARS", "battery_bars"),
                            ("TX_BATT_CYCLE_COUNT", "battery_cycle_count"),
                            ("TX_BATT_TEMP_F", "battery_temp_f"),
                        ],
                    );
                    if let Some(value) =
                        number(properties, "TX_POWER_LEVEL").filter(|value| *value < 255)
                    {
                        transmitter.insert("power_level".to_owned(), json!(value));
                    }
                    result.insert("transmitter".to_owned(), Value::Object(transmitter));
                }
            }
        }
        Protocol::P10t => {
            number_fields(
                &mut result,
                properties,
                &[
                    ("AUDIO_IN_LVL", "audio_in_level"),
                    ("AUDIO_IN_LVL_L", "audio_in_level_l"),
                    ("AUDIO_IN_LVL_R", "audio_in_level_r"),
                    ("RF_TX_LVL", "rf_tx_level"),
                    ("AUDIO_TX_MODE", "audio_tx_mode"),
                    ("AUDIO_IN_LINE_LVL", "audio_in_line_level"),
                ],
            );
            text_fields(&mut result, properties, &[("GROUP_CHAN", "group_channel")]);
            if let Some(value @ ("0" | "1")) = properties.get("RF_MUTE").and_then(Value::as_str) {
                result.insert("rf_mute".to_owned(), json!(value == "1"));
            }
        }
    }
    Value::Object(result)
}
