use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, PartialEq)]
pub(super) enum Record {
    Hash(BTreeMap<String, String>),
    Json(Value),
}

#[derive(Default)]
pub(super) struct Records {
    pub values: BTreeMap<String, Record>,
    devices: BTreeMap<String, Arc<Value>>,
    wireless: BTreeMap<String, Value>,
    correlations: crate::shure::correlations::Snapshot,
}

impl Records {
    pub fn apply(&mut self, event: &Value) -> Result<bool> {
        match event["event"].as_str().unwrap_or("") {
            crate::events::SNAPSHOT => {
                self.devices = serde_json::from_value(event["devices"].clone())?;
                self.wireless = serde_json::from_value(event["shure_devices"].clone())?;
                if let Some(correlations) = event.get("shure_correlations") {
                    self.correlations = serde_json::from_value(correlations.clone())?;
                }
                self.values.clear();
                for (name, device) in &self.devices {
                    self.values.insert(
                        format!("netaudio:daemon:device:{name}"),
                        device_record(device),
                    );
                }
                self.managed(&event["managed"], &event["managed_domains"]);
                self.correlate();
            }
            crate::events::DEVICE_DISCOVERED
            | crate::events::DEVICE_UPDATED
            | crate::events::TELEMETRY_UPDATED => {
                let name = event["server_name"]
                    .as_str()
                    .context("Published device has no identity")?;
                self.values.insert(
                    format!("netaudio:daemon:device:{name}"),
                    device_record(&event["device"]),
                );
                self.devices
                    .insert(name.to_owned(), Arc::new(event["device"].clone()));
                self.correlate();
            }
            crate::events::DEVICE_REMOVED => {
                let name = event["server_name"]
                    .as_str()
                    .context("Removed device has no identity")?;
                self.values
                    .remove(&format!("netaudio:daemon:device:{name}"));
                self.devices.remove(name);
                self.correlate();
            }
            crate::events::MANAGED_STATUS => {
                self.managed(&event["managed"], &event["managed_domains"])
            }
            "shure_device_discovered" | "shure_device_updated" => {
                let mac = event["mac"]
                    .as_str()
                    .context("Published wireless device has no identity")?;
                self.wireless
                    .insert(mac.to_owned(), event["device"].clone());
                if event["device"]["online"] != true {
                    self.values.remove(&format!("netaudio:shure:meters:{mac}"));
                }
                self.correlate();
            }
            "shure_device_removed" => {
                let mac = event["mac"]
                    .as_str()
                    .context("Removed wireless device has no identity")?;
                self.wireless.remove(mac);
                self.values.remove(&format!("netaudio:shure:{mac}"));
                self.values.remove(&format!("netaudio:shure:meters:{mac}"));
            }
            "shure_correlations_changed" => {
                self.correlations = serde_json::from_value(event["correlations"].clone())?;
                self.correlate();
            }
            crate::events::SHURE_METER_VALUES => {
                let mac = event["mac"]
                    .as_str()
                    .context("Wireless meter has no identity")?;
                self.values.insert(
                    format!("netaudio:shure:meters:{mac}"),
                    Record::Json(event.clone()),
                );
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn managed(&mut self, status: &Value, domains: &Value) {
        self.values.insert(
            "netaudio:daemon:ddm:status".into(),
            Record::Json(status.clone()),
        );
        self.values.insert(
            "netaudio:daemon:ddm:domains".into(),
            Record::Json(domains.clone()),
        );
    }

    fn correlate(&mut self) {
        for (mac, device) in &self.wireless {
            let mut device = device.clone();
            if let Some(dante) = crate::shure::correlations::linked_device(
                mac,
                &device,
                &self.devices,
                &self.correlations,
            ) {
                device["dante"] = correlation_view(dante);
            }
            self.values
                .insert(format!("netaudio:shure:{mac}"), Record::Json(device));
        }
    }
}

fn device_record(device: &Value) -> Record {
    let mut fields = BTreeMap::new();
    for field in [
        "server_name",
        "name",
        "ipv4",
        "model_id",
        "bluetooth_device",
        "last_seen",
    ] {
        let value = match &device[field] {
            Value::Null => String::new(),
            Value::String(value) => value.clone(),
            value => value.to_string(),
        };
        fields.insert(field.to_owned(), value);
    }
    for field in ["bluetooth_connected", "online"] {
        fields.insert(
            field.to_owned(),
            match device[field].as_bool() {
                Some(true) => "1",
                Some(false) => "0",
                None => "",
            }
            .to_owned(),
        );
    }
    Record::Hash(fields)
}

fn correlation_view(record: &Value) -> Value {
    let mut view = serde_json::Map::new();
    for (target, source) in [
        ("server_name", "server_name"),
        ("name", "name"),
        ("ip", "ipv4"),
        ("mac", "mac_address"),
        ("sample_rate", "sample_rate_hz"),
        ("supported_sample_rates", "supported_sample_rates_hz"),
        ("encoding", "encoding"),
        ("supported_encodings", "supported_encodings"),
        ("encoding_configurable", "encoding_configurable"),
        ("latency", "latency_ms"),
        ("active_latency", "active_latency_ms"),
        ("configured_latency", "configured_latency_ms"),
        ("default_latency", "default_latency_ms"),
        ("min_latency", "min_latency_ms"),
        ("max_latency", "max_latency_ms"),
        ("standard_latency_choices", "standard_latency_choices_ms"),
    ] {
        view.insert(target.into(), record[source].clone());
    }
    for (target, source) in [
        ("tx_channels", "transmitters"),
        ("rx_channels", "receivers"),
    ] {
        if let Some(channels) = record["channels"][source].as_object() {
            view.insert(target.into(), Value::Object(channels.iter().filter_map(|(number, channel)| {
                channel["name"].as_str().filter(|name| !name.is_empty()).map(|name| (number.clone(), serde_json::json!({"name":name,"friendly_name":channel["friendly_name"].as_str().unwrap_or("")})))
            }).collect()));
        }
    }
    if let Some(subscriptions) = record["subscriptions"].as_array() {
        view.insert(
            "subscriptions".into(),
            Value::Array(
                subscriptions
                    .iter()
                    .map(|subscription| {
                        serde_json::json!({
                            "rx_channel_name": subscription["rx_channel"],
                            "tx_device_name": subscription["tx_device"],
                            "tx_channel_name": subscription["tx_channel"],
                            "status": subscription["status"]["label"],
                        })
                    })
                    .collect(),
            ),
        );
    }
    Value::Object(view)
}
