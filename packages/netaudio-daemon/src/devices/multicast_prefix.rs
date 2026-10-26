use super::{capabilities, operations::ControlError};
use netaudio_core::capabilities::{SettingsCapabilityFacts, settings_capabilities};
use serde_json::{Value, json};
use std::net::Ipv4Addr;

#[derive(Clone, Debug)]
pub struct Configuration {
    pub prefix: Ipv4Addr,
}

impl Configuration {
    pub fn new(value: &Value) -> Result<Self, ControlError> {
        let prefix = value
            .as_str()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| ControlError::Invalid("prefix must be an IPv4 address".to_owned()))?;
        Ok(Self { prefix })
    }

    pub fn precondition(&self, record: &Value) -> Value {
        json!({
            "prefix": record["aes67_multicast_prefix"],
            "properties": record["settings_properties"],
            "supported": record["aes67_configuration_supported"],
            "managed": record["requires_managed_control"],
        })
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        let availability = capabilities::operation(record, "aes67")?;

        if !availability.write_permitted {
            return Err(ControlError::Conflict(format!(
                "AES67 multicast prefix is not writable: {}",
                availability.reasons.join(", ")
            )));
        }

        let facts = SettingsCapabilityFacts {
            multicast_prefix: record["aes67_multicast_prefix"].as_str().map(str::to_owned),
            properties: record["settings_properties"].as_array().cloned(),
        };

        if !settings_capabilities(facts).aes67_multicast_prefix {
            return Err(ControlError::Conflict(
                "device does not advertise an AES67 multicast prefix".to_owned(),
            ));
        }

        Ok(
            (record["aes67_multicast_prefix"] != self.prefix.to_string()).then(|| {
                json!({
                    "command": "set_aes67_multicast_prefix",
                    "prefix": self.prefix,
                })
            }),
        )
    }

    pub fn completion(&self, observed: Option<&str>, sent: bool, receipt: &Value) -> Value {
        json!({
            "prefix": observed,
            "requested": self.prefix,
            "request_sent": sent,
            "request_acknowledgement": receipt,
            "configuration_confirmed": observed.map(|value| value == self.prefix.to_string()),
            "persistence_confirmed": null,
        })
    }
}
