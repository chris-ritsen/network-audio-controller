use super::capabilities;
use super::operations::ControlError;
use serde_json::{Map, Value, json};

#[derive(Clone, Debug)]
pub struct Configuration {
    pub enabled: bool,
}

impl Configuration {
    pub fn new(value: &Value) -> Result<Self, ControlError> {
        Ok(Self {
            enabled: value
                .as_bool()
                .ok_or_else(|| ControlError::Invalid("enabled must be a boolean".to_owned()))?,
        })
    }

    pub fn precondition(&self, record: &Value) -> Value {
        json!({
            "supported": record["aes67_configuration_supported"],
            "current": record["aes67_current"],
            "configured": record["aes67_configured"],
            "reboot_required": record["aes67_reboot_required"],
            "managed": record["requires_managed_control"],
        })
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        let available = capabilities::operation(record, "aes67")?;

        if !available.write_permitted {
            return Err(ControlError::Conflict(format!(
                "AES67 configuration is not writable: {}",
                available.reasons.join(", ")
            )));
        }

        Ok((record["aes67_configured"].as_bool() != Some(self.enabled))
            .then(|| json!({"command": "enable_aes67", "enabled": self.enabled})))
    }

    pub fn completion(&self, observed: Option<&Map<String, Value>>, sent: bool) -> Value {
        let field = |name: &str| {
            observed
                .and_then(|fields| fields.get(name))
                .cloned()
                .unwrap_or(Value::Null)
        };
        let configured = field("aes67_configured");
        let current = field("aes67_current");
        json!({
            "request_sent": sent,
            "request_acknowledged": null,
            "configuration_confirmed": configured.as_bool().map(|configured| configured == self.enabled),
            "effective_state_confirmed": current.as_bool().map(|current| current == self.enabled),
            "current": current,
            "configured": configured,
            "requested": self.enabled,
            "reboot_required": field("aes67_reboot_required"),
            "persistence_confirmed": null,
        })
    }
}
