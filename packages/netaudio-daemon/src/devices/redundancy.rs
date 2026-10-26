use super::capabilities;
use super::operations::ControlError;
use netaudio_core::network::{
    DanteRedundancyMode, RedundancyConfirmation, RedundancyControlRequest, redundancy_control,
};
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct Configuration {
    pub mode: DanteRedundancyMode,
}

impl Configuration {
    pub fn new(mode: &Value) -> Result<Self, ControlError> {
        Ok(Self {
            mode: serde_json::from_value(mode.clone())
                .map_err(|_| ControlError::Invalid("Invalid redundancy mode".to_owned()))?,
        })
    }

    pub fn precondition(&self, record: &Value) -> Value {
        let state = &record["dante_redundancy"];
        json!({
            "interfaces": record["interfaces"],
            "availability": record["operation_availability"]["redundancy"],
            "current": state["current"],
            "configured": state["configured"],
            "state_fresh": state["state_fresh"],
            "available_modes": state["available_modes"],
            "available_modes_source": state["available_modes_source"],
            "available_modes_fresh": state["available_modes_fresh"],
            "managed": record["requires_managed_control"],
        })
    }

    pub fn matches(&self, state: &Value) -> bool {
        state["configured"] == json!(self.mode) && state["state_fresh"] == true
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        let availability = capabilities::operation(record, "redundancy")?;
        let control = redundancy_control(RedundancyControlRequest {
            state: record["dante_redundancy"].clone(),
            mode: Some(self.mode),
            readback: None,
        });

        if !availability.writable || !control.reasons.is_empty() {
            return Err(ControlError::Conflict(format!(
                "Dante redundancy is not writable: {}",
                availability
                    .reasons
                    .iter()
                    .copied()
                    .chain(control.reasons.iter().copied())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        Ok((!control.configuration_matched).then(|| {
            json!({
                "command": "set_dante_redundancy",
                "mode": self.mode,
                "switch_configuration_choice": control.switch_configuration_choice,
            })
        }))
    }

    pub fn completion(&self, before: &Value, after: Option<&Value>, sent: bool) -> Value {
        let verification = after.map(|after| {
            let request = serde_json::from_value(json!({
                "state": after["dante_redundancy"],
                "mode": self.mode,
                "readback": {
                    "before_mode": before["dante_redundancy"]["current"],
                    "before_interfaces": before["interfaces"],
                    "after_interfaces": after["interfaces"],
                },
            }))
            .map_err(|error| error.to_string())?;
            match redundancy_control(request).readback {
                Some(RedundancyConfirmation::Verified) => Ok(()),
                Some(RedundancyConfirmation::NetworkChanged) => {
                    Err("Other network settings changed during verification".to_owned())
                }
                _ => Err("Configured redundancy did not match".to_owned()),
            }
        });
        let configured = verification.as_ref().map(Result::is_ok);
        let effective = after
            .and_then(|after| after["dante_redundancy"]["current"].as_str())
            .map(|current| json!(current) == json!(self.mode));
        json!({
            "state": if configured == Some(true) {
                if effective == Some(true) { "effective_state_confirmed" } else { "configuration_confirmed" }
            } else { "unverified" },
            "requested_mode": self.mode,
            "mutation_sent": sent,
            "request_sent": sent,
            "request_acknowledgement": null,
            "request_acknowledged": null,
            "device_side_confirmation": null,
            "configuration_confirmed": configured,
            "effective_state_confirmation": effective,
            "effective_state_confirmed": effective,
            "current": after.map(|after| &after["dante_redundancy"]["current"]),
            "configured": after.map(|after| &after["dante_redundancy"]["configured"]),
            "reboot_required": after.map(|after| &after["dante_redundancy"]["reboot_required"]),
            "verification_error": verification.and_then(Result::err),
            "persistence_confirmation": null,
            "reboot_evidence": null,
        })
    }
}
