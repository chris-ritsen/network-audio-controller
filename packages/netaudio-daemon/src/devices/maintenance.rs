use super::operations::ControlError;
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub enum Operation {
    Reboot,
    FactoryReset,
    ClearConfiguration {
        preserve_network: bool,
        reboot: bool,
    },
}

impl Operation {
    pub fn new(name: &str, parameters: &Value) -> Result<Self, ControlError> {
        match name {
            "reboot" => Ok(Self::Reboot),
            "factory_reset" => Ok(Self::FactoryReset),
            "clear_configuration" => {
                let preserve_network = match parameters["mode"].as_str() {
                    Some("all") => false,
                    Some("keep_network") => true,
                    _ => {
                        return Err(ControlError::Invalid(
                            "mode must be 'all' or 'keep_network'".to_owned(),
                        ));
                    }
                };
                let reboot = match parameters.get("reboot") {
                    None => false,
                    Some(Value::Bool(reboot)) => *reboot,
                    _ => {
                        return Err(ControlError::Invalid("reboot must be a boolean".to_owned()));
                    }
                };
                Ok(Self::ClearConfiguration {
                    preserve_network,
                    reboot,
                })
            }
            _ => Err(ControlError::Invalid(
                "unknown maintenance operation".to_owned(),
            )),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Reboot => "reboot",
            Self::FactoryReset => "factory_reset",
            Self::ClearConfiguration { .. } => "clear_configuration",
        }
    }

    pub fn command_name(&self) -> &'static str {
        match self {
            Self::ClearConfiguration {
                preserve_network: true,
                ..
            } => "clear_all_configuration_preserving_internet_protocol_settings",
            Self::ClearConfiguration { .. } => "clear_all_configuration",
            _ => self.name(),
        }
    }

    pub fn requested(&self) -> Value {
        match self {
            Self::ClearConfiguration {
                preserve_network,
                reboot,
            } => json!({
                "mode": if *preserve_network { "keep_network" } else { "all" },
                "reboot": reboot,
            }),
            _ => json!({}),
        }
    }

    pub fn validate(&self, record: &Value) -> Result<(), ControlError> {
        if record["online"] != true {
            return Err(ControlError::Unavailable("device is offline".to_owned()));
        }

        if record["requires_managed_control"] == true {
            return Err(ControlError::Conflict(
                "maintenance requires an available managed-control transport".to_owned(),
            ));
        }

        match record["is_locked"].as_bool() {
            Some(false) => Ok(()),
            Some(true) => Err(ControlError::Conflict("device is locked".to_owned())),
            None => Err(ControlError::ObservationUnavailable(
                "device lock state is unknown; no maintenance command was sent".to_owned(),
            )),
        }
    }

    pub fn effect(&self) -> &'static str {
        match self {
            Self::Reboot => "Audio stops while the device restarts; its address may change.",
            Self::FactoryReset => {
                "Restores factory settings and can interrupt audio and network access."
            }
            Self::ClearConfiguration {
                preserve_network: true,
                ..
            } => {
                "Clears device configuration while preserving network settings. Some devices apply the clear at their next reboot."
            }
            Self::ClearConfiguration { .. } => {
                "Clears device configuration, including network settings. Some devices apply the clear at their next reboot."
            }
        }
    }
}

pub fn response_status(result: &Value) -> u16 {
    if result.get("error").is_some() {
        502
    } else if result["cleared"] == true {
        200
    } else {
        202
    }
}
