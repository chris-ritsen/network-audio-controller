use super::capabilities;
use super::operations::ControlError;
use netaudio_core::network::{
    InterfaceConfigurationRequest, NetworkInterface, verify_interface_configuration,
};
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct Configuration {
    pub interface: NetworkInterface,
    request: Value,
    normalized: Value,
}

impl Configuration {
    pub fn new(parameters: &Value) -> Result<Self, ControlError> {
        let mode = parameters["mode"].as_str().unwrap_or_default();
        let request = match mode {
            "dhcp" => json!({"mode": mode}),
            "static" => json!({
                "mode": mode,
                "ip_address": parameters["ip"],
                "netmask": parameters["netmask"],
                "dns_server": parameters["dns"],
                "gateway": parameters["gateway"],
            }),
            _ => {
                return Err(ControlError::Invalid(
                    "mode must be 'dhcp' or 'static'".to_owned(),
                ));
            }
        };
        let interface = parameters
            .get("interface")
            .cloned()
            .unwrap_or(json!("primary"));
        let interface = serde_json::from_value(interface).map_err(|_| {
            ControlError::Invalid("interface must be 'primary' or 'secondary'".to_owned())
        })?;
        let configuration: InterfaceConfigurationRequest = serde_json::from_value(request.clone())
            .map_err(|error| ControlError::Invalid(error.to_string()))?;
        let normalized = configuration
            .normalize()
            .map_err(|error| ControlError::Invalid(error.to_string()))?;
        Ok(Self {
            interface,
            request,
            normalized: serde_json::to_value(normalized).map_err(anyhow::Error::from)?,
        })
    }

    pub fn requested(&self) -> Value {
        json!({"interface": self.interface, "configuration": self.normalized})
    }

    pub fn selected<'a>(&self, interfaces: &'a Value) -> Option<&'a Value> {
        let identity = json!(self.interface);
        let mut entries = interfaces
            .as_array()?
            .iter()
            .filter(|entry| entry["interface"] == identity);
        let selected = entries.next()?;
        entries.next().is_none().then_some(selected)
    }

    pub fn matches(&self, value: &Value) -> bool {
        self.normalized
            .as_object()
            .expect("normalized interface object")
            .iter()
            .all(|(name, expected)| value.get(name) == Some(expected))
    }

    pub fn precondition(&self, record: &Value) -> Value {
        let redundancy = &record["dante_redundancy"];
        json!({
            "interfaces": record["interfaces"],
            "supported": record["static_ipv4_configuration_supported"],
            "availability": record["operation_availability"]["static_ipv4"],
            "redundancy_current": redundancy["current"],
            "redundancy_configured": redundancy["configured"],
            "managed": record["requires_managed_control"],
        })
    }

    pub fn command(&self, record: &Value) -> Result<Option<Value>, ControlError> {
        let network = capabilities::network(record)?;
        let name = json!(self.interface);
        let name = name.as_str().expect("interface enum string");
        let mode = self.request["mode"]
            .as_str()
            .expect("validated network mode");
        let available = capabilities::operation(record, "static_ipv4")?;

        if !available.writable {
            return Err(ControlError::Conflict(format!(
                "network configuration is not writable: {}",
                available.reasons.join(", ")
            )));
        }

        if !network["interface_configuration_modes"][name]
            .as_array()
            .is_some_and(|modes| modes.iter().any(|value| value == mode))
        {
            return Err(ControlError::Conflict(format!(
                "{mode} configuration is unavailable for the {name} interface"
            )));
        }

        let selected = self.selected(&record["interfaces"]).ok_or_else(|| {
            ControlError::Conflict("interface identity is unavailable or ambiguous".to_owned())
        })?;

        if self.matches(&selected["configured"]) {
            return Ok(None);
        }

        Ok(Some(if mode == "dhcp" {
            json!({"command": "set_interface_dhcp", "interface": self.interface})
        } else {
            json!({
                "command": "set_interface_static",
                "interface": self.interface,
                "ip": self.normalized["ip_address"],
                "netmask": self.normalized["netmask"],
                "dns": self.normalized["dns_server"],
                "gateway": self.normalized["gateway"],
            })
        }))
    }

    pub fn completion(&self, before: &Value, after: Option<&Value>, sent: bool) -> Value {
        let selected = after.and_then(|record| self.selected(&record["interfaces"]));
        let current = selected.map(|selected| {
            json!({
                "mode": selected["mode"],
                "ip_address": selected["ip_address"],
                "netmask": selected["netmask"],
                "dns_server": selected["dns_server"],
                "gateway": selected["gateway"],
            })
        });
        let effective = current.as_ref().and_then(|current| {
            let expected = self
                .normalized
                .as_object()
                .expect("normalized interface object");

            if expected.keys().any(|name| current[name].is_null()) {
                return None;
            }

            Some(self.matches(current))
        });
        let verification = after.map(|after| {
            let request = serde_json::from_value(json!({
                "configuration": self.request,
                "interface": self.interface,
                "before": before["interfaces"],
                "after": after["interfaces"],
                "before_redundancy": before["dante_redundancy"],
                "after_redundancy": after["dante_redundancy"],
            }))
            .map_err(|error| error.to_string())?;
            verify_interface_configuration(request).map_err(|error| error.to_string())
        });
        json!({
            "request_sent": sent,
            "request_acknowledged": null,
            "configuration_confirmed": verification.as_ref().map(Result::is_ok),
            "effective_state_confirmed": effective,
            "requested": self.requested(),
            "configured": selected.map(|selected| &selected["configured"]),
            "current": current,
            "reboot_required": selected.map(|selected| &selected["reboot_required"]),
            "verification_error": verification.and_then(Result::err),
            "persistence_confirmed": null,
        })
    }
}
