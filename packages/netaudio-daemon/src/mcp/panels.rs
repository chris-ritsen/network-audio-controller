use super::service::CallError;
use crate::devices::operations::ControlOperation;
use crate::devices::panels::{Change, Operation};
use crate::devices::registry::DeviceRegistry;
use serde_json::{Value, json};
use std::sync::Arc;

pub async fn change(
    arguments: &Value,
    registry: &Arc<DeviceRegistry>,
) -> Result<Change, CallError> {
    let parameters = arguments
        .as_object()
        .ok_or_else(|| CallError::invalid("arguments must be an object"))?;
    let mut change =
        Change::parse(parameters).map_err(|error| CallError::invalid(error.to_string()))?;
    match change.category.as_str() {
        "bluetooth_discovery" if !change.requested.is_boolean() => {
            let word = super::views::text(&change.requested).trim().to_lowercase();
            change.requested = match word.as_str() {
                "discoverable" | "visible" | "on" | "true" | "yes" | "enable" | "enabled" | "1" => {
                    json!(true)
                }
                "not discoverable" | "undiscoverable" | "hidden" | "off" | "false" | "no"
                | "disable" | "disabled" | "2" => json!(false),
                _ => {
                    return Err(CallError::invalid(
                        "bluetooth_discovery takes discoverable or not discoverable",
                    ));
                }
            };
        }
        "bluetooth_pairing" => {
            if change.requested.as_str().is_some_and(|value| {
                matches!(
                    value.trim().to_lowercase().as_str(),
                    "clear"
                        | "clear all"
                        | "forget"
                        | "forget all"
                        | "forget phones"
                        | "unpair all"
                )
            }) {
                change.requested = json!("clear");
            }
        }
        "bluetooth_identification" => {
            if let Some(value) = change.requested.as_str() {
                change.requested = if matches!(
                    value.trim().to_lowercase().as_str(),
                    "dante device name" | "dante name" | "device name" | "use dante device name"
                ) {
                    json!({"name_source": 1})
                } else {
                    json!({"name_source": 2, "custom_name": value})
                };
            }
            if let Some(requested) = change.requested.as_object_mut() {
                if requested.get("name_source").is_none() && requested.contains_key("custom_name") {
                    requested.insert("name_source".into(), json!(2));
                }
                if requested.get("name_source") == Some(&json!(1))
                    && !requested.contains_key("custom_name")
                {
                    let device = arguments["device"]
                        .as_str()
                        .ok_or_else(|| CallError::invalid("device must be a string"))?;
                    let observed = registry
                        .apply(device, ControlOperation::Panel(Operation::Inspect))
                        .await
                        .map_err(|error| CallError::invalid(error.to_string()))?
                        .observation
                        .ok_or_else(|| CallError::unavailable("Bluetooth name"))?;
                    let identification = &observed["observations"]["bluetooth_identification"];
                    let name = identification["value"]["custom_name"]
                        .as_str()
                        .filter(|_| identification["fresh"] == true)
                        .ok_or_else(|| {
                            CallError::invalid(
                                "the device's current Bluetooth name could not be read",
                            )
                        })?;
                    requested.insert("custom_name".into(), json!(name));
                }
            }
        }
        _ => {}
    }
    Ok(change)
}
