use super::{CallError, Service};
use crate::mcp::protocol;
use crate::presets;
use serde_json::{Value, json};
use std::collections::BTreeSet;

impl Service {
    pub(super) async fn preview_preset(&self, arguments: &Value) -> Result<Value, CallError> {
        let parsed = match (
            arguments["preset"]
                .as_str()
                .filter(|value| !value.is_empty()),
            arguments["xml"].as_str().filter(|value| !value.is_empty()),
        ) {
            (Some(_), Some(_)) => {
                return Err(CallError::invalid("give either preset or xml, not both"));
            }
            (Some(reference), None) => self.presets.read(reference).await?.parsed,
            (None, Some(_)) => self.presets.parse(arguments).await?,
            (None, None) => {
                return Err(CallError::invalid(
                    "give preset, the name of a saved preset, or xml",
                ));
            }
        };
        let records = self.inventory().await?;
        let identifiers = if let Some(requested) = arguments["devices"]
            .as_array()
            .filter(|values| !values.is_empty())
        {
            let mut identifiers = Vec::new();
            let mut seen = BTreeSet::new();

            for selector in requested {
                let selector = selector
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| CallError::invalid("devices must be named"))?;
                let mut matches = records.iter().filter(|(key, record)| {
                    std::iter::once(key.as_str())
                        .chain(
                            ["name", "server_name", "inventory_id"]
                                .into_iter()
                                .filter_map(|field| record[field].as_str()),
                        )
                        .any(|value| caseless::default_caseless_match_str(value, selector))
                });
                let (identifier, _) = matches.next().ok_or_else(|| {
                    CallError::invalid(format!("no device named {selector:?} is in the inventory"))
                })?;

                if matches.next().is_some() {
                    return Err(CallError::invalid(format!(
                        "device {selector:?} is ambiguous; use its inventory ID"
                    )));
                }

                if !seen.insert(identifier.clone()) {
                    return Err(CallError::invalid("A device may only be selected once."));
                }

                identifiers.push(identifier.clone());
            }

            presets::device_identifiers(&json!(identifiers), true).map_err(CallError::invalid)?
        } else {
            records.keys().cloned().collect()
        };
        let preview = presets::Service::preview_parsed(&parsed, &identifiers, &records)?;
        let devices: Vec<_> = preview["devices"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|entry| {
                let settings: Vec<_> = entry["settings"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|setting| {
                        format!(
                            "{}: {}",
                            setting["label"].as_str().unwrap_or_default(),
                            setting["value"].as_str().unwrap_or_default(),
                        )
                    })
                    .collect();
                let matches: Vec<_> = entry["targets"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|target| target["name"].clone())
                    .collect();
                json!({
                    "preset_device": entry["name"],
                    "settings": settings,
                    "preserved": entry["preserved"],
                    "matches": matches,
                })
            })
            .collect();
        Ok(json!({
            "preset": preview["name"],
            "digest": preview["digest"],
            "devices": devices,
        }))
    }
}

impl From<presets::Error> for CallError {
    fn from(error: presets::Error) -> Self {
        let code = match error {
            presets::Error::Internal(_) => protocol::INTERNAL_ERROR,
            _ => protocol::INVALID_PARAMS,
        };
        Self {
            code,
            message: error.to_string(),
        }
    }
}
