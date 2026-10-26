use super::{CallError, NEXT_STEP, Prepared, PreparedKind};
use crate::shure::Registry;
use serde_json::{Value, json};

pub(super) struct PreparedWireless {
    arguments: Value,
    pub precondition: Value,
}

pub(super) fn prepare(arguments: &Value, registry: &Registry) -> Result<Prepared, CallError> {
    let (change, precondition) = registry
        .prepare(arguments)
        .map_err(|error| CallError::invalid(error.to_string()))?;
    let target = change.channel.map_or_else(
        || change.device.clone(),
        |channel| {
            format!(
                "{} ch{channel} {}",
                change.device,
                change.channel_name.as_deref().unwrap_or("")
            )
        },
    );
    let channel = change
        .channel
        .map_or(String::new(), |channel| format!("{channel} "));
    let mut preview = json!({"operation": "set_wireless_value", "preview": true, "change": {
        "target": target.trim(), "setting": change.key, "from": change.current_described, "to": change.requested_described,
        "command": format!("SET {channel}{} {}", change.key, change.value),
    }, "next": NEXT_STEP});
    if change.current_described == change.requested_described {
        preview["note"] = json!("already so; nothing would change");
    }
    Ok(Prepared {
        preview,
        devices: Vec::new(),
        kind: PreparedKind::Wireless(PreparedWireless {
            arguments: arguments.clone(),
            precondition,
        }),
    })
}

impl PreparedWireless {
    pub async fn apply(self, registry: &Registry) -> Result<(Value, bool), CallError> {
        let result = registry
            .execute(self.arguments, Some(self.precondition))
            .await?;
        Ok((result.value, result.status >= 400))
    }
}
