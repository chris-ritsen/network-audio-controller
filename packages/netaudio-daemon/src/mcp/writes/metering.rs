use super::{NEXT_STEP, Prepared, PreparedKind, require_record, required_text};
use crate::mcp::service::CallError;
use crate::mcp::views::Records;
use crate::monitoring::metering_streams::MeteringStreams;
use serde_json::{Value, json};
use std::sync::Arc;

pub struct PreparedMetering {
    pub reference: String,
    pub precondition: Value,
    pub start: bool,
}

impl PreparedMetering {
    pub fn apply(
        self,
        streams: &Arc<MeteringStreams>,
        client: &str,
    ) -> Result<(Value, bool), CallError> {
        let client = format!("mcp:{client}");
        let result = if self.start {
            streams.start(&self.reference, &client)
        } else {
            streams.stop(&self.reference, &client)
        };
        result.map_err(|error| CallError::invalid(error.to_string()))?;
        Ok((json!({"success": true}), false))
    }
}

pub fn supports(operation: &str) -> bool {
    matches!(operation, "start_metering" | "stop_metering")
}

pub fn prepare(operation: &str, request: &Value, records: &Records) -> Result<Prepared, CallError> {
    let record = require_record(records, &request["device"])?;
    let reference = required_text(record, "server_name")?.to_owned();
    let start = operation == "start_metering";
    Ok(Prepared {
        preview: json!({
            "changed": false,
            "confirmation_required": true,
            "operation": operation,
            "device": record["name"],
            "effect": if start {
                "Start detailed metering for this MCP client."
            } else {
                "Release detailed metering requested by this MCP client. Other clients retain their subscriptions."
            },
            "next": NEXT_STEP,
        }),
        devices: Vec::new(),
        kind: PreparedKind::Metering(PreparedMetering {
            reference,
            precondition: json!({
                "server_name": record["server_name"],
                "name": record["name"],
                "ipv4": record["ipv4"],
                "mac_address": record["mac_address"],
                "services": record["services"],
            }),
            start,
        }),
    })
}
