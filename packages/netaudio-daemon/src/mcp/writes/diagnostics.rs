use super::{NEXT_STEP, Prepared, PreparedKind, require_record, required_text};
use crate::devices::registry::{DeviceRegistry, DiagnosticOptions, diagnostic_precondition};
use crate::mcp::service::CallError;
use crate::mcp::views::Records;
use serde_json::{Value, json};

pub struct PreparedDiagnostics {
    pub reference: String,
    pub precondition: Value,
    options: DiagnosticOptions,
}

impl PreparedDiagnostics {
    pub fn apply(self, registry: &DeviceRegistry) -> Result<(Value, bool), CallError> {
        let result = registry
            .diagnostics_snapshot_conditionally(
                &self.reference,
                self.options,
                Some(&self.precondition),
            )
            .map_err(|error| CallError::invalid(error.to_string()))?;
        Ok((result, false))
    }
}

pub fn supports(operation: &str) -> bool {
    matches!(
        operation,
        "reset_device_diagnostics" | "set_clock_warning_policy"
    )
}

pub fn prepare(operation: &str, request: &Value, records: &Records) -> Result<Prepared, CallError> {
    let record = require_record(records, &request["device"])?;
    let reference = required_text(record, "server_name")?.to_owned();
    let device = required_text(record, "name")?;
    let reset = operation == "reset_device_diagnostics";
    let warning = if reset {
        None
    } else {
        Some(
            request["clock_variation_warnings"]
                .as_bool()
                .ok_or_else(|| CallError::invalid("clock_variation_warnings must be a boolean"))?,
        )
    };
    let mut preview = json!({
        "changed": false,
        "confirmation_required": true,
        "operation": operation,
        "device": device,
        "next": NEXT_STEP,
    });

    if reset {
        preview["effect"] = json!(format!(
            "NetAudio resets its diagnostic baseline for {device}; retained measurements remain available."
        ));
    } else {
        preview["change"] = json!({
            "clock_variation_warnings": {
                "from": record["clock_observations"]["warning_enabled"],
                "to": warning,
            },
        });
    }

    Ok(Prepared {
        preview,
        devices: Vec::new(),
        kind: PreparedKind::Diagnostics(PreparedDiagnostics {
            reference,
            precondition: diagnostic_precondition(record),
            options: DiagnosticOptions {
                reset,
                warning,
                ..DiagnosticOptions::default()
            },
        }),
    })
}
