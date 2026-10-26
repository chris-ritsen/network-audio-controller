use super::operations::ControlError;
use super::record::DeviceRecord;
use anyhow::{Context, Result};
use netaudio_core::performance_configuration::{
    PerformanceCompletionKind, PerformanceCompletionRequest, PerformanceCompletionState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum PerformanceOperation {
    ReceiveFlowPerformance {
        latency_microseconds: u64,
        frames_per_packet: u16,
    },
    TransmitFlowPerformance {
        latency_microseconds: u64,
        frames_per_packet: u16,
    },
    UnicastPerformance {
        latency_microseconds: u64,
        frames_per_packet: u16,
    },
    ReceiveFlowDefaultSlots {
        default_slots: u16,
    },
    StoreCurrentConfiguration,
}

pub struct PerformancePlan {
    pub command: Value,
    pub requested: BTreeMap<u16, u32>,
    pub protocol: u16,
    pub storage: bool,
}

impl PerformanceOperation {
    pub fn name(&self) -> &'static str {
        match self {
            Self::ReceiveFlowPerformance { .. } => "receive_flow_performance",
            Self::TransmitFlowPerformance { .. } => "transmit_flow_performance",
            Self::UnicastPerformance { .. } => "unicast_performance",
            Self::ReceiveFlowDefaultSlots { .. } => "receive_flow_default_slots",
            Self::StoreCurrentConfiguration => "store_current_configuration",
        }
    }

    pub fn plan(&self, record: &DeviceRecord) -> Result<PerformancePlan, ControlError> {
        let capabilities = capabilities(record)?;
        let available = &capabilities["operations"][self.name()];

        if available["writable"] != true {
            let reasons = available["reasons"]
                .as_array()
                .context("performance availability has no reasons")?
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ControlError::Conflict(format!(
                "{} is unavailable: {reasons}",
                self.name()
            )));
        }

        let protocol = record
            .routing_protocol()?
            .context("device has no observed ARC protocol identifier")?
            .protocol_id;
        let storage = matches!(self, Self::StoreCurrentConfiguration);
        let mut command = serde_json::to_value(self).map_err(anyhow::Error::from)?;
        command
            .as_object_mut()
            .context("performance request is not an object")?
            .remove("operation");
        command["command"] = if storage {
            json!(self.name())
        } else {
            json!(format!("set_{}", self.name()))
        };
        command["negotiated_protocol_id"] = json!(protocol);

        if !storage {
            command["supported_property_ids"] = capabilities["supported_property_ids"].clone();
        }

        if matches!(
            self,
            Self::ReceiveFlowPerformance { .. } | Self::UnicastPerformance { .. }
        ) {
            let version = capabilities["platform_software_version"]
                .as_array()
                .context("platform software version is unavailable")?;
            command["platform_software_version"] =
                json!(version.iter().take(3).collect::<Vec<_>>());
        }

        let requested = if storage {
            netaudio_core::spec::build_command_from_json(&command.to_string())
                .map_err(|error| ControlError::Invalid(error.to_string()))?;
            BTreeMap::new()
        } else {
            netaudio_core::spec::plan_performance_command(&command.to_string())
                .map_err(|error| ControlError::Invalid(error.to_string()))?
                .into_iter()
                .map(|property| (property.property_id, property.value))
                .collect()
        };
        Ok(PerformancePlan {
            command,
            requested,
            protocol,
            storage,
        })
    }
}

impl PerformancePlan {
    pub fn readback_command(&self) -> Option<Value> {
        (!self.storage).then(|| {
            json!({
                "command": "query_performance_settings",
                "negotiated_protocol_id": self.protocol,
                "property_ids": self.requested.keys().collect::<Vec<_>>(),
            })
        })
    }

    pub fn completion(
        &self,
        acknowledgement: Option<Vec<u8>>,
        readback: Option<Vec<u8>>,
    ) -> Result<Value> {
        let receipt = acknowledgement
            .as_deref()
            .map(netaudio_core::responses::command_receipt);
        let completion =
            netaudio_core::performance_configuration::completion(PerformanceCompletionRequest {
                kind: if self.storage {
                    PerformanceCompletionKind::Storage
                } else {
                    PerformanceCompletionKind::Configuration
                },
                requested: self.requested.clone(),
                acknowledgement,
                readback,
            })
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let observations = if self.storage {
            Vec::new()
        } else {
            vec![json!({
                "phase": "readback",
                "outcome": completion.readback_outcome,
                "missing_property_ids": completion.missing_property_ids.iter()
                    .map(|property| format!("0x{property:04x}")).collect::<Vec<_>>(),
                "mismatched_properties": serialized_properties(&completion.mismatched_properties),
            })]
        };
        let mut result = json!({
            "operation": self.command["command"],
            "state": completion.state,
            "requested_properties": serialized_properties(&self.requested),
            "effective_properties": serialized_properties(&completion.effective_properties),
            "request_acknowledgement": if self.storage { None } else { receipt.as_ref() },
            "device_confirmation": null,
            "effective_state_confirmation": completion.effective_state_confirmation,
            "persistence_request_acknowledgement": if self.storage { receipt.as_ref() } else { None },
            "persistence_confirmation": completion.persistence_confirmation,
            "message": completion.message,
            "verification_observations": observations,
        });

        if matches!(
            completion.state,
            PerformanceCompletionState::Rejected
                | PerformanceCompletionState::Contradicted
                | PerformanceCompletionState::Unverified
        ) {
            result["error"] = result["message"].clone();
        }

        Ok(result)
    }
}

pub fn capabilities(record: &DeviceRecord) -> Result<Value> {
    let properties = record
        .properties
        .get("settings_properties")
        .and_then(Value::as_array)
        .map(|properties| {
            properties
                .iter()
                .map(|property| property["property_id"].clone())
                .collect::<Vec<_>>()
        });
    let facts = serde_json::from_value(json!({
        "protocol_id": record.routing_protocol()?.map(|protocol| protocol.protocol_id),
        "managed": record.properties.get("requires_managed_control").and_then(Value::as_bool) == Some(true),
        "property_ids": properties,
        "platform_software_version": record.properties.get("platform_software_version"),
    }))?;
    serde_json::to_value(
        netaudio_core::performance_configuration::capabilities(facts)
            .map_err(|error| anyhow::anyhow!("{error}"))?,
    )
    .context("serialize performance capabilities")
}

fn serialized_properties(properties: &BTreeMap<u16, u32>) -> BTreeMap<String, u32> {
    properties
        .iter()
        .map(|(property, value)| (format!("0x{property:04x}"), *value))
        .collect()
}

pub fn snapshot(record: &Value) -> Result<Value> {
    let values = record["performance_settings"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let properties: Vec<_> = values
        .keys()
        .filter_map(|key| key.parse::<u16>().ok())
        .collect();
    let facts = serde_json::from_value(json!({"property_ids": properties, "values": values}))?;
    Ok(serde_json::to_value(
        netaudio_core::performance_configuration::snapshot(facts),
    )?)
}
