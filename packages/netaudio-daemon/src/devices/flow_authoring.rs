use super::{operations::ControlError, record::DeviceRecord};
use netaudio_core::flow_plan::{FlowCommandPlan, FlowCreateRequest, FlowDeleteRequest};
use netaudio_core::flow_readback::{self, FlowReadbackRequest};
use netaudio_core::flow_specification::TransmitFlowSpecification;
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub enum Operation {
    Plan(Value),
    PlanDelete(u16),
    Create(Value),
    Delete(u16),
}

pub struct Plan {
    pub native: FlowCommandPlan,
    pub presentation: Value,
}

impl Operation {
    pub fn create(specification: &Value, planning: bool) -> Result<Self, ControlError> {
        let specification: TransmitFlowSpecification =
            serde_json::from_value(specification.clone())
                .map_err(|error| ControlError::Invalid(error.to_string()))?;
        specification.validate().map_err(ControlError::Invalid)?;
        let specification = serde_json::to_value(specification).map_err(anyhow::Error::from)?;
        Ok(if planning {
            Self::Plan(specification)
        } else {
            Self::Create(specification)
        })
    }

    pub fn delete(identifier: &Value) -> Result<Self, ControlError> {
        let identifier = identifier
            .as_u64()
            .and_then(|identifier| u16::try_from(identifier).ok())
            .filter(|identifier| {
                (1..=netaudio_core::commands::MAX_LEGACY_FLOW_ID).contains(identifier)
            })
            .ok_or_else(|| {
                ControlError::Invalid(
                    "flow identifier is outside the transmit-flow inventory range".to_owned(),
                )
            })?;
        Ok(Self::Delete(identifier))
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete(_) | Self::PlanDelete(_))
    }

    pub fn is_planning(&self) -> bool {
        matches!(self, Self::Plan(_) | Self::PlanDelete(_))
    }

    pub fn preview(&self) -> Self {
        match self {
            Self::Create(specification) | Self::Plan(specification) => {
                Self::Plan(specification.clone())
            }
            Self::Delete(identifier) | Self::PlanDelete(identifier) => {
                Self::PlanDelete(*identifier)
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Plan(_) | Self::PlanDelete(_) => "validate_transmit_flow",
            Self::Create(_) => "create_transmit_flow",
            Self::Delete(_) => "delete_transmit_flow",
        }
    }

    pub fn action(&self) -> &'static str {
        match self {
            Self::Plan(_) | Self::Create(_) => "create",
            Self::Delete(_) | Self::PlanDelete(_) => "delete",
        }
    }

    pub fn requested(&self) -> Value {
        match self {
            Self::Plan(specification) | Self::Create(specification) => specification.clone(),
            Self::Delete(identifier) | Self::PlanDelete(identifier) => {
                json!({"flow_id": identifier})
            }
        }
    }

    pub fn precondition(&self, record: &Value) -> Value {
        json!({
            "channels": record["channels"]["transmitters"],
            "capability": record["transmit_flow_authoring_capability_word"],
            "capacity": record["maximum_transmit_flow_channel_slots"],
            "sample_rate_hz": record["sample_rate_hz"],
            "encoding": record["encoding"],
            "aes67_current": record["aes67_current"],
            "managed": record["requires_managed_control"],
            "transmit_flows": record["transmitter_flows"],
        })
    }

    pub fn plan(&self, record: &DeviceRecord) -> Result<Plan, ControlError> {
        let protocol = record
            .routing_protocol()?
            .map(|protocol| protocol.protocol_id);
        let specification = match self {
            Self::Plan(specification) | Self::Create(specification) => specification.clone(),
            Self::Delete(_) | Self::PlanDelete(_) => Value::Null,
        };
        let requested = &specification;
        let properties = &record.properties;
        let mut capabilities = serde_json::Map::new();

        for name in requested["protocol"]["required_capabilities"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            capabilities.insert(
                name.to_owned(),
                properties.get(name).cloned().unwrap_or(Value::Null),
            );
        }

        for (name, field) in [
            (
                "aes67_configuration_supported",
                "aes67_configuration_supported",
            ),
            ("aes67_current", "aes67_current"),
            ("redundancy_supported", "switch_redundancy_supported"),
        ] {
            capabilities.insert(
                name.to_owned(),
                properties.get(field).cloned().unwrap_or(Value::Null),
            );
        }

        capabilities.insert(
            "transmit_performance".to_owned(),
            super::performance::snapshot(&record.presentation()?)?["transmit_flow_performance"]
                .clone(),
        );
        let facts = json!({
            "managed": properties.get("requires_managed_control") == Some(&Value::Bool(true)),
            "locked": properties.get("is_locked"),
            "capability_word": properties.get("transmit_flow_authoring_capability_word"),
            "advertised_protocol": protocol,
            "protocol_version": properties.get("flow_protocol_version").or_else(|| properties.get("protocol_version")),
            "sample_rate": properties.get("sample_rate_hz"),
            "encoding": properties.get("encoding"),
            "channels": record.transmitters.keys().collect::<Vec<_>>(),
            "maximum_flow_channel_slots": properties.get("maximum_transmit_flow_channel_slots"),
            "capabilities": capabilities,
        });
        let native = match self {
            Self::Delete(identifier) | Self::PlanDelete(identifier) => {
                let request: FlowDeleteRequest = serde_json::from_value(
                    json!({"protocol_id": protocol, "flow_id": identifier, "device": facts}),
                )
                .map_err(anyhow::Error::from)?;
                netaudio_core::flow_plan::plan_delete(&request)
            }
            _ => {
                let request: FlowCreateRequest = serde_json::from_value(json!({
                    "protocol_id": protocol,
                    "specification": specification,
                    "device": facts,
                }))
                .map_err(anyhow::Error::from)?;
                netaudio_core::flow_plan::plan_create(&request)
            }
        };
        let state_preconditions = ["sample_rate_hz", "encoding_bits"]
            .into_iter()
            .filter_map(|name| {
                requested
                    .get(name)
                    .filter(|value| !value.is_null())
                    .map(|value| (name.to_owned(), value.clone()))
            })
            .collect::<serde_json::Map<_, _>>();
        let identifier = match self {
            Self::Delete(identifier) | Self::PlanDelete(identifier) => json!(identifier),
            _ => requested["identity"]["global_flow_id"].clone(),
        };
        let presentation = json!({
            "operation": self.action(),
            "state": if native.reasons.is_empty() { "planned" } else { "unsupported" },
            "transport": if facts["managed"] == true { "ddm" } else { "direct" },
            "protocol_id": protocol,
            "serializer_cohort": native.serializer_cohort,
            "supported": native.reasons.is_empty(),
            "reasons": native.reasons,
            "authoring_family": native.authoring_family,
            "specification": specification,
            "flow_id": identifier,
            "command_specification": native.command,
            "wire_authored_fields": native.wire_authored_fields,
            "state_preconditions": state_preconditions,
        });
        Ok(Plan {
            native,
            presentation,
        })
    }

    pub fn preflight(
        &self,
        record: &DeviceRecord,
        protocol: u16,
        inventory: &Value,
    ) -> Result<Value, ControlError> {
        let result = match self {
            Self::Delete(identifier) | Self::PlanDelete(identifier) => {
                let request = serde_json::from_value(json!({
                    "capability_word": record.properties.get("transmit_flow_authoring_capability_word"),
                    "flow_id": identifier,
                    "inventory": inventory,
                    "protocol_id": protocol,
                })).map_err(anyhow::Error::from)?;
                serde_json::to_value(flow_readback::delete_preflight(request))
            }
            Self::Create(specification) | Self::Plan(specification) => {
                let request = serde_json::from_value(json!({
                    "inventory": inventory,
                    "requested_flow_id": specification["identity"]["global_flow_id"],
                }))
                .map_err(anyhow::Error::from)?;
                serde_json::to_value(flow_readback::create_preflight(request))
            }
        };
        result.map_err(|error| ControlError::Internal(error.into()))
    }

    pub fn inspect_readback(
        &self,
        before: &Value,
        after: &Value,
        protocol: u16,
        allocation: Option<u16>,
        authoring_refreshed: bool,
    ) -> Result<Value, ControlError> {
        let (identifier, record, comparison) = match self {
            Self::Delete(identifier) | Self::PlanDelete(identifier) => {
                let record = after["flows"]
                    .as_array()
                    .and_then(|records| {
                        records.iter().find(|record| {
                            record["global_flow_id"]
                                .as_u64()
                                .or_else(|| record["flow_number"].as_u64())
                                == Some(u64::from(*identifier))
                        })
                    })
                    .cloned();
                let comparison = match (find_record(before, *identifier), record.as_ref()) {
                    (Some(before), Some(after)) => {
                        let requested =
                            flow_readback::transmit_flow_specification(&FlowReadbackRequest {
                                protocol_id: protocol,
                                record: before.clone(),
                            })
                            .map_err(ControlError::Conflict)?;
                        let effective =
                            flow_readback::transmit_flow_specification(&FlowReadbackRequest {
                                protocol_id: protocol,
                                record: after.clone(),
                            })
                            .map_err(ControlError::Conflict)?;
                        Some(flow_readback::compare_transmit_flows(
                            &serde_json::from_value(
                                json!({"requested": requested, "effective": effective}),
                            )
                            .map_err(anyhow::Error::from)?,
                        ))
                    }
                    _ => None,
                };
                (Some(*identifier), record, comparison)
            }
            Self::Plan(specification) | Self::Create(specification) => {
                let correlated = specification["identity"]["global_flow_id"]
                    .as_u64()
                    .and_then(|identifier| u16::try_from(identifier).ok())
                    .or(allocation);
                let request = serde_json::from_value(json!({
                    "before": before,
                    "after": after,
                    "requested": specification,
                    "protocol_id": protocol,
                    "correlated_flow_id": correlated,
                }))
                .map_err(anyhow::Error::from)?;
                let candidate =
                    flow_readback::creation_candidate(request).map_err(ControlError::Conflict)?;
                (
                    candidate.flow_id,
                    candidate.record.map(Value::Object),
                    candidate.comparison,
                )
            }
        };
        let verification = flow_readback::verification(
            serde_json::from_value(json!({
                "operation": self.action(),
                "record_present": record.is_some(),
                "comparison": comparison,
                "authoring_refreshed": authoring_refreshed,
            }))
            .map_err(anyhow::Error::from)?,
        );
        let effective = record
            .map(|record| {
                flow_readback::transmit_flow_specification(&FlowReadbackRequest {
                    protocol_id: protocol,
                    record,
                })
            })
            .transpose()
            .map_err(ControlError::Conflict)?;
        let topology = flow_readback::topology_change(serde_json::from_value(json!({
            "before": before, "after": after, "protocol_id": protocol, "target_flow_id": identifier,
        })).map_err(anyhow::Error::from)?).map_err(ControlError::Conflict)?;
        Ok(json!({
            "verification": verification,
            "effective": effective,
            "comparison": comparison,
            "concurrent_topology_activity": topology,
        }))
    }
}

pub fn result(
    operation: &Operation,
    state: &str,
    sent: bool,
    receipt: &Value,
    observation: Option<&Value>,
    message: &str,
    evidence: &[Value],
) -> Value {
    let confirmation = match state {
        "confirmed" | "deleted" => Some(true),
        "inconsistent" => Some(false),
        _ => None,
    };
    json!({
        "operation": operation.action(), "state": state, "transport": "direct",
        "mutation_sent": sent, "request_acknowledgement": receipt,
        "device_confirmation": null, "persistence_confirmation": null,
        "effective_state_confirmation": confirmation,
        "requested": if operation.is_delete() {
            evidence.iter().find_map(|entry| entry.get("selected_specification")).cloned().unwrap_or(Value::Null)
        } else { operation.requested() },
        "effective": observation.map(|observation| &observation["effective"]),
        "comparison": observation.map(|observation| &observation["comparison"]),
        "message": message, "verification_observations": evidence,
        "media_packet_reception_confirmed": false, "clock_lock_confirmed": false, "decoded_audio_confirmed": false,
    })
}

fn find_record(inventory: &Value, identifier: u16) -> Option<&Value> {
    inventory["flows"].as_array()?.iter().find(|record| {
        record["global_flow_id"]
            .as_u64()
            .or_else(|| record["flow_number"].as_u64())
            == Some(u64::from(identifier))
    })
}
