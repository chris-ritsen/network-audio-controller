use super::operations::ControlError;
use super::record::DeviceRecord;
use netaudio_core::capabilities::{AvailabilityRequest, availability};
use netaudio_core::flow_readback::{FlowReadbackRequest, transmit_flow_topology};
use netaudio_core::sample_rate_topology::{
    EvidenceRequest, TopologyImpactRequest, TopologyReadbackRequest, topology_impact,
    verify_topology,
};
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct Operation {
    pub target: u32,
    pub confirm_destructive: bool,
    pub preview: bool,
}

impl Operation {
    pub fn new(parameters: &Value) -> Result<Self, ControlError> {
        let target = parameters["sample_rate"]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value != 0)
            .ok_or_else(|| {
                ControlError::Invalid("sample_rate must be a positive integer".to_owned())
            })?;
        let confirm_destructive = match parameters.get("confirm_destructive") {
            None => false,
            Some(Value::Bool(value)) => *value,
            _ => {
                return Err(ControlError::Invalid(
                    "confirm_destructive must be a boolean".to_owned(),
                ));
            }
        };
        Ok(Self {
            target,
            confirm_destructive,
            preview: false,
        })
    }

    pub fn precondition(&self, record: &Value) -> Value {
        if record["requires_managed_control"] == true {
            return json!({
                "rate": record["sample_rate_hz"],
                "supported": record["supported_sample_rates_hz"],
                "update_mode": record["sample_rate_update_mode"],
                "capability": record["sample_rate_configuration_supported"],
                "capacities": record["sample_rate_channel_capacities"],
                "topology": record["managed_topology"]["snapshot"],
            });
        }
        json!({
            "rate": record["sample_rate_hz"],
            "supported": record["supported_sample_rates_hz"],
            "update_mode": record["sample_rate_update_mode"],
            "capability": record["sample_rate_configuration_supported"],
            "capacities": record["sample_rate_channel_capacities"],
            "managed": record["requires_managed_control"],
            "receivers": record["channels"]["receivers"],
            "transmitters": record["channels"]["transmitters"],
            "subscriptions": record["subscriptions"].as_array().map(|routes| routes.iter().map(|route| json!({
                "number": route["rx_channel_number"],
                "receiver": route["rx_channel"],
                "source": route["tx_device"],
                "channel": route["tx_channel"],
            })).collect::<Vec<_>>()),
            "flows": record["transmitter_flows"],
            "flow_protocol": record["flow_protocol_id"],
        })
    }

    pub fn validate_status(&self, record: &Value) -> Result<Value, ControlError> {
        let status = status(record)?;

        if !status["available_values"]
            .as_array()
            .is_some_and(|values| values.contains(&json!(self.target)))
        {
            return Err(ControlError::Conflict(format!(
                "requested sample rate {} is not supported; device reports {}",
                self.target, status["available_values"]
            )));
        }

        Ok(status)
    }

    pub fn writable(&self, record: &Value) -> Result<(), ControlError> {
        let facts: AvailabilityRequest = serde_json::from_value(json!({
            "operation": "sample_rate",
            "permission": record["managed_operation_permissions"]["sample_rate"],
            "supported": record["sample_rate_configuration_supported"],
            "readable": record["sample_rate_hz"].is_u64(),
            "locked": record["is_locked"],
            "audio": {
                "update_mode": record["sample_rate_update_mode"],
                "available_values": record["supported_sample_rates_hz"],
                "requested_value": self.target,
            },
            "transport_available": true,
            "has_adapter": false,
            "managed": record["requires_managed_control"] == true,
        }))
        .map_err(anyhow::Error::from)?;
        let permission = availability(facts);

        if !permission.write_permitted {
            return Err(ControlError::Conflict(format!(
                "sample-rate control unavailable: {}",
                permission.reasons.join(", ")
            )));
        }

        Ok(())
    }

    pub fn preflight(
        &self,
        record: &Value,
        snapshot: Option<Value>,
    ) -> Result<Value, ControlError> {
        let current = self.validate_status(record)?["current_value"].clone();
        let target_capacity = if snapshot.is_some() {
            capacity(record, self.target)?
        } else {
            Value::Null
        };
        let impact = if let Some(snapshot) = &snapshot {
            let request: TopologyImpactRequest = serde_json::from_value(json!({
                "snapshot": snapshot,
                "target_capacity": target_capacity,
            }))
            .map_err(anyhow::Error::from)?;
            serde_json::to_value(topology_impact(request).map_err(ControlError::Conflict)?)
                .map_err(anyhow::Error::from)?
        } else {
            json!({
                "requires_destructive_confirmation": false,
                "reversible_receiver_clipping": [],
                "destructive_transmitter_membership_loss": [],
                "uncharacterized_transmitter_flows": [],
            })
        };
        Ok(json!({
            "device_name": record["name"],
            "current_sample_rate_hertz": current,
            "target_sample_rate_hertz": self.target,
            "current_topology": snapshot,
            "target_capacity": target_capacity,
            "reversible_receiver_clipping": impact["reversible_receiver_clipping"],
            "destructive_transmitter_membership_loss": impact["destructive_transmitter_membership_loss"],
            "uncharacterized_transmitter_flows": impact["uncharacterized_transmitter_flows"],
            "requires_destructive_confirmation": impact["requires_destructive_confirmation"],
            "is_classified": impact["uncharacterized_transmitter_flows"].as_array().is_some_and(Vec::is_empty),
            "topology_characterized": snapshot.is_some() && !target_capacity.is_null(),
            "capacity_known": !target_capacity.is_null(),
        }))
    }

    pub fn refusal(&self, preflight: &Value) -> Option<(&'static str, &'static str)> {
        if preflight["is_classified"] == false {
            return Some((
                "unsupported",
                "sample-rate contraction affects transmitter flows whose membership transition is not proven",
            ));
        }

        if preflight["requires_destructive_confirmation"] == true && !self.confirm_destructive {
            return Some((
                "confirmation_required",
                if preflight["capacity_known"] == true {
                    "sample-rate change would permanently remove transmitter flow members; explicit confirmation is required"
                } else {
                    "the device did not report its target channel capacity, so the effect on transmitter flows cannot be predicted; explicit confirmation is required"
                },
            ));
        }

        None
    }

    pub fn verify(&self, preflight: &Value, after: &Value) -> Result<(), ControlError> {
        let request: TopologyReadbackRequest = serde_json::from_value(json!({
            "before": preflight["current_topology"],
            "after": after,
            "target_capacity": preflight["target_capacity"],
            "target_sample_rate_hertz": self.target,
        }))
        .map_err(anyhow::Error::from)?;
        verify_topology(request).map_err(ControlError::Conflict)
    }
}

pub fn status(record: &Value) -> Result<Value, ControlError> {
    if record["sample_rate_hz"].is_null() || record["supported_sample_rates_hz"].is_null() {
        return Err(ControlError::ObservationUnavailable(
            "sample-rate readback was unavailable".to_owned(),
        ));
    }

    let request: EvidenceRequest = serde_json::from_value(json!({
        "kind": "status",
        "status": {"current_value": record["sample_rate_hz"], "available_values": record["supported_sample_rates_hz"]},
    })).map_err(|error| ControlError::Conflict(format!("invalid sample-rate readback: {error}")))?;
    serde_json::to_value(request.validate().map_err(ControlError::Conflict)?)
        .map_err(anyhow::Error::from)
        .map_err(ControlError::from)
}

pub fn capacity(record: &Value, rate: u32) -> Result<Value, ControlError> {
    let request: EvidenceRequest = serde_json::from_value(json!({
        "kind": "capacity", "capacities": record["sample_rate_channel_capacities"], "sample_rate_hertz": rate,
    })).map_err(|error| ControlError::Conflict(format!("invalid sample-rate capacities: {error}")))?;
    serde_json::to_value(request.validate().map_err(ControlError::Conflict)?)
        .map_err(anyhow::Error::from)
        .map_err(ControlError::from)
}

pub fn snapshot(record: &DeviceRecord, protocol: u16, rate: u32) -> Result<Value, ControlError> {
    let presentation = record.presentation()?;
    let subscriptions: Vec<_> = record
        .subscriptions
        .iter()
        .filter(|route| {
            ["tx_channel", "tx_device"]
                .iter()
                .any(|key| route[key].as_str().is_some_and(|value| !value.is_empty()))
        })
        .map(|route| {
            json!({
                "receiver_channel_number": route["rx_channel_number"],
                "receiver_channel_name": route["rx_channel"].as_str().unwrap_or_default(),
                "transmitter_channel_name": route["tx_channel"].as_str().unwrap_or_default(),
                "transmitter_device_name": route["tx_device"].as_str().unwrap_or_default(),
            })
        })
        .collect();
    let flows = record.properties["transmitter_flows"]
        .as_array()
        .ok_or_else(|| {
            ControlError::ObservationUnavailable(
                "fresh transmitter-flow inventory is unavailable".to_owned(),
            )
        })?;
    Snapshot {
        capacity: json!({
            "sample_rate_hertz": rate,
            "receive_channel_count": record.properties["rx_count_raw"],
            "transmit_channel_count": record.properties["tx_count_raw"],
        }),
        receiver_numbers: record.receivers.keys().copied().collect(),
        transmitter_numbers: record.transmitters.keys().copied().collect(),
        subscriptions,
        flows,
        protocol,
    }
    .resolve(&presentation, rate)
}

pub struct Snapshot<'a> {
    pub capacity: Value,
    pub receiver_numbers: Vec<u16>,
    pub transmitter_numbers: Vec<u16>,
    pub subscriptions: Vec<Value>,
    pub flows: &'a [Value],
    pub protocol: u16,
}

impl Snapshot<'_> {
    pub fn resolve(self, record: &Value, rate: u32) -> Result<Value, ControlError> {
        let advertised = capacity(record, rate)?;
        if !advertised.is_null() && advertised != self.capacity {
            return Err(ControlError::Conflict(
                "fresh channel counts do not match the expected sample-rate capacity".to_owned(),
            ));
        }
        for (numbers, count) in [
            (
                self.receiver_numbers,
                &self.capacity["receive_channel_count"],
            ),
            (
                self.transmitter_numbers,
                &self.capacity["transmit_channel_count"],
            ),
        ] {
            let request: EvidenceRequest = serde_json::from_value(json!({
                "kind": "receiver_inventory", "channel_numbers": numbers,
                "receive_channel_count": count,
            }))
            .map_err(anyhow::Error::from)?;
            request.validate().map_err(ControlError::Conflict)?;
        }
        let flows = self
            .flows
            .iter()
            .map(|record| {
                transmit_flow_topology(&FlowReadbackRequest {
                    protocol_id: self.protocol,
                    record: record.clone(),
                })
                .map_err(ControlError::Conflict)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({
            "capacity": self.capacity,
            "receiver_subscriptions": self.subscriptions,
            "transmitter_flows": flows,
            "flow_protocol_identifier": self.protocol,
        }))
    }
}

pub fn success(
    preflight: &Value,
    status: &Value,
    topology: Option<&Value>,
    changed: bool,
) -> Value {
    json!({
        "success": true,
        "changed": changed,
        "preflight": preflight,
        "readback": {"sample_rate_hertz": status["current_value"], "supported_sample_rates_hertz": status["available_values"], "topology": topology},
        "state": "confirmed",
        "state_verified": true,
    })
}

pub fn response_status(result: &Value) -> u16 {
    match result["state"].as_str() {
        Some("confirmed" | "preview") => 200,
        Some("unsupported" | "confirmation_required") => 409,
        _ => 502,
    }
}
