use super::operations::ControlError;
use super::record::DeviceRecord;
use super::transport::{CommandIntent, DeviceTransport, TransportError};
use netaudio_core::flow_readback::{
    FlowReadbackRequest, inventory_complete, transmit_flow_specification,
};
use netaudio_core::inventory::{Inventory, InventoryState};
use netaudio_core::protocol::{
    RESULT_CODE_FRONTEND_UNAVAILABLE, RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS,
    response_envelope,
};
use serde_json::{Value, json};

const MAXIMUM_INVENTORY_PAGES: usize = 256;
const RESULT_UNSUPPORTED_REVISION: u16 = 0x0010;

pub struct TransmitInventory {
    pub protocol: u16,
    pub raw: Value,
    pub canonical: Value,
}

pub fn protocols(record: &DeviceRecord) -> Result<Vec<u16>, ControlError> {
    let protocol = record
        .routing_protocol()?
        .ok_or_else(|| ControlError::Conflict("flow protocol is unknown".to_owned()))?;
    let observed = record
        .properties
        .get("flow_protocol_id")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok());

    match observed {
        Some(observed) => netaudio_core::protocol::transmit_flow_inventory_protocols(
            protocol.protocol_id,
            observed,
        )
        .map_err(|message| ControlError::Conflict(message.to_owned())),
        None => Ok(protocol.flow_query_protocol_ids),
    }
}

pub async fn read_transmit(
    transport: &mut DeviceTransport,
    protocols: &[u16],
) -> Result<TransmitInventory, ControlError> {
    let mut unavailable = Vec::new();

    for &protocol in protocols {
        let mut inventory = Inventory::new("tx_flows", protocol, MAXIMUM_INVENTORY_PAGES)
            .map_err(|message| ControlError::Conflict(message.to_owned()))?;
        let mut pages = 0;

        loop {
            let command = match inventory.state() {
                InventoryState::Complete { inventory, .. } => {
                    let raw = Value::Object(inventory);
                    let canonical = canonical_inventory(&raw, protocol)?;
                    return Ok(TransmitInventory {
                        protocol,
                        raw,
                        canonical,
                    });
                }
                InventoryState::Pending { next_command, .. }
                | InventoryState::Partial { next_command, .. } => Value::Object(next_command),
            };
            let response = match transport.execute(&command, CommandIntent::Read).await {
                Ok(response) => response,
                Err(TransportError::ReadTimeout) if pages == 0 => {
                    unavailable.push(format!("0x{protocol:04x}: no response"));
                    break;
                }
                Err(error) => {
                    return Err(ControlError::Transport {
                        error,
                        acknowledged_commands: 0,
                    });
                }
            };
            let envelope =
                response_envelope(&response).ok_or_else(|| malformed("flow page envelope"))?;

            if !matches!(
                envelope.result_code,
                RESULT_CODE_SUCCESS | RESULT_CODE_MORE_PAGES
            ) {
                if pages == 0
                    && matches!(
                        envelope.result_code,
                        RESULT_CODE_FRONTEND_UNAVAILABLE | RESULT_UNSUPPORTED_REVISION
                    )
                {
                    unavailable.push(format!(
                        "0x{protocol:04x}: unavailable with result {}",
                        envelope.result_code
                    ));
                    break;
                }

                return Err(ControlError::Rejected {
                    result_code: envelope.result_code,
                    acknowledged_commands: 0,
                });
            }

            inventory.accept(&response).map_err(malformed)?;
            pages += 1;
        }
    }

    tracing::warn!(protocols = ?unavailable, "transmitter flow inventory unavailable");
    Err(ControlError::Transport {
        error: TransportError::ReadTimeout,
        acknowledged_commands: 0,
    })
}

fn malformed(message: &'static str) -> ControlError {
    ControlError::Transport {
        error: TransportError::MalformedResponse(message),
        acknowledged_commands: 0,
    }
}

pub(crate) fn canonical_inventory(inventory: &Value, protocol: u16) -> Result<Value, ControlError> {
    if !inventory_complete(inventory.clone()) {
        return Err(ControlError::Conflict(
            "complete flow inventory is unavailable".to_owned(),
        ));
    }

    let records = inventory["flows"]
        .as_array()
        .ok_or_else(|| malformed("transmitter flow inventory"))?;
    let mut specifications = Vec::new();
    let mut errors = Vec::new();

    for (index, record) in records.iter().enumerate() {
        match transmit_flow_specification(&FlowReadbackRequest {
            protocol_id: protocol,
            record: record.clone(),
        }) {
            Ok(specification) => specifications
                .push(serde_json::to_value(specification).map_err(anyhow::Error::from)?),
            Err(error) => {
                errors.push(json!({"record_index": index, "error": error, "raw_record": record}))
            }
        }
    }

    Ok(json!({
        "schema_version": 1,
        "flow_protocol_id": protocol,
        "maximum_flow_slots": inventory["maximum_flow_slots"],
        "reported_flow_count": inventory.get("reported_flow_count").cloned().unwrap_or_else(|| json!(records.len())),
        "flows": specifications,
        "unparsed_records": errors,
    }))
}
