use crate::channel_inventory::ChannelInventory;
use crate::flow_inventory::{FlowDirection, FlowInventory};
use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum InventoryState {
    Pending {
        next_command: Map<String, Value>,
        inventory: (),
    },
    Complete {
        next_command: (),
        inventory: Map<String, Value>,
    },
}

impl InventoryState {
    pub fn pending(command: Map<String, Value>) -> Self {
        Self::Pending {
            next_command: command,
            inventory: (),
        }
    }

    pub fn complete(inventory: Value) -> Self {
        let Value::Object(inventory) = inventory else {
            unreachable!("native inventory parsers return objects")
        };

        Self::Complete {
            next_command: (),
            inventory,
        }
    }
}

pub enum Inventory {
    Channels(ChannelInventory),
    Flows(FlowInventory),
}

impl Inventory {
    pub fn new(kind: &str, protocol_id: u16, maximum_pages: usize) -> Result<Self, &'static str> {
        match kind {
            "rx_channels" => {
                ChannelInventory::new("rx", protocol_id, maximum_pages).map(Self::Channels)
            }
            "tx_channels" => {
                ChannelInventory::new("tx", protocol_id, maximum_pages).map(Self::Channels)
            }
            "tx_flows" => {
                FlowInventory::new(FlowDirection::Transmitter, protocol_id, maximum_pages)
                    .map(Self::Flows)
            }
            "rx_flows" => FlowInventory::new(FlowDirection::Receiver, protocol_id, maximum_pages)
                .map(Self::Flows),
            _ => Err("invalid inventory kind"),
        }
    }

    pub fn accept(&mut self, response: &[u8]) -> Result<(), &'static str> {
        match self {
            Self::Channels(inventory) => inventory.accept(response),
            Self::Flows(inventory) => inventory.accept(response),
        }
    }

    pub fn state(&self) -> InventoryState {
        match self {
            Self::Channels(inventory) => inventory.state(),
            Self::Flows(inventory) => inventory.state(),
        }
    }
}
