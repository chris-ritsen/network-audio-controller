use std::collections::HashSet;

use crate::inventory::InventoryState;
use serde_json::{json, Value};

use crate::commands::{PROTOCOL_DANTE_FLOW, PROTOCOL_DANTE_FLOW_2801};
use crate::protocol::{
    is_modern_arc_protocol, response_envelope, RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS,
};
use crate::responses::{
    parse_modern_arc_receiver_flow_status_page, parse_receiver_flow_page,
    parse_transmitter_flow_status_page, parse_tx_flow_page, FIXED_RECEIVER_FLOW_PROTOCOL_IDS,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FlowDirection {
    Receiver,
    Transmitter,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ReceiverFlowFamily {
    Fixed,
    Segmented,
}

struct FlowPage {
    capacity: u8,
    identifiers: Vec<u16>,
    records: Vec<Value>,
    receiver_page: Option<Value>,
}

pub struct FlowInventory {
    direction: FlowDirection,
    receiver_family: ReceiverFlowFamily,
    protocol_id: u16,
    observed_protocol_id: Option<u16>,
    maximum_pages: usize,
    pages: usize,
    starting_flow: u16,
    capacity: Option<u8>,
    identifiers: HashSet<u16>,
    records: Vec<Value>,
    receiver_pages: Vec<Value>,
    raw_pages: Vec<Vec<u8>>,
    complete: Option<Value>,
    observed_opcode: Option<u16>,
}

impl FlowInventory {
    pub fn new(
        direction: FlowDirection,
        protocol_id: u16,
        maximum_pages: usize,
    ) -> Result<Self, &'static str> {
        let supported_legacy = protocol_id == PROTOCOL_DANTE_FLOW
            || (direction == FlowDirection::Transmitter && protocol_id == PROTOCOL_DANTE_FLOW_2801);

        if !supported_legacy && !is_modern_arc_protocol(protocol_id) {
            return Err("unsupported flow inventory protocol");
        }

        let receiver_family = if is_modern_arc_protocol(protocol_id) {
            ReceiverFlowFamily::Segmented
        } else {
            ReceiverFlowFamily::Fixed
        };

        Self::with_receiver_family(direction, receiver_family, protocol_id, maximum_pages)
    }

    pub fn new_receiver(
        family: ReceiverFlowFamily,
        protocol_id: u16,
        maximum_pages: usize,
    ) -> Result<Self, &'static str> {
        let supported = match family {
            ReceiverFlowFamily::Fixed => FIXED_RECEIVER_FLOW_PROTOCOL_IDS.contains(&protocol_id),
            ReceiverFlowFamily::Segmented => is_modern_arc_protocol(protocol_id),
        };

        if !supported {
            return Err("unsupported flow inventory protocol");
        }

        Self::with_receiver_family(FlowDirection::Receiver, family, protocol_id, maximum_pages)
    }

    fn with_receiver_family(
        direction: FlowDirection,
        receiver_family: ReceiverFlowFamily,
        protocol_id: u16,
        maximum_pages: usize,
    ) -> Result<Self, &'static str> {
        if maximum_pages == 0 || maximum_pages > 256 {
            return Err("flow inventory page limit must be between 1 and 256");
        }

        Ok(Self {
            direction,
            receiver_family,
            protocol_id,
            observed_protocol_id: None,
            maximum_pages,
            pages: 0,
            starting_flow: 1,
            capacity: None,
            identifiers: HashSet::new(),
            records: Vec::new(),
            receiver_pages: Vec::new(),
            raw_pages: Vec::new(),
            complete: None,
            observed_opcode: None,
        })
    }

    pub fn accept(&mut self, response: &[u8]) -> Result<(), &'static str> {
        if self.complete.is_some() {
            return Err("flow inventory is already complete");
        }

        if self.pages >= self.maximum_pages {
            return Err("flow inventory exceeded its page limit");
        }

        let envelope = response_envelope(response).ok_or("malformed flow page envelope")?;

        if self.fixed_receiver() {
            if !FIXED_RECEIVER_FLOW_PROTOCOL_IDS.contains(&envelope.protocol_id)
                || envelope.protocol_id > self.protocol_id
                || self
                    .observed_protocol_id
                    .is_some_and(|protocol_id| protocol_id != envelope.protocol_id)
            {
                return Err("flow page protocol changed during pagination");
            }
        } else if envelope.protocol_id != self.protocol_id {
            return Err("flow page protocol changed during pagination");
        }

        if self
            .observed_opcode
            .is_some_and(|opcode| opcode != envelope.opcode)
        {
            return Err("flow inventory layout changed during pagination");
        }

        if !matches!(
            envelope.result_code,
            RESULT_CODE_SUCCESS | RESULT_CODE_MORE_PAGES
        ) {
            return Err("device rejected flow inventory request");
        }

        if self.direction == FlowDirection::Transmitter
            && envelope.opcode == crate::commands::OPCODE_QUERY_TX_FLOWS_2809
        {
            let page = parse_transmitter_flow_status_page(response)
                .ok_or("malformed transmitter flow status page")?;
            self.complete = Some(json!(page));
            self.pages += 1;

            return Ok(());
        }

        let page = self.parse_page(response)?;

        if self
            .capacity
            .is_some_and(|capacity| capacity != page.capacity)
        {
            return Err("flow page capacity changed during pagination");
        }

        let mut numbers = HashSet::with_capacity(page.identifiers.len());

        let bounded_by_capacity = self.direction == FlowDirection::Transmitter;

        for number in &page.identifiers {
            if *number == 0
                || (bounded_by_capacity && *number > u16::from(page.capacity))
                || (self.direction == FlowDirection::Receiver && *number < self.starting_flow)
                || self.identifiers.contains(number)
                || !numbers.insert(*number)
            {
                return Err("flow page has an invalid or repeated flow identifier");
            }
        }

        let next = match envelope.result_code {
            RESULT_CODE_SUCCESS => None,
            RESULT_CODE_MORE_PAGES => {
                let next = numbers
                    .iter()
                    .max()
                    .ok_or("flow page made no progress")?
                    .checked_add(1)
                    .ok_or("flow identifier overflow")?;

                if next <= self.starting_flow
                    || (bounded_by_capacity && next > u16::from(page.capacity))
                {
                    return Err("flow page made no progress");
                }

                Some(next)
            }
            _ => unreachable!("result code checked before parsing"),
        };

        // Commit only after validating the entire response and continuation.
        self.capacity = Some(page.capacity);
        self.observed_opcode = Some(envelope.opcode);
        self.observed_protocol_id = Some(envelope.protocol_id);
        self.identifiers.extend(numbers);
        self.records.extend(page.records);
        self.pages += 1;

        self.raw_pages.push(response.to_vec());

        if let Some(receiver_page) = page.receiver_page {
            self.receiver_pages.push(receiver_page);
        }

        if let Some(next) = next {
            self.starting_flow = next;
        } else if let Some(mut final_page) = self.receiver_pages.last().cloned() {
            final_page["reported_flow_count"] = json!(self.records.len());
            final_page["flows"] = json!(self.records);
            final_page["pages"] = json!(self.receiver_pages);
            final_page["raw_pages"] = json!(self.raw_pages);
            final_page["complete"] = json!(true);
            self.complete = Some(final_page);
        } else {
            self.complete = Some(json!({
                "maximum_flow_slots": page.capacity,
                "reported_flow_count": self.records.len(),
                "flows": self.records,
            }));
        }

        Ok(())
    }

    fn parse_page(&self, response: &[u8]) -> Result<FlowPage, &'static str> {
        if self.direction == FlowDirection::Transmitter {
            let page = parse_tx_flow_page(response).ok_or("malformed transmitter flow page")?;

            return Ok(FlowPage {
                capacity: page.max_flow_slots,
                identifiers: page.flows.iter().map(|flow| flow.flow_number).collect(),
                records: page.flows.iter().map(|flow| json!(flow)).collect(),
                receiver_page: None,
            });
        }

        if self.receiver_family == ReceiverFlowFamily::Segmented {
            let page = parse_modern_arc_receiver_flow_status_page(response)
                .ok_or("malformed receiver flow status page")?;

            return Ok(FlowPage {
                capacity: page.maximum_flow_slots,
                identifiers: page.flows.iter().map(|flow| flow.flow_number).collect(),
                records: page.flows.iter().map(|flow| json!(flow)).collect(),
                receiver_page: Some(json!(page)),
            });
        }

        let page = parse_receiver_flow_page(response).ok_or("malformed receiver flow page")?;

        Ok(FlowPage {
            capacity: page.maximum_flow_slots,
            identifiers: page.flows.iter().map(|flow| flow.flow_number).collect(),
            records: page.flows.iter().map(|flow| json!(flow)).collect(),
            receiver_page: Some(json!(page)),
        })
    }

    fn fixed_receiver(&self) -> bool {
        self.direction == FlowDirection::Receiver
            && self.receiver_family == ReceiverFlowFamily::Fixed
    }

    pub fn state(&self) -> InventoryState {
        if let Some(inventory) = &self.complete {
            return InventoryState::complete(inventory.clone());
        }

        let (name, protocol_field) = match self.direction {
            FlowDirection::Transmitter => ("query_tx_flows", Some("flow_protocol_id")),
            FlowDirection::Receiver if self.receiver_family == ReceiverFlowFamily::Segmented => {
                ("query_modern_arc_receiver_flow_status", Some("protocol_id"))
            }
            FlowDirection::Receiver => ("query_receiver_flows", Some("protocol_id")),
        };
        let mut command = serde_json::Map::from_iter([
            ("command".into(), json!(name)),
            ("starting_flow".into(), json!(self.starting_flow)),
        ]);

        if let Some(field) = protocol_field {
            command.insert(field.into(), json!(self.protocol_id));
        }

        if self.direction == FlowDirection::Transmitter
            && self.observed_opcode == Some(crate::commands::OPCODE_QUERY_TX_FLOWS)
        {
            command.insert("inventory_layout".into(), json!("fixed"));
        }

        if self.direction == FlowDirection::Receiver && self.pages > 0 {
            InventoryState::Partial {
                next_command: command,
                inventory: (),
                partial_inventory: serde_json::Map::from_iter([
                    ("complete".into(), json!(false)),
                    ("flows".into(), json!(self.records)),
                    ("pages".into(), json!(self.receiver_pages)),
                    ("raw_pages".into(), json!(self.raw_pages)),
                ]),
            }
        } else {
            InventoryState::pending(command)
        }
    }
}
