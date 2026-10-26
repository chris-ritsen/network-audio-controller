use std::net::SocketAddrV4;

use crate::commands;
use crate::protocol::{RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS};
use crate::responses::{self, TxFlow};

use super::endpoint_control::{ARC_REVISION, SAMPLE_BITS, SAMPLE_RATE};
use super::source_control::{Error, Handle, Header};

const FIRST_FLOW_IDENTIFIER: u16 = 1;
const MAXIMUM_FLOW_IDENTIFIER: u16 = 32;

pub struct Inventory {
    protocol_revision: u16,
    next: Option<u16>,
    flows: Vec<TxFlow>,
}

impl Default for Inventory {
    fn default() -> Self {
        Self {
            protocol_revision: ARC_REVISION,
            next: Some(FIRST_FLOW_IDENTIFIER),
            flows: Vec::new(),
        }
    }
}

impl Inventory {
    pub fn new(protocol_revision: u16) -> Self {
        Self { protocol_revision, ..Self::default() }
    }

    pub fn request(&self, transaction: u16) -> Result<Vec<u8>, Error> {
        commands::build_query_fixed_tx_flows_from(
            self.protocol_revision,
            self.next.ok_or(Error("source flow inventory is complete"))?,
            transaction,
        )
        .map_err(|_| Error("source flow query could not be encoded"))
    }

    pub fn accept(&mut self, bytes: &[u8]) -> Result<bool, Error> {
        let header = Header::decode(bytes)?;
        let page = responses::parse_tx_flow_page(bytes)
            .ok_or(Error("source flow inventory has an unsupported or incomplete layout"))?;
        let first = self.next.ok_or(Error("source flow inventory is already complete"))?;
        let mut highest = None;

        for flow in page.flows {
            if flow.flow_number < first
                || flow.flow_number > MAXIMUM_FLOW_IDENTIFIER
                || highest.is_some_and(|previous| flow.flow_number <= previous)
            {
                return Err(Error("source flow page repeated or reversed its cursor"));
            }

            highest = Some(flow.flow_number);
            self.flows.push(flow);
        }

        self.next = match header.result {
            RESULT_CODE_SUCCESS => None,
            RESULT_CODE_MORE_PAGES => {
                let next = highest
                    .and_then(|highest| highest.checked_add(1))
                    .filter(|next| *next <= MAXIMUM_FLOW_IDENTIFIER)
                    .ok_or(Error("partial source flow inventory made no progress"))?;
                Some(next)
            }
            _ => return Err(Error("source flow query was rejected")),
        };

        Ok(self.next.is_none())
    }

    pub fn confirm(
        &self,
        handle: Handle,
        destination: SocketAddrV4,
        channels: &[u16],
        frames_per_packet: u16,
    ) -> Result<(), Error> {
        if self.next.is_some() {
            return Err(Error("source flow inventory is incomplete"));
        }

        let bytes = handle.bytes();
        let identifier = u16::from_be_bytes([bytes[0], bytes[1]]);

        if identifier == 0 || bytes[2..] == [0; 4] {
            return Err(Error("source allocation handle has no current resource owner"));
        }

        let flow = self
            .flows
            .iter()
            .find(|flow| flow.flow_number == identifier)
            .ok_or(Error("owned audio allocation is absent from source inventory"))?;
        let expected_destination = serde_json::json!({
            "address": destination.ip(),
            "port": destination.port(),
            "interface": null,
        });

        if flow.sample_rate != SAMPLE_RATE
            || flow.encoding != SAMPLE_BITS
            || flow.frames_per_packet != Some(frames_per_packet)
            || flow.channels != channels
            || flow.destinations != [expected_destination]
        {
            return Err(Error("source flow readback differs from the owned audio allocation"));
        }

        Ok(())
    }
}
