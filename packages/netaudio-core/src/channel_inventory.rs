use std::collections::{BTreeMap, HashMap};

use crate::inventory::InventoryState;
use serde_json::{json, Value};

use crate::commands::{
    OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809, OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809,
};
use crate::protocol::{
    is_modern_arc_protocol, response_envelope, RESULT_CODE_MORE_PAGES, RESULT_CODE_SUCCESS,
};
use crate::responses::{
    parse_modern_arc_receiver_channel_status_page, parse_modern_arc_transmitter_channel_status_page,
};

type Identity = (u16, u16);

struct Record {
    identity: Identity,
    global_id: u16,
    value: Value,
}

fn stable_record(record: &Value) -> Value {
    let mut stable = record.clone();
    if let Some(fields) = stable.as_object_mut() {
        fields.retain(|key, _| !key.ends_with("_pointer") && key != "raw_record_hexadecimal");
    }

    stable
}

/// Transport-independent modern ARC inventory. Rejected responses leave the
/// operation unchanged; callers may inspect state without advancing it.
pub struct ChannelInventory {
    receiver: bool,
    protocol_id: u16,
    maximum_pages: usize,
    records: BTreeMap<Identity, Record>,
    global_identities: HashMap<u16, Identity>,
    capacities: Vec<Value>,
    raw_bodies: Vec<Value>,
    next_range: Option<(u16, u16)>,
    final_page: Option<Value>,
}

impl ChannelInventory {
    pub fn new(
        channel_type: &str,
        protocol_id: u16,
        maximum_pages: usize,
    ) -> Result<Self, &'static str> {
        let receiver = match channel_type {
            "rx" => true,
            "tx" => false,
            _ => return Err("invalid channel type"),
        };

        if !is_modern_arc_protocol(protocol_id) {
            return Err("unsupported channel inventory protocol");
        }

        if maximum_pages == 0 || maximum_pages > 256 {
            return Err("channel inventory page limit must be between 1 and 256");
        }

        Ok(Self {
            receiver,
            protocol_id,
            maximum_pages,
            records: BTreeMap::new(),
            global_identities: HashMap::new(),
            capacities: Vec::new(),
            raw_bodies: Vec::new(),
            next_range: Some((1, 1)),
            final_page: None,
        })
    }

    pub fn accept(&mut self, response: &[u8]) -> Result<(), &'static str> {
        if self.next_range.is_none() {
            return Err("channel inventory is already complete");
        }

        if self.capacities.len() >= self.maximum_pages {
            return Err("channel inventory exceeded its page limit");
        }

        let envelope = response_envelope(response).ok_or("malformed channel page envelope")?;
        let expected_opcode = if self.receiver {
            OPCODE_QUERY_RECEIVER_CHANNEL_STATUS_2809
        } else {
            OPCODE_QUERY_TRANSMITTER_CHANNEL_STATUS_2809
        };

        if envelope.protocol_id != self.protocol_id {
            return Err("channel page protocol changed during pagination");
        }

        if envelope.opcode != expected_opcode {
            return Err("malformed channel page: unexpected response type");
        }

        if ![RESULT_CODE_SUCCESS, RESULT_CODE_MORE_PAGES].contains(&envelope.result_code) {
            return Err("device rejected channel inventory request");
        }

        // Identity comes from typed, validated parser output, not caller JSON.
        let (records, page) = if self.receiver {
            let page = parse_modern_arc_receiver_channel_status_page(response)
                .ok_or("malformed receiver channel page")?;
            let records = page
                .records
                .iter()
                .map(|record| Record {
                    identity: (record.media_type_code, record.media_local_channel_id),
                    global_id: record.channel_number,
                    value: json!(record),
                })
                .collect::<Vec<_>>();

            (records, json!(page))
        } else {
            let page = parse_modern_arc_transmitter_channel_status_page(response)
                .ok_or("malformed transmitter channel page")?;
            let records = page
                .records
                .iter()
                .map(|record| Record {
                    identity: (record.media_type_code, record.media_local_channel_id),
                    global_id: record.channel_number,
                    value: json!(record),
                })
                .collect::<Vec<_>>();

            (records, json!(page))
        };

        let mut added = 0;
        for record in &records {
            if self
                .global_identities
                .get(&record.global_id)
                .is_some_and(|identity| *identity != record.identity)
            {
                return Err("channel pages contain a conflicting global ID");
            }

            if let Some(existing) = self.records.get(&record.identity) {
                if stable_record(&existing.value) != stable_record(&record.value) {
                    return Err("channel pages contain a conflicting duplicate");
                }
            } else {
                added += 1;
            }
        }

        if !self.capacities.is_empty() && added == 0 {
            return Err("channel page made no progress");
        }

        let next_range = if envelope.result_code == RESULT_CODE_MORE_PAGES {
            let media_type = records
                .last()
                .ok_or("partial channel page made no progress")?
                .identity
                .0;
            let next_id = (1..=u16::MAX)
                .find(|id| {
                    let identity = (media_type, *id);
                    !self.records.contains_key(&identity)
                        && !records.iter().any(|record| record.identity == identity)
                })
                .ok_or("channel pagination exhausted the media-local ID space")?;

            Some((media_type, next_id))
        } else {
            None
        };

        // Commit only after validating the entire page and its continuation.
        for record in records {
            self.global_identities
                .insert(record.global_id, record.identity);
            self.records.entry(record.identity).or_insert(record);
        }

        self.capacities.push(page["page_capacity"].clone());
        self.raw_bodies.push(page["raw_body_hexadecimal"].clone());
        self.next_range = next_range;

        if next_range.is_none() {
            self.final_page = Some(page);
        }

        Ok(())
    }

    pub fn state(&self) -> InventoryState {
        if let Some((media_selector, starting_channel_identifier)) = self.next_range {
            return InventoryState::pending(serde_json::Map::from_iter([
                (
                    "command".into(),
                    json!(if self.receiver {
                        "query_modern_arc_receiver_channel_status"
                    } else {
                        "query_modern_arc_transmitter_channel_status"
                    }),
                ),
                ("protocol_id".into(), json!(self.protocol_id)),
                ("media_selector".into(), json!(media_selector)),
                (
                    "starting_channel_identifier".into(),
                    json!(starting_channel_identifier),
                ),
                ("ending_channel_identifier".into(), json!(0)),
            ]));
        }

        let inventory = {
            let page = self
                .final_page
                .as_ref()
                .expect("completed channel inventory has a final page");
            let mut result = page.clone();
            let mut records: Vec<_> = self.records.values().collect();
            records.sort_by_key(|record| (record.global_id, record.identity));
            result["records"] = json!(records
                .iter()
                .map(|record| &record.value)
                .collect::<Vec<_>>());
            result["reported_record_count"] = json!(records.len());
            result["total_record_count"] = json!(records.len());
            result["page_count"] = json!(self.capacities.len());
            result["page_capacities"] = json!(self.capacities);
            result["raw_page_body_hexadecimal"] = json!(self.raw_bodies);

            result
        };

        InventoryState::complete(inventory)
    }
}
