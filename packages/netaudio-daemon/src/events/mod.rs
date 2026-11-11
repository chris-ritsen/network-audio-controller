mod coalescing;
mod queue;
mod view;

pub use queue::{event_queue, EventReceiver, EventSender};
pub(crate) use view::TELEMETRY_FIELDS;
pub use view::{substantive_record, DeviceView, EventOptions};

use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;

pub const CLIENT_QUEUE_CAPACITY: usize = 128;
pub const CLIENT_QUEUE_LIMIT: usize = 16_384;
pub const DEVICE_DISCOVERED: &str = "device_discovered";
pub const DEVICE_PATCH: &str = "device_patch";
pub const DEVICE_REMOVED: &str = "device_removed";
pub const DEVICE_UPDATED: &str = "device_updated";
pub const EXTERNAL_FLOW_CHANGED: &str = "external_flow_changed";
pub const MANAGED_STATUS: &str = "managed_status";
pub const METER_VALUES: &str = "meter_values";
pub const SHURE_CORRELATIONS_CHANGED: &str = "shure_correlations_changed";
pub const SHURE_DEVICE_DISCOVERED: &str = "shure_device_discovered";
pub const SHURE_DEVICE_UPDATED: &str = "shure_device_updated";
pub const SHURE_DISCOVERY_STATUS: &str = "shure_discovery_status";
pub const SHURE_METER_VALUES: &str = "shure_meter_values";
pub const SNAPSHOT: &str = "snapshot";
pub const TELEMETRY_UPDATED: &str = "telemetry_updated";

#[derive(Clone, Debug)]
pub struct Event {
    pub sequence: u64,
    pub value: Arc<Value>,
    pub encoded: Bytes,
    slot: Option<coalescing::Slot>,
}

impl Event {
    pub fn new(sequence: u64, mut value: Value) -> Result<Self, serde_json::Error> {
        value["revision"] = Value::String(sequence.to_string());
        crate::representation::browser_values(&mut value);
        let mut encoded = b"data: ".to_vec();
        serde_json::to_writer(&mut encoded, &value)?;
        encoded.extend_from_slice(b"\n\n");

        Ok(Self {
            sequence,
            slot: coalescing::slot(&value),
            value: Arc::new(value),
            encoded: Bytes::from(encoded),
        })
    }
}
