mod queue;
mod view;

pub use queue::{EventReceiver, EventSender, event_queue};
pub(crate) use view::TELEMETRY_FIELDS;
pub use view::{DeviceView, EventOptions, substantive_record};

use bytes::Bytes;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;

pub const CLIENT_QUEUE_CAPACITY: usize = 128;
pub const DEVICE_DISCOVERED: &str = "device_discovered";
pub const DEVICE_UPDATED: &str = "device_updated";
pub const DEVICE_REMOVED: &str = "device_removed";
pub const DEVICE_PATCH: &str = "device_patch";
pub const TELEMETRY_UPDATED: &str = "telemetry_updated";
pub const METER_VALUES: &str = "meter_values";
pub const SHURE_METER_VALUES: &str = "shure_meter_values";
pub const MANAGED_STATUS: &str = "managed_status";
pub const SNAPSHOT: &str = "snapshot";
pub const EXTERNAL_FLOW_CHANGED: &str = "external_flow_changed";

#[derive(Clone, Debug)]
pub struct Event {
    pub sequence: u64,
    pub value: Arc<Value>,
    pub encoded: Bytes,
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
            value: Arc::new(value),
            encoded: Bytes::from(encoded),
        })
    }

    pub fn meter_key(&self) -> Option<Cow<'_, str>> {
        match self.value.get("event")?.as_str()? {
            METER_VALUES => self.value.get("server_name")?.as_str().map(Cow::Borrowed),
            SHURE_METER_VALUES => Some(Cow::Owned(format!(
                "shure:{}:{}:{}",
                self.value.get("mac")?.as_str()?,
                self.value.get("channel")?,
                self.value["key"].as_str().unwrap_or("sample")
            ))),
            _ => None,
        }
    }
}
