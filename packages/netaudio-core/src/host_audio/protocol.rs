use super::{control::Command, measurement, recording, snapshot::Snapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const VERSION: u32 = 1;
pub const MAXIMUM_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Jack,
    Pulse,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Observe,
    Measure,
    Record,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub sequence: u64,
    pub action: RequestAction,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequestAction {
    Snapshot,
    Measure {
        request: measurement::Request,
    },
    Record {
        configuration: recording::Configuration,
    },
    Execute {
        operation_id: String,
        command: Command,
        precondition: Option<Value>,
    },
    Shutdown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub sequence: u64,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Hello {
        protocol_version: u32,
        provider: Provider,
        purpose: Purpose,
        executable_sha256: String,
        package_version: String,
        process_id: u32,
    },
    Observation {
        revision: u64,
        observed_at: String,
        snapshot: Box<Snapshot>,
    },
    Dispatching {
        request_sequence: u64,
        operation_id: String,
        plan: Value,
    },
    Completed {
        request_sequence: u64,
        status: u16,
        body: Value,
    },
    Measurement {
        request_sequence: u64,
        readings: measurement::Readings,
    },
    Recording {
        request_sequence: u64,
        record: recording::Record,
    },
    Unavailable {
        reason: String,
    },
}
