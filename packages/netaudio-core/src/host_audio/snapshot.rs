use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Input,
    Output,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JackProperty {
    pub value: String,
    pub property_type: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JackPort {
    pub identity: u64,
    pub client: String,
    pub short_name: String,
    pub direction: Direction,
    #[serde(rename = "type")]
    pub port_type: String,
    pub native_type: String,
    pub physical: bool,
    pub terminal: bool,
    pub aliases: Vec<String>,
    pub connections: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pretty_name: Option<String>,
    pub properties: BTreeMap<String, JackProperty>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JackSnapshot {
    pub available: bool,
    pub reason: Option<String>,
    pub seen_running: bool,
    pub server: String,
    pub client_name: Option<String>,
    pub connected_at: Option<String>,
    pub sample_rate: Option<u32>,
    pub buffer_size: Option<u32>,
    pub realtime: Option<bool>,
    pub cpu_load: Option<f64>,
    pub xruns: u64,
    pub last_xrun: Option<String>,
    pub xruns_since: Option<String>,
    pub ports: BTreeMap<String, JackPort>,
    pub changes: Vec<Value>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct PulseServer {
    pub name: Option<String>,
    pub version: Option<String>,
    pub host: Option<String>,
    pub default_sink: Option<String>,
    pub default_source: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PulseKind {
    Sink,
    Source,
    SinkInput,
    SourceOutput,
}

impl PulseKind {
    pub fn description(self) -> &'static str {
        match self {
            Self::Sink => "output",
            Self::Source => "input",
            Self::SinkInput => "playback stream",
            Self::SourceOutput => "recording stream",
        }
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct PulseEntry {
    pub index: u32,
    pub name: Option<String>,
    pub description: Option<String>,
    pub driver: Option<String>,
    pub state: Option<String>,
    pub mute: Option<bool>,
    pub volume_percent: Vec<u32>,
    pub volume_db: Vec<Option<f64>>,
    pub volume_raw: Vec<u32>,
    pub volume_writable: bool,
    pub channels: Vec<String>,
    pub format: Option<String>,
    pub monitor_source: Option<String>,
    pub monitor_of: Option<String>,
    pub jack_client: Option<String>,
    pub device: Option<String>,
    pub application: Option<String>,
    pub binary: Option<String>,
    pub process_id: Option<String>,
    pub media: Option<String>,
    pub role: Option<String>,
    pub sink: Option<String>,
    pub source: Option<String>,
    pub destination_index: Option<u32>,
    pub corked: Option<bool>,
    pub properties: BTreeMap<String, Vec<u8>>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PulseSnapshot {
    pub available: bool,
    pub reason: Option<String>,
    pub seen_running: bool,
    pub connected_at: Option<String>,
    pub server: PulseServer,
    pub sinks: Vec<PulseEntry>,
    pub sources: Vec<PulseEntry>,
    pub sink_inputs: Vec<PulseEntry>,
    pub source_outputs: Vec<PulseEntry>,
    pub changes: Vec<Value>,
}

impl PulseSnapshot {
    pub fn entries(&self, kind: PulseKind) -> &[PulseEntry] {
        match kind {
            PulseKind::Sink => &self.sinks,
            PulseKind::Source => &self.sources,
            PulseKind::SinkInput => &self.sink_inputs,
            PulseKind::SourceOutput => &self.source_outputs,
        }
    }

    pub fn find(&self, kind: PulseKind, index: u32) -> Option<&PulseEntry> {
        self.entries(kind).iter().find(|entry| entry.index == index)
    }
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "provider", content = "state", rename_all = "snake_case")]
pub enum Snapshot {
    Jack(JackSnapshot),
    Pulse(PulseSnapshot),
}
