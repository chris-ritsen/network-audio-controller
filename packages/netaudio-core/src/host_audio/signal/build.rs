use super::*;
use crate::host_audio::{
    cards::{Bridge, Card},
    snapshot::PulseServer,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JackPort {
    pub client: String,
    pub direction: PortDirection,
    #[serde(rename = "type")]
    pub port_type: String,
    pub aliases: Vec<String>,
    pub connections: Vec<String>,
    pub pretty_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JackView {
    pub available: bool,
    pub reason: Option<String>,
    pub ports: BTreeMap<String, JackPort>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PulseEntry {
    pub index: u32,
    pub name: Option<String>,
    pub description: Option<String>,
    pub state: Option<String>,
    pub mute: Option<bool>,
    #[serde(default)]
    pub volume_percent: Vec<u32>,
    pub monitor_of: Option<String>,
    pub jack_client: Option<String>,
    pub application: Option<String>,
    pub binary: Option<String>,
    pub media: Option<String>,
    pub sink: Option<String>,
    pub source: Option<String>,
    pub corked: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PulseView {
    pub available: bool,
    pub reason: Option<String>,
    pub server: PulseServer,
    pub sinks: Vec<PulseEntry>,
    pub sources: Vec<PulseEntry>,
    pub sink_inputs: Vec<PulseEntry>,
    pub source_outputs: Vec<PulseEntry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct HostSnapshot {
    pub host: String,
    pub jack: JackView,
    pub pulse: PulseView,
    pub cards: Vec<Card>,
    pub bridges: BTreeMap<String, Bridge>,
    pub card_links: BTreeMap<String, String>,
}

pub struct WirelessLink {
    pub record: String,
    pub evidence: Evidence,
}

pub struct Input<'a> {
    pub local_host: &'a str,
    pub hosts: &'a BTreeMap<String, HostSnapshot>,
    pub records: &'a BTreeMap<String, Value>,
    pub wireless: &'a BTreeMap<String, Value>,
    pub wireless_links: &'a BTreeMap<String, WirelessLink>,
}

pub(super) fn build(input: Input<'_>) -> Result<Graph, String> {
    let mut graph = Graph {
        local_host: input.local_host.to_owned(),
        ..Graph::default()
    };
    let network = network::add(&mut graph, input.records)?;
    network::wireless(&mut graph, input.wireless, input.wireless_links, &network)?;
    for (host, snapshot) in input.hosts {
        if host != &snapshot.host {
            return Err(format!(
                "Host audio snapshot identity {:?} does not match {host:?}",
                snapshot.host
            ));
        }
        hosts::add(&mut graph, snapshot, &network)?;
    }
    for edges in graph
        .downstream
        .values_mut()
        .chain(graph.upstream.values_mut())
    {
        edges.sort_by(|left, right| natural(&left.identity, &right.identity));
    }
    graph.problems.sort();
    graph.problems.dedup();
    Ok(graph)
}
