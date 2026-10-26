mod build;
mod hosts;
mod levels;
mod names;
mod network;
pub mod observation;
mod presentation;

pub use build::{HostSnapshot, Input, JackPort, JackView, PulseEntry, PulseView, WirelessLink};
pub use levels::{Level, Levels, MeterTargets, State, wireless_text};
pub use names::{fold, natural, plain_label};
pub use presentation::{Path, joined, stage_name};

use super::cards::Channel;
use super::snapshot::Direction as PortDirection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const MAXIMUM_NODES: usize = 65_536;
const MAXIMUM_EDGES: usize = 262_144;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Upstream,
    Downstream,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Evidence {
    Observed,
    Reported,
    Configured,
    Inferred,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDirection {
    Receiver,
    Transmitter,
}

impl NetworkDirection {
    pub fn name(self) -> &'static str {
        match self {
            Self::Receiver => "rx",
            Self::Transmitter => "tx",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PulseKind {
    Sink,
    Source,
    Playback,
    Recording,
}

impl PulseKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Sink => "sink",
            Self::Source => "source",
            Self::Playback => "playback",
            Self::Recording => "recording",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct VolumeStage {
    pub name: Option<String>,
    pub level: Option<f64>,
    pub muted: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JackNode {
    pub host: String,
    pub port: String,
    pub direction: PortDirection,
    pub hardware: Option<Channel>,
    pub mixes: Vec<String>,
    pub stage: Option<VolumeStage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DanteNode {
    pub device: String,
    pub record: Option<String>,
    pub server_name: Option<String>,
    pub direction: NetworkDirection,
    pub channel: Option<u32>,
    pub online: Option<bool>,
    pub outlet: Option<Value>,
    pub unresolved: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PulseNode {
    pub host: String,
    pub kind: PulseKind,
    pub entry: PulseEntry,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WirelessNode {
    pub address: String,
    pub number: u32,
    pub device: Value,
    pub channel: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub enum Details {
    Jack(JackNode),
    Dante(DanteNode),
    Pulse(PulseNode),
    Wireless(WirelessNode),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Node {
    pub identity: String,
    pub label: String,
    #[serde(flatten)]
    pub details: Details,
}

impl Node {
    pub fn host(&self) -> Option<&str> {
        match &self.details {
            Details::Jack(node) => Some(&node.host),
            Details::Pulse(node) => Some(&node.host),
            _ => None,
        }
    }

    pub fn is_output(&self) -> bool {
        match &self.details {
            Details::Pulse(node) => node.kind == PulseKind::Sink,
            Details::Jack(node) => node
                .hardware
                .as_ref()
                .is_some_and(|hardware| hardware.direction == super::cards::Direction::Playback),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Edge {
    pub identity: String,
    pub note: Option<String>,
    pub evidence: Evidence,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Step {
    pub depth: usize,
    pub identity: String,
    pub note: Option<String>,
    pub evidence: Evidence,
    pub repeated: bool,
    pub parent: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Walk {
    pub steps: Vec<Step>,
    pub remaining: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Graph {
    pub nodes: BTreeMap<String, Node>,
    pub downstream: BTreeMap<String, Vec<Edge>>,
    pub upstream: BTreeMap<String, Vec<Edge>>,
    pub names: BTreeMap<String, Vec<(u32, String)>>,
    pub local_host: String,
    pub problems: Vec<String>,
    #[serde(skip)]
    edge_count: usize,
}

impl Graph {
    pub fn build(input: Input<'_>) -> Result<Self, String> {
        build::build(input)
    }

    pub fn edges(&self, direction: Direction) -> &BTreeMap<String, Vec<Edge>> {
        match direction {
            Direction::Upstream => &self.upstream,
            Direction::Downstream => &self.downstream,
        }
    }

    fn add(&mut self, node: Node, mut names: BTreeMap<String, u32>) -> Result<(), String> {
        if self.nodes.contains_key(&node.identity) {
            return Err(format!(
                "Signal graph contains duplicate identity {:?}",
                node.identity
            ));
        }
        if self.nodes.len() >= MAXIMUM_NODES {
            return Err(format!(
                "Signal graph exceeds its {MAXIMUM_NODES}-node resource budget"
            ));
        }
        names.entry(node.label.clone()).or_insert(0);
        names
            .entry(plain_label(&node.label).to_owned())
            .or_insert(0);
        for (name, priority) in names {
            let name = fold(&name);
            if name.is_empty() {
                continue;
            }
            let entries = self.names.entry(name).or_default();
            if let Some((previous, _)) = entries
                .iter_mut()
                .find(|(_, identity)| *identity == node.identity)
            {
                *previous = (*previous).min(priority);
            } else {
                entries.push((priority, node.identity.clone()));
            }
        }
        self.nodes.insert(node.identity.clone(), node);
        Ok(())
    }

    fn link(
        &mut self,
        source: &str,
        destination: &str,
        note: Option<String>,
        evidence: Evidence,
    ) -> Result<(), String> {
        if !self.nodes.contains_key(source) || !self.nodes.contains_key(destination) {
            return Err(format!(
                "Signal graph edge references an absent endpoint: {source:?} -> {destination:?}"
            ));
        }
        if self
            .downstream
            .get(source)
            .is_some_and(|edges| edges.iter().any(|edge| edge.identity == destination))
        {
            return Ok(());
        }
        if self.edge_count >= MAXIMUM_EDGES {
            return Err(format!(
                "Signal graph exceeds its {MAXIMUM_EDGES}-edge resource budget"
            ));
        }
        self.downstream
            .entry(source.to_owned())
            .or_default()
            .push(Edge {
                identity: destination.to_owned(),
                note: note.clone(),
                evidence,
            });
        self.upstream
            .entry(destination.to_owned())
            .or_default()
            .push(Edge {
                identity: source.to_owned(),
                note,
                evidence,
            });
        self.edge_count += 1;
        Ok(())
    }

    pub fn walk(&self, starts: &[String], direction: Direction, limit: usize) -> Walk {
        let edges = self.edges(direction);
        let mut steps = Vec::new();
        let mut seen = starts.iter().cloned().collect::<BTreeSet<_>>();
        let mut pending = Vec::new();
        for start in starts.iter().rev() {
            if let Some(followers) = edges.get(start) {
                for edge in followers.iter().rev() {
                    pending.push((start.clone(), edge, 1));
                }
            }
        }
        while let Some((parent, edge, depth)) = pending.pop() {
            if steps.len() >= limit {
                break;
            }
            let repeated = !seen.insert(edge.identity.clone());
            steps.push(Step {
                depth,
                identity: edge.identity.clone(),
                note: edge.note.clone(),
                evidence: edge.evidence,
                repeated,
                parent,
            });
            if !repeated {
                if let Some(followers) = edges.get(&edge.identity) {
                    for following in followers.iter().rev() {
                        pending.push((edge.identity.clone(), following, depth + 1));
                    }
                }
            }
        }
        let mut reachable = starts.iter().cloned().collect::<BTreeSet<_>>();
        let mut pending = starts.to_vec();
        while let Some(identity) = pending.pop() {
            for edge in edges.get(&identity).into_iter().flatten() {
                if reachable.insert(edge.identity.clone()) {
                    pending.push(edge.identity.clone());
                }
            }
        }
        let remaining = reachable.difference(&seen).count();
        Walk { steps, remaining }
    }
}
