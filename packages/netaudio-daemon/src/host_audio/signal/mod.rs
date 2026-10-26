mod history;
mod measurement;
mod presentation;

use super::{manager::Manager, observations::Observations, peers::Resource, provider::Outcome};
use crate::{
    monitoring::{journal::service::Journal, metering_streams::MeteringStreams},
    state::ReadModel,
};
use anyhow::{Context as _, Result, ensure};
use futures_util::{StreamExt, stream};
use netaudio_core::host_audio::signal::{
    self, Details, Direction, Graph, HostSnapshot, Levels, NetworkDirection,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub struct Service {
    pub manager: Arc<Manager>,
    model: Arc<ReadModel>,
    journal: Arc<Journal>,
    metering: Arc<MeteringStreams>,
    shure: Arc<crate::shure::Registry>,
    pub observations: Arc<Observations>,
    cancellation: CancellationToken,
    capacity: Arc<Semaphore>,
}

struct Context {
    graph: Graph,
    snapshots: BTreeMap<String, HostSnapshot>,
    raw: BTreeMap<String, Value>,
    notes: Vec<String>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TraceDirection {
    #[default]
    Both,
    Upstream,
    Downstream,
}

impl TraceDirection {
    fn directions(self) -> &'static [Direction] {
        match self {
            Self::Both => &[Direction::Upstream, Direction::Downstream],
            Self::Upstream => &[Direction::Upstream],
            Self::Downstream => &[Direction::Downstream],
        }
    }
}

fn default_levels() -> bool {
    true
}
fn default_seconds() -> f64 {
    0.5
}

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "HostTraceRequest")]
pub struct TraceRequest {
    pub point: String,
    #[serde(default)]
    pub direction: TraceDirection,
    #[serde(default = "default_levels")]
    pub levels: bool,
    #[serde(default = "default_seconds")]
    pub seconds: f64,
}

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "HostPointLevelsRequest")]
pub struct PointsRequest {
    #[serde(deserialize_with = "point_names")]
    #[schemars(with = "PointNames")]
    pub points: Vec<String>,
    #[serde(default = "default_seconds")]
    pub seconds: f64,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
enum PointNames {
    One(String),
    Several(Vec<String>),
}

fn point_names<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    Ok(match PointNames::deserialize(deserializer)? {
        PointNames::One(point) => vec![point],
        PointNames::Several(points) => points,
    })
}

pub fn validate_point(point: &str) -> Result<()> {
    ensure!(
        !point.trim().is_empty() && point.len() <= 1024 && !point.contains('\0'),
        "Signal point must contain from 1 through 1024 bytes of text"
    );
    Ok(())
}

impl TraceRequest {
    pub fn validate(&self) -> Result<()> {
        validate_point(&self.point)?;
        ensure!(
            self.seconds.is_finite(),
            "Measurement duration must be finite"
        );
        Ok(())
    }
}

impl PointsRequest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.points.is_empty() && self.points.len() <= 32,
            "Provide from 1 through 32 signal points"
        );
        for point in &self.points {
            validate_point(point)?;
        }
        ensure!(
            self.seconds.is_finite(),
            "Measurement duration must be finite"
        );
        Ok(())
    }
}

impl Service {
    pub fn new(
        manager: Arc<Manager>,
        model: Arc<ReadModel>,
        journal: Arc<Journal>,
        metering: Arc<MeteringStreams>,
        shure: Arc<crate::shure::Registry>,
        observations: Arc<Observations>,
        cancellation: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            manager,
            model,
            journal,
            metering,
            shure,
            observations,
            cancellation,
            capacity: Arc::new(Semaphore::new(4)),
        })
    }

    async fn context(&self) -> Result<Context> {
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .context("Signal trace capacity is busy")?;
        let local = self.manager.read(None).await?;
        ensure!(local.status == 200, "Local host audio observation failed");
        let mut raw = BTreeMap::from([(self.manager.host.clone(), local.body)]);
        let mut total_bytes = serde_json::to_vec(&raw)?.len();
        ensure!(
            total_bytes <= 32 * 1024 * 1024,
            "Host audio graph snapshots exceed their total response budget"
        );
        let peers = self.manager.peers.clone();
        let mut requests = stream::iter(peers.hosts().into_iter().map(|host| {
            let peers = peers.clone();
            async move {
                let result = peers
                    .request(&host, Resource::Snapshot, Duration::from_secs(3))
                    .await;
                (host, result)
            }
        }))
        .buffer_unordered(8);
        let mut notes = Vec::new();
        while let Some((host, result)) = requests.next().await {
            match result {
                Ok((200, value)) => {
                    total_bytes += serde_json::to_vec(&value)?.len();
                    ensure!(
                        total_bytes <= 32 * 1024 * 1024,
                        "Host audio graph snapshots exceed their total response budget"
                    );
                    raw.insert(host, value);
                }
                Ok((status, _)) => notes.push(format!(
                    "The NetAudio server on {host} did not report its audio (HTTP {status})"
                )),
                Err(error) => notes.push(format!("Audio on {host} could not be read: {error}")),
            }
        }
        let mut records = self.model.records()?;
        crate::devices::cached::include_remembered(&mut records, self.journal.remembered().await?);
        let records = records
            .into_iter()
            .map(|(name, record)| (name, record.as_ref().clone()))
            .collect::<BTreeMap<_, _>>();
        let wireless = self
            .shure
            .records()?
            .as_object()
            .context("Wireless inventory must be an object")?
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let correlations = self.shure.correlations.snapshot()?;
        let host = self.manager.host.clone();
        let mut context = tokio::task::spawn_blocking(move || {
            let mut snapshots = BTreeMap::new();
            for (name, value) in &raw {
                match serde_json::from_value::<HostSnapshot>(value.clone()) {
                    Ok(snapshot) if snapshot.host == *name => {
                        snapshots.insert(name.clone(), snapshot);
                    }
                    Ok(_) => notes.push(format!(
                        "Host audio report from {name} has a conflicting host identity"
                    )),
                    Err(error) => {
                        notes.push(format!("Host audio report from {name} is invalid: {error}"))
                    }
                }
            }
            ensure!(
                snapshots.contains_key(&host),
                "Local host audio report is invalid"
            );
            let mut links = BTreeMap::new();
            if let Some(error) = &correlations.error {
                notes.push(error.clone());
            } else {
                for (address, device) in &wireless {
                    let Some(address_normalized) = crate::shure::correlations::normalize(address)
                    else {
                        continue;
                    };
                    let explicit = correlations.mappings.get(&address_normalized);
                    let candidates = records
                        .iter()
                        .filter(|(_, record)| {
                            if let Some(explicit) = explicit {
                                record["mac_address"]
                                    .as_str()
                                    .and_then(crate::shure::correlations::normalize)
                                    .as_ref()
                                    == Some(explicit)
                            } else {
                                device["device_type"] == "ad4d"
                                    && ["model", "product_name", "dante_model"].iter().any(
                                        |field| {
                                            record[*field].as_str().is_some_and(|value| {
                                                value.eq_ignore_ascii_case("ad4d")
                                            })
                                        },
                                    )
                            }
                        })
                        .map(|(key, _)| key)
                        .collect::<Vec<_>>();
                    if let [key] = candidates.as_slice() {
                        links.insert(
                            address.clone(),
                            signal::WirelessLink {
                                record: (*key).clone(),
                                evidence: if explicit.is_some() {
                                    signal::Evidence::Configured
                                } else {
                                    signal::Evidence::Inferred
                                },
                            },
                        );
                    } else if explicit.is_some() || candidates.len() > 1 {
                        notes.push(format!(
                            "Wireless Dante mapping for {address} is absent or ambiguous"
                        ));
                    }
                }
            }
            let graph = Graph::build(signal::Input {
                local_host: &host,
                hosts: &snapshots,
                records: &records,
                wireless: &wireless,
                wireless_links: &links,
            })
            .map_err(anyhow::Error::msg)?;
            Ok::<_, anyhow::Error>(Context {
                graph,
                snapshots,
                raw,
                notes,
                _permit: permit,
            })
        })
        .await??;
        self.observations.apply(&mut context.graph).await;
        Ok(context)
    }

    pub async fn trace(&self, request: TraceRequest) -> Result<Outcome> {
        request.validate()?;
        let mut context = self.context().await?;
        let display = self.observations.displays(&request.point).await;
        let mut presented = BTreeMap::new();
        if let Some(reading) = &display {
            for presentation in &reading.presentations {
                let name = format!("{} {}", presentation.host, presentation.output);
                let identities = resolve(&context.graph, &name);
                if identities.is_empty() {
                    context.notes.push(format!(
                        "The display output {name} is not in the known signal graph"
                    ));
                } else {
                    presented.insert(name, identities);
                }
            }
        }
        let mut starts = context.graph.resolve(&request.point);
        let mut matched = None;
        if starts.is_empty() {
            starts = context.graph.resolve_device(&request.point);
            if !starts.is_empty() {
                matched = Some(format!(
                    "Traces every routed channel of {} ({})",
                    request.point,
                    starts.len()
                ));
            }
        }
        if starts.is_empty() {
            starts = context.graph.resolve_loosely(&request.point);
            if !starts.is_empty() {
                matched = Some(format!(
                    "No point has that exact name; tracing {}",
                    starts
                        .iter()
                        .map(|identity| signal::plain_label(&context.graph.nodes[identity].label))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        if !presented.is_empty() {
            starts = unique(presented.values().flatten().cloned());
        }
        if starts.is_empty() {
            if let Some(reading) = display {
                let mut value = json!({"point":request.point});
                presentation::display_answer(
                    &mut value,
                    &context.graph,
                    &reading,
                    &presented,
                    None,
                );
                return Ok(Outcome {
                    status: 200,
                    body: value,
                });
            }
            return Ok(Outcome {
                status: 404,
                body: json!({"error":format!("Nothing named {:?} in the signal chain",request.point),"suggestions":context.graph.suggestions(&request.point),"applications_using_audio":presentation::applications(&context.snapshots),"notes":context.notes}),
            });
        }
        let path = context
            .graph
            .path(&request.point, starts, request.direction.directions());
        let related = if path.walks.values().all(|walk| walk.steps.is_empty()) {
            context.graph.related_paths(
                &context
                    .graph
                    .also_named(&request.point, &path.starts.iter().cloned().collect()),
            )
        } else {
            Vec::new()
        };
        let identities = unique(
            context.graph.path_identities(&path).into_iter().chain(
                related
                    .iter()
                    .flat_map(|path| context.graph.path_identities(path)),
            ),
        );
        let levels = if request.levels {
            Some(
                self.measure(&mut context, &identities, request.seconds)
                    .await?,
            )
        } else {
            None
        };
        let mut value = context.graph.trace(&path, levels.as_ref());
        if let Some(matched) = matched {
            value["matched"] = json!(matched);
        }
        if request.levels {
            value["measured_at"] = json!(presentation::now());
            if value.get("level_changes").is_none() {
                let changes = self.recorded_changes(&context.graph, &path)?;
                if !changes.is_empty() {
                    value["level_changes"] = json!(changes);
                }
            }
        }
        if !related.is_empty() {
            value["paths"] = json!(
                related
                    .iter()
                    .map(|path| (
                        path.name.clone(),
                        context.graph.trace(path, levels.as_ref())
                    ))
                    .collect::<BTreeMap<_, _>>()
            );
            value["also_named"] =
                json!("Connected points with related names are shown under paths");
        }
        if let Some(reading) = display {
            presentation::display_answer(
                &mut value,
                &context.graph,
                &reading,
                &presented,
                levels.as_ref(),
            );
        }
        self.finish(&context, &mut value);
        Ok(Outcome {
            status: 200,
            body: value,
        })
    }

    fn finish(&self, context: &Context, value: &mut Value) {
        let notes = unique(context.notes.iter().chain(&context.graph.problems).cloned());
        if !notes.is_empty() {
            value["notes"] = json!(notes);
        }
        let hosts = context
            .snapshots
            .keys()
            .filter(|host| *host != &self.manager.host)
            .cloned()
            .collect::<Vec<_>>();
        if !hosts.is_empty() {
            value["other_computers"] = json!(format!(
                "Also included: {}; prefix their points with the computer name",
                hosts.join(", ")
            ));
        }
        let stages =
            presentation::volume_stages(&context.graph, self.observations.volume_authority());
        if !stages.is_empty() {
            value["volume_stages"] = json!(stages);
        }
    }
}

fn unique(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn resolve(graph: &Graph, point: &str) -> Vec<String> {
    let exact = graph.resolve(point);
    if exact.is_empty() {
        graph.resolve_loosely(point)
    } else {
        exact
    }
}
