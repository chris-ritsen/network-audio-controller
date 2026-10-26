use anyhow::{Context, Result, ensure};
use netaudio_core::host_audio::signal::{self, Details, Graph, VolumeStage};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

const DEADLINE: Duration = Duration::from_millis(500);
const MAXIMUM_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Deserialize)]
struct Source {
    redis_url: String,
    hash: String,
    field: String,
    changed_by: Option<String>,
}

#[derive(Clone, Deserialize)]
struct Displays {
    redis_url: String,
    hash: String,
    presentation: String,
    lease: String,
    outputs: BTreeMap<String, String>,
    words: Vec<String>,
}

pub struct Observations {
    outlets: Option<Source>,
    volumes: Option<Source>,
    displays: Option<Displays>,
    capacity: Arc<Semaphore>,
    pub elsewhere: BTreeMap<String, String>,
}

pub struct Presentation {
    pub host: String,
    pub display: String,
    pub endpoint: String,
    pub output: String,
    pub picture: Option<&'static str>,
}

#[derive(Default)]
pub struct DisplayReading {
    pub presentations: Vec<Presentation>,
    pub problems: Vec<String>,
}

fn tokens(value: &str) -> BTreeSet<String> {
    signal::fold(value)
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse<T: for<'de> Deserialize<'de>>(
    value: Option<&toml::Value>,
    name: &str,
) -> Result<Option<T>> {
    value
        .filter(|value| !value.as_table().is_some_and(toml::Table::is_empty))
        .map(|value| {
            value
                .clone()
                .try_into()
                .with_context(|| format!("Invalid {name} configuration"))
        })
        .transpose()
}

impl Observations {
    pub fn new(configuration: &crate::configuration::Configuration) -> Result<Arc<Self>> {
        let document = &configuration.document;
        let host = document.get("host_audio").and_then(toml::Value::as_table);
        let outlets: Option<Source> = parse(document.get("power_outlets"), "power_outlets")?;
        let volumes: Option<Source> = parse(
            host.and_then(|host| host.get("volume_states")),
            "host_audio.volume_states",
        )?;
        let displays: Option<Displays> = parse(
            host.and_then(|host| host.get("displays")),
            "host_audio.displays",
        )?;
        for source in outlets.iter().chain(volumes.iter()) {
            ensure!(
                !source.hash.is_empty() && !source.field.is_empty(),
                "External observation hash and field must be nonempty"
            );
            redis::Client::open(source.redis_url.as_str())
                .map_err(|_| anyhow::anyhow!("External observation Redis URL is invalid"))?;
        }
        if let Some(displays) = &displays {
            ensure!(
                !displays.hash.is_empty()
                    && displays.presentation.contains("{host}")
                    && displays.lease.contains("{host}")
                    && !displays.outputs.is_empty()
                    && displays.outputs.len() <= 128,
                "Display observations require a hash, host templates, and from 1 through 128 output mappings"
            );
            redis::Client::open(displays.redis_url.as_str())
                .map_err(|_| anyhow::anyhow!("Display observation Redis URL is invalid"))?;
        }
        let elsewhere = parse(
            document.get("mcp").and_then(|value| value.get("elsewhere")),
            "mcp.elsewhere",
        )?
        .unwrap_or_default();
        Ok(Arc::new(Self {
            outlets,
            volumes,
            displays,
            capacity: Arc::new(Semaphore::new(4)),
            elsewhere,
        }))
    }

    async fn connection(url: &str) -> Result<redis::aio::MultiplexedConnection> {
        let client = redis::Client::open(url)
            .map_err(|_| anyhow::anyhow!("External observation Redis URL is invalid"))?;
        client
            .get_multiplexed_async_connection_with_config(
                &redis::AsyncConnectionConfig::new()
                    .set_connection_timeout(Some(DEADLINE))
                    .set_response_timeout(Some(DEADLINE)),
            )
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "External observation connection failed ({:?})",
                    error.kind()
                )
            })
    }

    async fn hash(&self, source: &Source, fields: &[String]) -> Result<Vec<Option<String>>> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        let _permit = self
            .capacity
            .try_acquire()
            .context("External observation capacity is busy")?;
        let values: Vec<Option<String>> = tokio::time::timeout(DEADLINE, async {
            let mut connection = Self::connection(&source.redis_url).await?;
            redis::cmd("HMGET")
                .arg(&source.hash)
                .arg(fields)
                .query_async(&mut connection)
                .await
                .map_err(|error| {
                    anyhow::anyhow!("External observation read failed ({:?})", error.kind())
                })
        })
        .await
        .context("External observation read timed out")??;
        ensure!(
            values.iter().flatten().map(String::len).sum::<usize>() <= MAXIMUM_BYTES,
            "External observation exceeds its response size budget"
        );
        ensure!(
            values.len() == fields.len(),
            "External observation response count is invalid"
        );
        Ok(values)
    }

    pub async fn outlets(&self) -> Result<Vec<Value>> {
        let Some(source) = &self.outlets else {
            return Ok(Vec::new());
        };
        let values = self
            .hash(source, std::slice::from_ref(&source.field))
            .await?;
        let raw = values[0]
            .as_deref()
            .context("The configured power outlet observation is missing")?;
        let values: Vec<Value> =
            serde_json::from_str(raw).context("Power outlet observation is not a JSON list")?;
        ensure!(
            values.iter().all(|value| value.is_object()),
            "Power outlet observations must be objects"
        );
        Ok(values)
    }

    pub async fn apply(&self, graph: &mut Graph) {
        let (volumes, outlets) = tokio::join!(self.volume_states(graph), self.outlets());
        match volumes {
            Ok(states) => {
                for node in graph.nodes.values_mut() {
                    if let Details::Jack(node) = &mut node.details {
                        let client = node.port.split(':').next().unwrap_or(&node.port);
                        node.stage = states.get(&(node.host.clone(), client.to_owned())).cloned();
                    }
                }
            }
            Err(error) => graph.problems.push(error.to_string()),
        }
        match outlets {
            Ok(outlets) => {
                for node in graph.nodes.values_mut() {
                    if let Details::Dante(node) = &mut node.details
                        && node.online == Some(false)
                    {
                        node.outlet = outlet_for(&node.device, &outlets).cloned();
                    }
                }
            }
            Err(error) => graph.problems.push(error.to_string()),
        }
    }

    async fn volume_states(
        &self,
        graph: &Graph,
    ) -> Result<BTreeMap<(String, String), VolumeStage>> {
        let Some(source) = &self.volumes else {
            return Ok(BTreeMap::new());
        };
        let mut clients = BTreeSet::new();
        for (identity, node) in &graph.nodes {
            if let Details::Jack(node) = &node.details
                && graph.upstream.get(identity).is_some_and(|edges| {
                    edges
                        .iter()
                        .any(|edge| edge.evidence == signal::Evidence::Inferred)
                })
            {
                clients.insert((
                    node.host.clone(),
                    node.port.split(':').next().unwrap_or(&node.port).to_owned(),
                ));
            }
        }
        let clients = clients.into_iter().collect::<Vec<_>>();
        let fields = clients
            .iter()
            .map(|(host, client)| {
                source
                    .field
                    .replace("{host}", host)
                    .replace("{client}", client)
            })
            .collect::<Vec<_>>();
        let values = self.hash(source, &fields).await?;
        let mut states = BTreeMap::new();
        for (client, raw) in clients.into_iter().zip(values) {
            let Some(raw) = raw else {
                continue;
            };
            let value: Value =
                serde_json::from_str(&raw).context("Volume observation is not JSON")?;
            ensure!(value.is_object(), "Volume observation must be an object");
            let muted = match (value["muted"].as_bool(), value["solo_muted"].as_bool()) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false) | None) | (None, Some(false)) => Some(false),
                _ => None,
            };
            states.insert(
                client,
                VolumeStage {
                    name: value["name"].as_str().map(str::to_owned),
                    level: value["level"].as_f64(),
                    muted,
                },
            );
        }
        Ok(states)
    }

    pub fn volume_authority(&self) -> Option<&str> {
        self.volumes
            .as_ref()
            .and_then(|source| source.changed_by.as_deref())
    }

    pub async fn displays(&self, point: &str) -> Option<DisplayReading> {
        let source = self.displays.as_ref()?;
        let words = tokens(point);
        if !source
            .words
            .iter()
            .any(|word| words.contains(&signal::fold(word)))
        {
            return None;
        }
        Some(match self.read_displays(source, &words).await {
            Ok(reading) => reading,
            Err(error) => DisplayReading {
                problems: vec![error.to_string()],
                ..DisplayReading::default()
            },
        })
    }

    async fn read_displays(
        &self,
        source: &Displays,
        words: &BTreeSet<String>,
    ) -> Result<DisplayReading> {
        let _permit = self
            .capacity
            .try_acquire()
            .context("Display observation capacity is busy")?;
        let fields = source
            .outputs
            .keys()
            .map(|host| source.presentation.replace("{host}", host))
            .collect::<Vec<_>>();
        let leases = source
            .outputs
            .keys()
            .map(|host| source.lease.replace("{host}", host))
            .collect::<Vec<_>>();
        let (records, leases): (Vec<Option<String>>, Vec<Option<String>>) =
            tokio::time::timeout(DEADLINE, async {
                let mut connection = Self::connection(&source.redis_url).await?;
                redis::pipe()
                    .atomic()
                    .cmd("HMGET")
                    .arg(&source.hash)
                    .arg(fields)
                    .cmd("MGET")
                    .arg(leases)
                    .query_async(&mut connection)
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!("Display observation read failed ({:?})", error.kind())
                    })
            })
            .await
            .context("Display observation read timed out")??;
        ensure!(
            records.len() == source.outputs.len() && leases.len() == records.len(),
            "Display observation response count is invalid"
        );
        ensure!(
            records
                .iter()
                .chain(&leases)
                .flatten()
                .map(String::len)
                .sum::<usize>()
                <= MAXIMUM_BYTES,
            "Display observation exceeds its response size budget"
        );
        drop(_permit);
        let mut reading = DisplayReading::default();
        let mut display_names = BTreeSet::new();
        let now = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
        for (((host, output), record), lease) in source.outputs.iter().zip(records).zip(leases) {
            let Some(record) = record else {
                continue;
            };
            let record: Value =
                serde_json::from_str(&record).context("Display observation is not JSON")?;
            for candidate in std::iter::once(&record)
                .chain(record["candidates"].as_array().into_iter().flatten())
            {
                if let Some(display) = candidate["sink"].as_str() {
                    display_names.insert(signal::fold(display));
                }
            }
            if record["presented"] != true {
                continue;
            }
            let revision = &record["revision"];
            if revision.is_null()
                || record["sink"].as_str().is_none()
                || record["endpoint"].as_str().is_none()
            {
                reading
                    .problems
                    .push(format!("{host}'s presentation record is incomplete"));
                continue;
            }
            let lease: Option<Value> = lease
                .map(|lease| serde_json::from_str(&lease))
                .transpose()
                .context("Display observer lease is not JSON")?;
            let Some(lease) = lease else {
                reading.problems.push(format!(
                    "{host}'s presentation is not confirmed by a current observer"
                ));
                continue;
            };
            if lease["host"].as_str() != Some(host) || &lease["revision"] != revision {
                reading.problems.push(format!(
                    "{host}'s presentation observer confirms a different revision"
                ));
                continue;
            }
            if !lease["expires_at"]
                .as_f64()
                .is_some_and(|expires| expires >= now)
            {
                reading
                    .problems
                    .push(format!("{host}'s presentation observer lease is stale"));
                continue;
            }
            let mut presentation = Presentation {
                host: host.clone(),
                display: record["sink"].as_str().expect("validated sink").to_owned(),
                endpoint: record["endpoint"]
                    .as_str()
                    .expect("validated endpoint")
                    .to_owned(),
                output: output.clone(),
                picture: None,
            };
            if let Some(key) = record["display_state_key"]
                .as_str()
                .filter(|key| !key.is_empty())
            {
                let state_source = Source {
                    redis_url: source.redis_url.clone(),
                    hash: source.hash.clone(),
                    field: key.to_owned(),
                    changed_by: None,
                };
                match self.hash(&state_source, &[key.to_owned()]).await {
                    Ok(states) => {
                        if let Some(raw) = &states[0] {
                            if let Ok(state) = serde_json::from_str::<Value>(raw) {
                                presentation.picture = match state["state_name"].as_str() {
                                    Some("normal") => Some("on"),
                                    Some("pictureoff") => Some("off"),
                                    _ => None,
                                };
                            } else {
                                reading.problems.push(format!(
                                    "{} picture observation is invalid",
                                    presentation.display
                                ));
                            }
                        }
                    }
                    Err(error) => reading.problems.push(error.to_string()),
                }
            }
            reading.presentations.push(presentation);
        }
        let named = words.intersection(&display_names).collect::<BTreeSet<_>>();
        if !named.is_empty() {
            reading
                .presentations
                .retain(|presentation| named.contains(&signal::fold(&presentation.display)));
            for name in named {
                if !reading
                    .presentations
                    .iter()
                    .any(|presentation| signal::fold(&presentation.display) == *name)
                {
                    reading.problems.push(format!("The {} has no current confirmed presentation from the configured computers", name.to_uppercase()));
                }
            }
        }
        Ok(reading)
    }
}

pub fn outlet_for<'a>(device: &str, outlets: &'a [Value]) -> Option<&'a Value> {
    let generic = [
        "avio", "usb", "usbc", "dante", "rx", "tx", "lx", "aes", "aes3", "bt", "output", "input",
    ];
    let words = tokens(device)
        .into_iter()
        .filter(|word| {
            word.len() > 1
                && !word.chars().all(|character| character.is_ascii_digit())
                && !generic.contains(&word.as_str())
        })
        .collect::<BTreeSet<_>>();
    if words.is_empty() {
        return None;
    }
    let mut matches = outlets
        .iter()
        .filter(|outlet| words.is_subset(&tokens(outlet["name"].as_str().unwrap_or_default())));
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}
