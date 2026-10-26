use super::snapshot::{Direction, JackSnapshot, PulseEntry, PulseKind, PulseSnapshot, Snapshot};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    JackConnection {
        source: String,
        destination: String,
        connect: bool,
    },
    PulseVolume {
        target: String,
        volume_percent: Option<f64>,
        volume_db: Option<f64>,
    },
    PulseMute {
        target: String,
        mute: bool,
    },
    PulseDefault {
        target: String,
    },
    PulseMove {
        stream: String,
        destination: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Operation {
    JackConnection {
        source: String,
        destination: String,
        connect: bool,
    },
    PulseVolume {
        kind: PulseKind,
        index: u32,
        volumes: Vec<u32>,
    },
    PulseMute {
        kind: PulseKind,
        index: u32,
        mute: bool,
    },
    PulseDefault {
        kind: PulseKind,
        index: u32,
        name: String,
    },
    PulseMove {
        kind: PulseKind,
        index: u32,
        destination: u32,
        name: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Plan {
    pub operation: Operation,
    pub description: Value,
    pub precondition: Value,
    pub unchanged: bool,
}

fn fold(value: &str) -> String {
    caseless::default_case_fold_str(&value.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn validate_name(value: &str, host: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.contains('\0') {
        return Err("name an audio port, device or application without null characters".into());
    }
    if let Some((_, destination_host)) = value.rsplit_once('@') {
        if fold(destination_host).trim_end_matches(".local")
            != fold(host).trim_end_matches(".local")
        {
            return Err(format!(
                "this server changes only {host}'s audio; use the server on {destination_host}"
            ));
        }
    }
    Ok(())
}

pub fn resolve_jack_port(
    snapshot: &JackSnapshot,
    name: &str,
    host: &str,
) -> Result<String, String> {
    validate_name(name, host)?;
    if snapshot.ports.contains_key(name) {
        return Ok(name.into());
    }
    let wanted = fold(name);
    let matches: Vec<_> = snapshot
        .ports
        .iter()
        .filter_map(|(name, port)| {
            (fold(name) == wanted
                || port.aliases.iter().any(|alias| fold(alias) == wanted)
                || port
                    .pretty_name
                    .as_deref()
                    .is_some_and(|name| fold(name) == wanted))
            .then_some(name.clone())
        })
        .collect();
    match matches.as_slice() {
        [name] => Ok(name.clone()),
        [] => Err(format!("no JACK port named {name:?}")),
        _ => Err(format!(
            "{name:?} names several JACK ports: {}",
            matches.join(", ")
        )),
    }
}

pub fn describe_pulse(kind: PulseKind, entry: &PulseEntry) -> String {
    match kind {
        PulseKind::Sink | PulseKind::Source => format!(
            "{} {}",
            kind.description(),
            entry.name.as_deref().unwrap_or("unnamed")
        ),
        _ => format!(
            "{} {} {}",
            entry
                .application
                .as_deref()
                .or(entry.binary.as_deref())
                .unwrap_or("unknown application"),
            kind.description(),
            entry.index
        ),
    }
}

pub fn resolve_pulse<'a>(
    snapshot: &'a PulseSnapshot,
    name: &str,
    kinds: &[PulseKind],
    host: &str,
) -> Result<(PulseKind, &'a PulseEntry), String> {
    validate_name(name, host)?;
    let wanted = fold(name);
    let default = match wanted.as_str() {
        "default output" => Some((PulseKind::Sink, snapshot.server.default_sink.as_deref())),
        "default input" => Some((PulseKind::Source, snapshot.server.default_source.as_deref())),
        _ => None,
    };
    if let Some((kind, Some(name))) = default {
        if kinds.contains(&kind) {
            if let Some(entry) = snapshot
                .entries(kind)
                .iter()
                .find(|entry| entry.name.as_deref() == Some(name))
            {
                return Ok((kind, entry));
            }
        }
    }
    let mut matches = Vec::new();
    for kind in kinds {
        for entry in snapshot.entries(*kind) {
            let candidates = match kind {
                PulseKind::Sink | PulseKind::Source => {
                    vec![entry.name.as_deref(), entry.description.as_deref()]
                }
                _ => vec![
                    entry.application.as_deref(),
                    entry.binary.as_deref(),
                    entry.media.as_deref(),
                ],
            };
            let stream_index = matches!(kind, PulseKind::SinkInput | PulseKind::SourceOutput)
                && (wanted == entry.index.to_string()
                    || wanted == format!("stream {}", entry.index));
            if stream_index
                || candidates
                    .into_iter()
                    .flatten()
                    .any(|value| fold(value) == wanted)
            {
                matches.push((*kind, entry));
            }
        }
    }
    match matches.as_slice() {
        [entry] => Ok(*entry),
        [] => Err(format!("no PulseAudio object named {name:?}")),
        _ => Err(format!(
            "{name:?} matches several: {}",
            matches
                .iter()
                .map(|(kind, entry)| describe_pulse(*kind, entry))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn volume_value(percent: Option<f64>, decibels: Option<f64>) -> Result<u32, String> {
    let value = match (percent, decibels) {
        (Some(value), None) if value.is_finite() && (0.0..=150.0).contains(&value) => value / 100.0,
        (None, Some(value)) if value.is_finite() && value <= 11.0 => 10_f64.powf(value / 60.0),
        _ => {
            return Err(
                "give volume_percent from 0 to 150, or a finite volume_db no higher than 11".into(),
            );
        }
    };
    Ok((value * 65536.0).round() as u32)
}

pub fn volume_description(volumes: &[u32]) -> String {
    format!(
        "{}%",
        volumes
            .iter()
            .map(|value| ((*value as f64 / 65536.0 * 100.0).round() as u32).to_string())
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn mute_description(mute: bool) -> &'static str {
    if mute {
        "muted"
    } else {
        "not muted"
    }
}

pub fn prepare(snapshot: &Snapshot, command: &Command, host: &str) -> Result<Plan, String> {
    match (snapshot, command) {
        (
            Snapshot::Jack(snapshot),
            Command::JackConnection {
                source,
                destination,
                connect,
            },
        ) => {
            if !snapshot.available {
                return Err(format!(
                    "JACK is not available: {}",
                    snapshot.reason.as_deref().unwrap_or("unavailable")
                ));
            }
            let mut source = resolve_jack_port(snapshot, source, host)?;
            let mut destination = resolve_jack_port(snapshot, destination, host)?;
            if snapshot.ports[&source].direction == Direction::Input
                && snapshot.ports[&destination].direction == Direction::Output
            {
                std::mem::swap(&mut source, &mut destination);
            }
            let source_port = &snapshot.ports[&source];
            let destination_port = &snapshot.ports[&destination];
            if source_port.direction != Direction::Output
                || destination_port.direction != Direction::Input
            {
                return Err("connect an output port to an input port".into());
            }
            if source_port.native_type != destination_port.native_type {
                return Err(format!(
                    "{source} is {} but {destination} is {}",
                    source_port.native_type, destination_port.native_type
                ));
            }
            let connected = source_port.connections.contains(&destination);
            Ok(Plan {
                operation: Operation::JackConnection {
                    source: source.clone(),
                    destination: destination.clone(),
                    connect: *connect,
                },
                description: json!({"source":source,"destination":destination,"connected_now":connected,"requested":if *connect {"connected"}else{"disconnected"},"source_feeds":source_port.connections,"destination_hears":destination_port.connections}),
                precondition: json!({"source":source_port,"destination":destination_port,"connected_at":snapshot.connected_at}),
                unchanged: connected == *connect,
            })
        }
        (Snapshot::Pulse(snapshot), command) => prepare_pulse(snapshot, command, host),
        _ => Err("the requested operation belongs to a different audio provider".into()),
    }
}

fn prepare_pulse(snapshot: &PulseSnapshot, command: &Command, host: &str) -> Result<Plan, String> {
    if !snapshot.available {
        return Err(format!(
            "PulseAudio is not available: {}",
            snapshot.reason.as_deref().unwrap_or("unavailable")
        ));
    }
    let (target, kinds): (&str, &[PulseKind]) = match command {
        Command::PulseVolume { target, .. } | Command::PulseMute { target, .. } => (
            target,
            &[
                PulseKind::Sink,
                PulseKind::Source,
                PulseKind::SinkInput,
                PulseKind::SourceOutput,
            ],
        ),
        Command::PulseDefault { target } => (target, &[PulseKind::Sink, PulseKind::Source]),
        Command::PulseMove { stream, .. } => {
            (stream, &[PulseKind::SinkInput, PulseKind::SourceOutput])
        }
        _ => return Err("the requested operation belongs to a different audio provider".into()),
    };
    let (kind, entry) = resolve_pulse(snapshot, target, kinds, host)?;
    let index = entry.index;
    let mut precondition = json!({"entry":entry,"connected_at":snapshot.connected_at});
    if matches!(
        command,
        Command::PulseVolume { .. } | Command::PulseMute { .. }
    ) && matches!(kind, PulseKind::Sink | PulseKind::Source)
    {
        let (streams, default) = if kind == PulseKind::Sink {
            (&snapshot.sink_inputs, &snapshot.server.default_sink)
        } else {
            (&snapshot.source_outputs, &snapshot.server.default_source)
        };
        precondition["consumers"] = json!(streams
            .iter()
            .filter(|stream| stream.destination_index == Some(index))
            .collect::<Vec<_>>());
        precondition["default"] = json!(default);
    }
    let (operation, current, requested, unchanged) = match command {
        Command::PulseVolume {
            volume_percent,
            volume_db,
            ..
        } => {
            if !entry.volume_writable || entry.volume_raw.is_empty() {
                return Err("this PulseAudio object has no writable volume".into());
            }
            let value = volume_value(*volume_percent, *volume_db)?;
            let volumes = vec![value; entry.volume_raw.len()];
            let unchanged = volumes == entry.volume_raw;
            let requested = volume_description(&volumes);
            (
                Operation::PulseVolume {
                    kind,
                    index,
                    volumes,
                },
                volume_description(&entry.volume_raw),
                requested,
                unchanged,
            )
        }
        Command::PulseMute { mute, .. } => {
            let current = entry
                .mute
                .ok_or("this PulseAudio object has no observed mute state")?;
            (
                Operation::PulseMute {
                    kind,
                    index,
                    mute: *mute,
                },
                mute_description(current).into(),
                mute_description(*mute).into(),
                current == *mute,
            )
        }
        Command::PulseDefault { .. } => {
            let name = entry
                .name
                .clone()
                .ok_or("this PulseAudio object has no name")?;
            let current = match kind {
                PulseKind::Sink => &snapshot.server.default_sink,
                _ => &snapshot.server.default_source,
            };
            precondition["default"] = json!(current);
            (
                Operation::PulseDefault {
                    kind,
                    index,
                    name: name.clone(),
                },
                format!(
                    "default {} is {}",
                    kind.description(),
                    current.as_deref().unwrap_or("unknown")
                ),
                format!("default {} is {name}", kind.description()),
                current.as_deref() == Some(name.as_str()),
            )
        }
        Command::PulseMove { destination, .. } => {
            let destination_kind = match kind {
                PulseKind::SinkInput => PulseKind::Sink,
                _ => PulseKind::Source,
            };
            let (_, destination) = resolve_pulse(snapshot, destination, &[destination_kind], host)?;
            let name = destination
                .name
                .clone()
                .ok_or("the destination has no name")?;
            let current = if kind == PulseKind::SinkInput {
                &entry.sink
            } else {
                &entry.source
            };
            precondition["destination"] = json!(destination);
            (
                Operation::PulseMove {
                    kind,
                    index,
                    destination: destination.index,
                    name: name.clone(),
                },
                format!("on {}", current.as_deref().unwrap_or("unknown")),
                format!("on {name}"),
                entry.destination_index == Some(destination.index),
            )
        }
        _ => unreachable!(),
    };
    Ok(Plan {
        operation,
        description: json!({"target":describe_pulse(kind,entry),"kind":kind,"index":index,"current":current,"requested":requested}),
        precondition,
        unchanged,
    })
}

pub fn observed(snapshot: &Snapshot, operation: &Operation) -> Option<(bool, String)> {
    match (snapshot, operation) {
        (
            Snapshot::Jack(snapshot),
            Operation::JackConnection {
                source,
                destination,
                connect,
            },
        ) if snapshot.available => {
            let source_port = snapshot.ports.get(source)?;
            snapshot.ports.get(destination)?;
            let connected = source_port.connections.contains(destination);
            Some((
                connected == *connect,
                if connected {
                    "connected"
                } else {
                    "disconnected"
                }
                .into(),
            ))
        }
        (Snapshot::Pulse(snapshot), operation) if snapshot.available => {
            let (kind, index) = match operation {
                Operation::PulseVolume { kind, index, .. }
                | Operation::PulseMute { kind, index, .. }
                | Operation::PulseDefault { kind, index, .. }
                | Operation::PulseMove { kind, index, .. } => (*kind, *index),
                _ => return None,
            };
            let entry = snapshot.find(kind, index)?;
            match operation {
                Operation::PulseVolume { volumes, .. } => Some((
                    &entry.volume_raw == volumes,
                    volume_description(&entry.volume_raw),
                )),
                Operation::PulseMute { mute, .. } => entry
                    .mute
                    .map(|current| (current == *mute, mute_description(current).into())),
                Operation::PulseDefault { name, .. } => {
                    let current = if kind == PulseKind::Sink {
                        snapshot.server.default_sink.as_deref()
                    } else {
                        snapshot.server.default_source.as_deref()
                    }?;
                    Some((
                        current == name,
                        format!("default {} is {current}", kind.description()),
                    ))
                }
                Operation::PulseMove { destination, .. } => {
                    let name = if kind == PulseKind::SinkInput {
                        entry.sink.as_deref()
                    } else {
                        entry.source.as_deref()
                    }?;
                    Some((
                        entry.destination_index == Some(*destination),
                        format!("on {name}"),
                    ))
                }
                _ => None,
            }
        }
        _ => None,
    }
}
