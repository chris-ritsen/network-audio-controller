use super::snapshot::{Direction, JackSnapshot, PulseEntry, PulseSnapshot, Snapshot};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub fn between(previous: Option<&Snapshot>, current: &Snapshot, time: &str) -> Vec<Value> {
    let mut changes = Vec::new();
    let connected = match (previous, current) {
        (Some(Snapshot::Jack(before)), Snapshot::Jack(after)) => {
            before.connected_at != after.connected_at
        }
        (Some(Snapshot::Pulse(before)), Snapshot::Pulse(after)) => {
            before.connected_at != after.connected_at
        }
        _ => true,
    };
    if connected {
        changes.push(json!({"time":time,"kind":"server_connected"}));
        return changes;
    }
    match (previous, current) {
        (Some(Snapshot::Jack(before)), Snapshot::Jack(after)) => {
            jack(before, after, time, &mut changes)
        }
        (Some(Snapshot::Pulse(before)), Snapshot::Pulse(after)) => {
            pulse(before, after, time, &mut changes)
        }
        _ => {}
    }
    changes
}

fn jack(before: &JackSnapshot, after: &JackSnapshot, time: &str, changes: &mut Vec<Value>) {
    for (kind, before, after) in [
        ("sample_rate", before.sample_rate, after.sample_rate),
        ("buffer_size", before.buffer_size, after.buffer_size),
    ] {
        if before.is_some() && before != after {
            changes.push(json!({"time":time,"kind":kind,"from":before,"to":after}));
        }
    }
    if after.xruns > before.xruns {
        changes.push(json!({"time":after.last_xrun.as_deref().unwrap_or(time),"kind":"xrun","count":after.xruns-before.xruns,"delayed_microseconds":null}));
    }
    let clients = |snapshot: &JackSnapshot| {
        let mut clients = BTreeMap::<&str, usize>::new();
        for port in snapshot.ports.values() {
            *clients.entry(&port.client).or_default() += 1;
        }
        clients
            .into_iter()
            .map(|(client, count)| (client.to_owned(), count))
            .collect::<BTreeMap<_, _>>()
    };
    let previous = clients(before);
    let current = clients(after);
    for (client, count) in &current {
        if !previous.contains_key(client) {
            changes.push(json!({"time":time,"kind":"client_added","client":client,"ports":count}));
        }
    }
    for client in previous.keys() {
        if !current.contains_key(client) {
            changes.push(json!({"time":time,"kind":"client_removed","client":client}));
        }
    }
    let connections = |snapshot: &JackSnapshot| {
        snapshot
            .ports
            .iter()
            .filter(|(_, port)| port.direction == Direction::Output)
            .flat_map(|(name, port)| {
                port.connections
                    .iter()
                    .map(move |destination| (name.clone(), destination.clone()))
            })
            .collect::<BTreeSet<_>>()
    };
    let previous = connections(before);
    let current = connections(after);
    let added: Vec<_> = current.difference(&previous).collect();
    let removed: Vec<_> = previous.difference(&current).collect();
    if !added.is_empty() {
        changes.push(json!({"time":time,"kind":"connected","pairs":added}));
    }
    if !removed.is_empty() {
        changes.push(json!({"time":time,"kind":"disconnected","pairs":removed}));
    }
}

fn identity(entry: &PulseEntry) -> Value {
    json!({"index":entry.index,"name":entry.name,"application":entry.application,"media":entry.media})
}

fn pulse(before: &PulseSnapshot, after: &PulseSnapshot, time: &str, changes: &mut Vec<Value>) {
    for (kind, before, after) in [
        (
            "default_output",
            &before.server.default_sink,
            &after.server.default_sink,
        ),
        (
            "default_input",
            &before.server.default_source,
            &after.server.default_source,
        ),
    ] {
        if before != after {
            changes.push(json!({"time":time,"kind":kind,"from":before,"to":after}));
        }
    }
    for (kind, before, after) in [
        ("output", &before.sinks, &after.sinks),
        ("input", &before.sources, &after.sources),
        ("playback_stream", &before.sink_inputs, &after.sink_inputs),
        (
            "recording_stream",
            &before.source_outputs,
            &after.source_outputs,
        ),
    ] {
        let before: BTreeMap<_, _> = before.iter().map(|entry| (entry.index, entry)).collect();
        let after: BTreeMap<_, _> = after.iter().map(|entry| (entry.index, entry)).collect();
        for (index, entry) in &after {
            let mut change = identity(entry);
            change["time"] = json!(time);
            if let Some(previous) = before.get(index) {
                for (field, before, after) in [
                    (
                        "volume_percent",
                        json!(previous.volume_percent),
                        json!(entry.volume_percent),
                    ),
                    ("mute", json!(previous.mute), json!(entry.mute)),
                    ("sink", json!(previous.sink), json!(entry.sink)),
                    ("source", json!(previous.source), json!(entry.source)),
                ] {
                    if before != after {
                        let mut changed = change.clone();
                        changed["kind"] = json!(format!("{kind}_{field}"));
                        changed["from"] = before;
                        changed["to"] = after;
                        changes.push(changed);
                    }
                }
            } else {
                change["kind"] = json!(format!("{kind}_added"));
                changes.push(change);
            }
        }
        for (index, entry) in &before {
            if !after.contains_key(index) {
                let mut change = identity(entry);
                change["time"] = json!(time);
                change["kind"] = json!(format!("{kind}_removed"));
                changes.push(change);
            }
        }
    }
}
