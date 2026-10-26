use super::*;
use crate::host_audio::cards;

struct Scope<'a> {
    host: &'a str,
    remote: bool,
}

impl Scope<'_> {
    fn jack(&self, port: &str) -> String {
        format!("jack@{}:{port}", self.host)
    }

    fn pulse(&self, kind: PulseKind, key: &str) -> String {
        format!("pulse@{}:{}:{key}", self.host, kind.name())
    }

    fn label(&self, label: String) -> String {
        if self.remote {
            format!("{} {label}", self.host)
        } else {
            label
        }
    }

    fn names(&self, names: BTreeMap<String, u32>) -> BTreeMap<String, u32> {
        let mut scoped = BTreeMap::new();
        for (name, priority) in names {
            if name.is_empty() {
                continue;
            }
            scoped.insert(format!("{} {name}", self.host), priority);
            if self.remote {
                scoped.insert(format!("{name}@{}", self.host), priority);
                scoped.entry(name).or_insert(priority + 3);
            } else {
                scoped.insert(name, priority);
            }
        }
        scoped
    }
}

#[derive(Default)]
struct Ports {
    inputs: Vec<String>,
    outputs: Vec<String>,
}

fn clients(snapshot: &HostSnapshot) -> BTreeMap<String, Ports> {
    let mut clients = BTreeMap::<String, Ports>::new();
    if !snapshot.jack.available {
        return clients;
    }
    for (name, port) in &snapshot.jack.ports {
        if port.port_type != "audio" {
            continue;
        }
        let entry = clients.entry(port.client.clone()).or_default();
        match port.direction {
            PortDirection::Input => entry.inputs.push(name.clone()),
            PortDirection::Output => entry.outputs.push(name.clone()),
        }
    }
    for ports in clients.values_mut() {
        ports
            .inputs
            .sort_by(|first, second| natural(first, second).then_with(|| first.cmp(second)));
        ports
            .outputs
            .sort_by(|first, second| natural(first, second).then_with(|| first.cmp(second)));
    }
    clients
}

pub(super) fn add(
    graph: &mut Graph,
    snapshot: &HostSnapshot,
    network: &network::Network,
) -> Result<(), String> {
    let scope = Scope {
        host: &snapshot.host,
        remote: graph.local_host != snapshot.host,
    };
    let clients = clients(snapshot);
    let pulse_clients = if snapshot.pulse.available {
        pulse(graph, &scope, snapshot, &clients)?
    } else {
        graph.problems.push(format!(
            "PulseAudio on {} is not available: {}",
            scope.host,
            snapshot.pulse.reason.as_deref().unwrap_or("unknown reason")
        ));
        BTreeSet::new()
    };
    if snapshot.jack.available {
        jack(graph, &scope, snapshot, &clients, &pulse_clients, network)?;
        if snapshot.pulse.available {
            pulse_links(graph, &scope, snapshot, &clients)?;
        }
    } else {
        graph.problems.push(format!(
            "JACK on {} is not available: {}",
            scope.host,
            snapshot.jack.reason.as_deref().unwrap_or("unknown reason")
        ));
    }
    Ok(())
}

fn jack(
    graph: &mut Graph,
    scope: &Scope<'_>,
    snapshot: &HostSnapshot,
    clients: &BTreeMap<String, Ports>,
    pulse_clients: &BTreeSet<String>,
    network: &network::Network,
) -> Result<(), String> {
    for (name, port) in &snapshot.jack.ports {
        if port.port_type != "audio" {
            continue;
        }
        let hardware = cards::hardware_aliases(&port.aliases, &snapshot.cards)
            .or_else(|| cards::bridge_channel(name, &snapshot.bridges));
        let aliases = port
            .aliases
            .iter()
            .filter(|alias| !alias.starts_with("alsa_pcm:"))
            .cloned()
            .collect::<Vec<_>>();
        let label = if aliases.is_empty() {
            name.clone()
        } else {
            format!("{name} [{}]", aliases.join(", "))
        };
        let location = hardware.as_ref().map_or_else(
            || "JACK".to_owned(),
            |hardware| {
                let direction = match hardware.direction {
                    cards::Direction::Capture => "capture",
                    cards::Direction::Playback => "playback",
                };
                format!(
                    "JACK, sound card {} {direction} {}",
                    hardware.card, hardware.channel
                )
            },
        );
        let mixes = if port.direction == PortDirection::Input && port.connections.len() > 1 {
            port.connections.clone()
        } else {
            Vec::new()
        };
        let mut names = BTreeMap::from([
            (name.clone(), 0),
            (format!("jack {name}"), 0),
            (port.client.clone(), 2),
        ]);
        for alias in &port.aliases {
            names.insert(alias.clone(), 1);
        }
        if let Some(pretty) = &port.pretty_name {
            names.insert(pretty.clone(), 1);
        }
        graph.add(
            Node {
                identity: scope.jack(name),
                label: scope.label(format!("{label} ({location})")),
                details: Details::Jack(JackNode {
                    host: scope.host.to_owned(),
                    port: name.clone(),
                    direction: port.direction.clone(),
                    hardware,
                    mixes,
                    stage: None,
                }),
            },
            scope.names(names),
        )?;
    }
    for (name, port) in &snapshot.jack.ports {
        let identity = scope.jack(name);
        let Some(Node {
            details: Details::Jack(node),
            ..
        }) = graph.nodes.get(&identity)
        else {
            continue;
        };
        let hardware = node.hardware.clone();
        if port.direction == PortDirection::Output {
            for destination in &port.connections {
                let destination_identity = scope.jack(destination);
                if matches!(graph.nodes.get(&destination_identity).map(|node| &node.details), Some(Details::Jack(node)) if node.direction == PortDirection::Input)
                {
                    graph.link(&identity, &destination_identity, None, Evidence::Observed)?;
                } else {
                    graph.problems.push(format!("JACK on {} reports {name} connected to absent or invalid audio input {destination}", scope.host));
                }
            }
        }
        let Some(hardware) = hardware else {
            continue;
        };
        let devices = snapshot
            .card_links
            .iter()
            .filter(|(_, card)| **card == hardware.card)
            .map(|(device, _)| device)
            .collect::<Vec<_>>();
        if devices.len() > 1 {
            graph.problems.push(format!(
                "Sound card {} on {} has several Dante mappings",
                hardware.card, scope.host
            ));
            continue;
        }
        let Some(device) = devices.first() else {
            if snapshot
                .cards
                .iter()
                .any(|card| card.id == hardware.card && cards::is_dante(card))
            {
                graph.problems.push(format!("Sound card {} on {} is not linked to a Dante device; its signal trace stops there", hardware.card, scope.host));
            }
            continue;
        };
        let direction = match hardware.direction {
            cards::Direction::Capture => NetworkDirection::Receiver,
            cards::Direction::Playback => NetworkDirection::Transmitter,
        };
        if let Some(channel) = network.channel(device, direction, hardware.channel) {
            let note = Some(format!(
                "card {}{}",
                hardware.card,
                if scope.remote {
                    format!(" on {}", scope.host)
                } else {
                    String::new()
                }
            ));
            match hardware.direction {
                cards::Direction::Capture => {
                    graph.link(&channel, &identity, note, Evidence::Configured)?
                }
                cards::Direction::Playback => {
                    graph.link(&identity, &channel, note, Evidence::Configured)?
                }
            }
        } else {
            graph.problems.push(format!(
                "Sound card {} on {} maps to an absent or ambiguous Dante channel {} {} {}",
                hardware.card,
                scope.host,
                device,
                direction.name(),
                hardware.channel
            ));
        }
    }
    for (client, ports) in clients {
        if ports.inputs.is_empty()
            || ports.outputs.is_empty()
            || pulse_clients.contains(client)
            || snapshot.bridges.contains_key(client)
        {
            continue;
        }
        if ports.inputs.iter().chain(&ports.outputs).any(|name| matches!(&graph.nodes[&scope.jack(name)].details, Details::Jack(node) if node.hardware.is_some())) {
            continue;
        }
        if ports.inputs.len() == ports.outputs.len() {
            for (source, destination) in ports.inputs.iter().zip(&ports.outputs) {
                graph.link(
                    &scope.jack(source),
                    &scope.jack(destination),
                    Some(format!(
                        "through {client}, inferred from ports paired in order"
                    )),
                    Evidence::Inferred,
                )?;
            }
        } else {
            for source in &ports.inputs {
                for destination in &ports.outputs {
                    graph.link(
                        &scope.jack(source),
                        &scope.jack(destination),
                        Some(format!("through {client}, internal routing is inferred")),
                        Evidence::Inferred,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn pulse(
    graph: &mut Graph,
    scope: &Scope<'_>,
    snapshot: &HostSnapshot,
    clients: &BTreeMap<String, Ports>,
) -> Result<BTreeSet<String>, String> {
    let pulse = &snapshot.pulse;
    let mut jack_clients = BTreeSet::new();
    for (kind, entries, default_name, word, default_alias) in [
        (
            PulseKind::Sink,
            &pulse.sinks,
            &pulse.server.default_sink,
            "output",
            "default output",
        ),
        (
            PulseKind::Source,
            &pulse.sources,
            &pulse.server.default_source,
            "input",
            "default input",
        ),
    ] {
        for entry in entries {
            let name = entry
                .name
                .as_deref()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| format!("PulseAudio {word} on {} has no name", scope.host))?;
            let default = default_name.as_deref() == Some(name);
            let label = format!(
                "PulseAudio {word} {name}{}",
                if default { " (default)" } else { "" }
            );
            let mut names = BTreeMap::from([
                (name.to_owned(), 0),
                (format!("pulse {name}"), 0),
                (format!("{word} {name}"), 0),
            ]);
            if let Some(description) = &entry.description {
                names.insert(description.clone(), 1);
            }
            if default {
                names.insert(default_alias.to_owned(), 0);
            }
            graph.add(
                Node {
                    identity: scope.pulse(kind, name),
                    label: scope.label(label),
                    details: Details::Pulse(PulseNode {
                        host: scope.host.to_owned(),
                        kind,
                        entry: entry.clone(),
                    }),
                },
                scope.names(names),
            )?;
            if let Some(client) = entry
                .jack_client
                .as_ref()
                .filter(|client| clients.contains_key(*client))
            {
                jack_clients.insert(client.clone());
            }
        }
    }
    for entry in &pulse.sources {
        if let Some(monitor) = &entry.monitor_of {
            let source = scope.pulse(PulseKind::Sink, monitor);
            let destination = scope.pulse(
                PulseKind::Source,
                entry.name.as_deref().expect("validated source name"),
            );
            if graph.nodes.contains_key(&source) {
                graph.link(
                    &source,
                    &destination,
                    Some("monitor".to_owned()),
                    Evidence::Reported,
                )?;
            } else {
                graph.problems.push(format!(
                    "PulseAudio monitor on {} references absent output {monitor}",
                    scope.host
                ));
            }
        }
    }
    for (kind, entries) in [
        (PulseKind::Playback, &pulse.sink_inputs),
        (PulseKind::Recording, &pulse.source_outputs),
    ] {
        for entry in entries {
            let identity = scope.pulse(kind, &entry.index.to_string());
            let application = entry
                .application
                .as_deref()
                .or(entry.binary.as_deref())
                .unwrap_or("unknown application");
            let suffix = entry
                .media
                .as_deref()
                .filter(|media| !media.is_empty() && *media != application)
                .map(|media| format!(" “{media}”"))
                .unwrap_or_default();
            let label = format!(
                "{application} {} stream {}{suffix} (PulseAudio)",
                kind.name(),
                entry.index
            );
            let mut names = BTreeMap::from([
                (format!("stream {}", entry.index), 0),
                (format!("{} {}", kind.name(), entry.index), 0),
            ]);
            for value in [&entry.application, &entry.binary].into_iter().flatten() {
                names.insert(value.clone(), 1);
                names.insert(format!("{value} {}", kind.name()), 0);
            }
            if let Some(media) = &entry.media {
                names.insert(media.clone(), 2);
            }
            graph.add(
                Node {
                    identity: identity.clone(),
                    label: scope.label(label),
                    details: Details::Pulse(PulseNode {
                        host: scope.host.to_owned(),
                        kind,
                        entry: entry.clone(),
                    }),
                },
                scope.names(names),
            )?;
            let (device, device_kind) = match kind {
                PulseKind::Playback => (&entry.sink, PulseKind::Sink),
                _ => (&entry.source, PulseKind::Source),
            };
            if let Some(device) = device {
                let target = scope.pulse(device_kind, device);
                if graph.nodes.contains_key(&target) {
                    if kind == PulseKind::Playback {
                        graph.link(&identity, &target, None, Evidence::Reported)?;
                    } else {
                        graph.link(&target, &identity, None, Evidence::Reported)?;
                    }
                } else {
                    graph.problems.push(format!(
                        "PulseAudio {} stream {} on {} references absent {} {device}",
                        kind.name(),
                        entry.index,
                        scope.host,
                        device_kind.name()
                    ));
                }
            }
        }
    }
    Ok(jack_clients)
}

fn pulse_links(
    graph: &mut Graph,
    scope: &Scope<'_>,
    snapshot: &HostSnapshot,
    clients: &BTreeMap<String, Ports>,
) -> Result<(), String> {
    for (kind, entries) in [
        (PulseKind::Sink, &snapshot.pulse.sinks),
        (PulseKind::Source, &snapshot.pulse.sources),
    ] {
        for entry in entries {
            let Some((client, ports)) = entry
                .jack_client
                .as_deref()
                .and_then(|client| clients.get(client).map(|ports| (client, ports)))
            else {
                continue;
            };
            let identity = scope.pulse(
                kind,
                entry.name.as_deref().expect("validated endpoint name"),
            );
            let note = Some(format!("JACK client {client}"));
            if kind == PulseKind::Sink {
                for port in &ports.outputs {
                    graph.link(
                        &identity,
                        &scope.jack(port),
                        note.clone(),
                        Evidence::Reported,
                    )?;
                }
            } else {
                for port in &ports.inputs {
                    graph.link(
                        &scope.jack(port),
                        &identity,
                        note.clone(),
                        Evidence::Reported,
                    )?;
                }
            }
        }
    }
    Ok(())
}
