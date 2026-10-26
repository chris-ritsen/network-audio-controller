use super::*;
use serde_json::json;

pub fn joined(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [name] => name.clone(),
        _ => format!(
            "{} and {}",
            names[..names.len() - 1].join(", "),
            names.last().expect("nonempty names")
        ),
    }
}

fn subject(point: &str) -> String {
    let mut characters = point.chars();
    let Some(first) = characters.next() else {
        return String::new();
    };
    if first.is_lowercase()
        && point
            .chars()
            .all(|character| character.is_alphabetic() || character == ' ')
    {
        format!("{}{}", first.to_uppercase(), characters.as_str())
    } else {
        point.to_owned()
    }
}

fn unique(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

pub fn stage_name(node: &JackNode, local_host: &str) -> String {
    let client = node.port.split(':').next().unwrap_or(&node.port);
    let name = node
        .stage
        .as_ref()
        .and_then(|stage| stage.name.as_deref())
        .map(|name| format!(" ({name})"))
        .unwrap_or_default();
    let host = if node.host == local_host {
        String::new()
    } else {
        format!(" on {}", node.host)
    };
    format!("{client}{name}{host}")
}

#[derive(Clone, Debug)]
pub struct Path {
    pub name: String,
    pub starts: Vec<String>,
    pub walks: BTreeMap<String, Walk>,
}

impl Graph {
    pub fn path(&self, name: &str, starts: Vec<String>, directions: &[Direction]) -> Path {
        let walks = directions
            .iter()
            .map(|direction| {
                let name = match direction {
                    Direction::Upstream => "upstream",
                    Direction::Downstream => "downstream",
                };
                (name.to_owned(), self.walk(&starts, *direction, 80))
            })
            .collect();
        Path {
            name: name.to_owned(),
            starts,
            walks,
        }
    }

    pub fn path_identities(&self, path: &Path) -> Vec<String> {
        unique(
            path.starts.iter().cloned().chain(
                path.walks
                    .values()
                    .flat_map(|walk| walk.steps.iter().map(|step| step.identity.clone())),
            ),
        )
    }

    pub fn related_paths(&self, identities: &[String]) -> Vec<Path> {
        let mut groups = BTreeMap::<String, Vec<String>>::new();
        for identity in identities {
            if !self.upstream.contains_key(identity) && !self.downstream.contains_key(identity) {
                continue;
            }
            let node = &self.nodes[identity];
            let base = plain_label(&node.label);
            let name = match &node.details {
                Details::Jack(jack) if !base.contains(" [") => {
                    let client = jack
                        .port
                        .rsplit_once(':')
                        .map_or(jack.port.as_str(), |(client, _)| client);
                    format!("{} {client}", jack.host)
                }
                _ => base.to_owned(),
            };
            groups.entry(name).or_default().push(identity.clone());
        }
        let has_outputs = groups.values().any(|group| {
            group
                .iter()
                .all(|identity| self.nodes[identity].is_output())
        });
        groups
            .into_iter()
            .filter(|(_, group)| {
                !has_outputs
                    || group
                        .iter()
                        .all(|identity| self.nodes[identity].is_output())
            })
            .take(3)
            .map(|(name, starts)| {
                self.path(&name, starts, &[Direction::Upstream, Direction::Downstream])
            })
            .collect()
    }

    pub fn render(
        &self,
        walk: &Walk,
        direction: Direction,
        levels: &Levels,
    ) -> (Vec<String>, Vec<String>) {
        let arrow = match direction {
            Direction::Upstream => "←",
            Direction::Downstream => "→",
        };
        let mut lines = Vec::new();
        let mut stops = Vec::new();
        for step in &walk.steps {
            let node = &self.nodes[&step.identity];
            let mut text = format!(
                "{}{arrow} {}",
                "  ".repeat(step.depth.saturating_sub(1)),
                node.label
            );
            if let Some(note) = &step.note {
                text.push_str(&format!(
                    " {}{note}",
                    if note.starts_with("through ") {
                        ""
                    } else {
                        "via "
                    }
                ));
            }
            if step.repeated {
                text.push_str(" (shown above)");
            } else if let Some(level) = levels.get(&step.identity) {
                text.push_str(&format!(": {}", level.text));
            }
            if direction == Direction::Downstream && !step.repeated {
                if let Details::Jack(jack) = &node.details {
                    if !jack.mixes.is_empty() {
                        text.push_str(&format!(" (a mix of {})", jack.mixes.join(", ")));
                    }
                }
            }
            lines.push(text);
            if step.repeated {
                continue;
            }
            let (before, after) = match direction {
                Direction::Downstream => (&step.parent, &step.identity),
                Direction::Upstream => (&step.identity, &step.parent),
            };
            if levels
                .get(before)
                .is_some_and(|level| level.state.has_signal())
                && levels
                    .get(after)
                    .is_some_and(|level| level.state == State::Silent)
            {
                let suffix = if step.evidence == Evidence::Inferred {
                    "; the internal route is inferred"
                } else {
                    ""
                };
                stops.push(format!(
                    "{} is silent although {} has signal{suffix}",
                    plain_label(&self.nodes[after].label),
                    plain_label(&self.nodes[before].label)
                ));
            }
        }
        (lines, unique(stops))
    }

    pub fn muted_stages(&self, identities: &[String]) -> Vec<String> {
        unique(identities.iter().filter_map(|identity| {
            match &self.nodes[identity].details {
                Details::Jack(node)
                    if node.direction == PortDirection::Output
                        && node
                            .stage
                            .as_ref()
                            .is_some_and(|stage| stage.muted == Some(true)) =>
                {
                    Some(stage_name(node, &self.local_host))
                }
                _ => None,
            }
        }))
    }

    pub fn destinations(&self, walk: &Walk, levels: Option<&Levels>) -> Vec<String> {
        unique(
            walk.steps
                .iter()
                .filter(|step| !step.repeated && !self.downstream.contains_key(&step.identity))
                .filter(|step| {
                    levels.is_none_or(|levels| {
                        levels
                            .get(&step.identity)
                            .is_some_and(|level| level.state.has_signal())
                    })
                })
                .filter_map(|step| {
                    let node = &self.nodes[&step.identity];
                    match &node.details {
                        Details::Pulse(pulse)
                            if pulse.kind == PulseKind::Recording
                                && pulse.entry.media.as_deref() == Some("Peak detect") =>
                        {
                            let parent = &self.nodes[&step.parent];
                            match &parent.details {
                                Details::Pulse(source)
                                    if source.kind == PulseKind::Source
                                        && source.entry.monitor_of.is_none() =>
                                {
                                    Some(plain_label(&parent.label).to_owned())
                                }
                                _ => None,
                            }
                        }
                        _ => Some(plain_label(&node.label).to_owned()),
                    }
                }),
        )
    }

    pub fn level_changes(&self, path: &Path, levels: &Levels) -> Vec<String> {
        unique(path.walks.iter().flat_map(|(direction, walk)| walk.steps.iter().filter_map(move |step| {
            let note = step.note.as_deref()?.strip_prefix("through ")?;
            if step.repeated { return None; }
            let (before, after) = if direction == "downstream" { (&step.parent, &step.identity) } else { (&step.identity, &step.parent) };
            let first = levels.get(before)?;
            let second = levels.get(after)?;
            if first.state != State::Signal || second.state != State::Signal { return None; }
            let first = first.peak_dbfs?;
            let second = second.peak_dbfs?;
            if (second - first).abs() < 3.0 { return None; }
            let client = note.split(',').next().unwrap_or(note);
            Some(format!("{client}: observed output peak is {:.0} dB {} than input ({first} to {second} dBFS); internal routing is inferred", (second - first).abs(), if second < first { "lower" } else { "higher" }))
        })))
    }

    pub fn source_causes(&self, path: &Path, levels: &Levels) -> Vec<String> {
        let identities = self.path_identities(path);
        if identities.iter().any(|identity| {
            levels
                .get(identity)
                .is_some_and(|level| level.state.has_signal())
        }) {
            return Vec::new();
        }
        let roots = identities
            .iter()
            .filter(|identity| !self.upstream.contains_key(*identity));
        unique(roots.map(|identity| {
            let node = &self.nodes[identity];
            let name = plain_label(&node.label);
            let state = levels
                .get(identity)
                .map_or(State::Unknown, |level| level.state);
            let level = levels
                .get(identity)
                .map_or("not measured", |level| level.text.as_str());
            match &node.details {
                Details::Dante(dante) if dante.online == Some(false) => {
                    let power = dante
                        .outlet
                        .as_ref()
                        .filter(|outlet| outlet["state"] == "off")
                        .and_then(|outlet| outlet["name"].as_str())
                        .map(|outlet| format!(", with outlet {outlet} reporting off"))
                        .unwrap_or_default();
                    format!("{} is off the network{power}", dante.device)
                }
                Details::Pulse(pulse) if pulse.entry.corked == Some(true) => {
                    format!("{name} is paused")
                }
                Details::Pulse(pulse) if pulse.entry.mute == Some(true) => {
                    format!("{name} is muted")
                }
                Details::Pulse(pulse) if pulse.kind == PulseKind::Sink => {
                    format!("nothing is playing to {name}")
                }
                _ if state == State::Silent => format!("{name}: {level}"),
                _ => format!("{name} could not be measured: {level}"),
            }
        }))
    }

    pub fn answer(&self, path: &Path, levels: Option<&Levels>, stops: &[String]) -> String {
        let subject = subject(&path.name);
        let connected = path.walks.values().any(|walk| !walk.steps.is_empty());
        if !connected {
            return format!("Nothing is connected to {}.", path.name);
        }
        let Some(levels) = levels else {
            if let Some(walk) = path.walks.get("downstream") {
                let destinations = self.destinations(walk, None);
                if !destinations.is_empty() {
                    return format!("{subject} reaches {}.", joined(&destinations));
                }
            }
            let sources = path
                .walks
                .get("upstream")
                .into_iter()
                .flat_map(|walk| &walk.steps)
                .filter(|step| !step.repeated && !self.upstream.contains_key(&step.identity))
                .map(|step| plain_label(&self.nodes[&step.identity].label).to_owned())
                .collect::<Vec<_>>();
            return if sources.is_empty() {
                format!("The known connections for {} are shown below.", path.name)
            } else {
                format!("{subject} is fed by {}.", joined(&sources))
            };
        };
        let signal = path.starts.iter().any(|identity| {
            levels
                .get(identity)
                .is_some_and(|level| level.state.has_signal())
        });
        let unknown = path.starts.iter().any(|identity| {
            levels
                .get(identity)
                .is_none_or(|level| level.state == State::Unknown)
        });
        if signal {
            if let Some(stop) = stops.first() {
                return format!("{subject} has signal. {stop}.");
            }
            if let Some(walk) = path.walks.get("downstream") {
                let destinations = self.destinations(walk, Some(levels));
                if !destinations.is_empty() {
                    return format!(
                        "{subject} has signal, also observed at {}.",
                        joined(&destinations)
                    );
                }
            }
            return format!("{subject} has signal.");
        }
        let causes = self.source_causes(path, levels);
        let conclusion = if unknown {
            format!("{subject}'s level could not be fully measured.")
        } else {
            format!("{subject} has no measured signal.")
        };
        if !causes.is_empty() {
            format!("{conclusion} {}.", causes.join("; "))
        } else if let Some(stop) = stops.first() {
            format!("{conclusion} {stop}.")
        } else {
            conclusion
        }
    }

    pub fn trace(&self, path: &Path, levels: Option<&Levels>) -> Value {
        let empty = Levels::new();
        let measured = levels.unwrap_or(&empty);
        let starts = path
            .starts
            .iter()
            .map(|identity| {
                let label = &self.nodes[identity].label;
                measured
                    .get(identity)
                    .map_or_else(|| label.clone(), |level| format!("{label}: {}", level.text))
            })
            .collect::<Vec<_>>();
        let mut result = json!({"host":self.local_host,"point":path.name,"start":starts});
        let mut stops = Vec::new();
        for (name, walk) in &path.walks {
            let direction = if name == "downstream" {
                Direction::Downstream
            } else {
                Direction::Upstream
            };
            let (mut lines, found) = self.render(walk, direction, measured);
            if lines.is_empty() {
                lines.push(format!("nothing {name} of this point is known"));
            }
            result[name] = json!(lines);
            stops.extend(found);
            if walk.remaining > 0 {
                result[format!("more_{name}")] = json!(format!(
                    "{} more nodes not shown; trace from a later point to see them",
                    walk.remaining
                ));
            }
        }
        stops = unique(stops);
        result["answer"] = json!(self.answer(path, levels, &stops));
        if let Some(walk) = path.walks.get("downstream") {
            let destinations = self.destinations(walk, None);
            if !destinations.is_empty() {
                result["reaches"] = json!(format!("It ends at: {}.", destinations.join("; ")));
            }
        }
        if levels.is_some() {
            let identities = self.path_identities(path);
            let unknown = identities
                .iter()
                .filter(|identity| {
                    measured
                        .get(*identity)
                        .is_none_or(|level| level.state == State::Unknown)
                })
                .map(|identity| self.nodes[identity].label.clone())
                .collect::<Vec<_>>();
            let causes = self.source_causes(path, measured);
            if !causes.is_empty() && unknown.is_empty() {
                result["why_silent"] = json!(causes.join("; "));
            }
            result["signal_stops"] = if stops.is_empty() {
                json!("no measured transition from signal to silence")
            } else {
                json!(stops)
            };
            if !unknown.is_empty() {
                result["unmeasured"] = json!(unknown);
            }
            let changes = self.level_changes(path, measured);
            if !changes.is_empty() {
                result["level_changes"] = json!(changes);
            }
        }
        result
    }
}
