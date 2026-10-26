use super::super::observations::DisplayReading;
use super::*;

pub(super) fn time(second: i64) -> String {
    chrono::DateTime::from_timestamp(second, 0)
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "unknown time".to_owned())
}

pub(super) fn now() -> String {
    time(chrono::Utc::now().timestamp())
}

pub(super) fn applications(hosts: &BTreeMap<String, HostSnapshot>) -> BTreeMap<String, String> {
    hosts
        .iter()
        .map(|(host, snapshot)| {
            let names = snapshot
                .pulse
                .sink_inputs
                .iter()
                .chain(&snapshot.pulse.source_outputs)
                .filter_map(|entry| entry.application.as_ref().or(entry.binary.as_ref()))
                .cloned()
                .collect::<BTreeSet<_>>();
            (
                host.clone(),
                if names.is_empty() {
                    "none".to_owned()
                } else {
                    names.into_iter().collect::<Vec<_>>().join(", ")
                },
            )
        })
        .collect()
}

pub(super) fn volume_stages(graph: &Graph, authority: Option<&str>) -> Vec<String> {
    unique(graph.nodes.values().filter_map(|node| match &node.details {
        Details::Jack(node) => {
            let stage = node.stage.as_ref()?;
            let name = signal::stage_name(node, &graph.local_host);
            let level = stage
                .level
                .map(|level| format!("{level}%"))
                .unwrap_or_else(|| "level unknown".to_owned());
            let mute = match stage.muted {
                Some(true) => "muted",
                Some(false) => "not muted",
                None => "mute unknown",
            };
            let authority = authority
                .map(|authority| format!("; changed through {authority}"))
                .unwrap_or_else(|| "; NetAudio cannot change this stage".to_owned());
            Some(format!("{name}: {level}, {mute}{authority}"))
        }
        _ => None,
    }))
}

pub(super) fn display_answer(
    value: &mut Value,
    graph: &Graph,
    reading: &DisplayReading,
    presented: &BTreeMap<String, Vec<String>>,
    levels: Option<&Levels>,
) {
    let mut sentences = Vec::new();
    let mut displays = serde_json::Map::new();
    for presentation in &reading.presentations {
        let display = presentation.display.to_uppercase();
        let picture = if presentation.picture == Some("off") {
            ", and its picture is off"
        } else {
            ""
        };
        sentences.push(format!(
            "The {display} is showing {} through {}{picture}.",
            presentation.host, presentation.endpoint
        ));
        let point = format!("{} {}", presentation.host, presentation.output);
        displays.insert(display, json!({"showing":presentation.host,"through":presentation.endpoint,"picture":presentation.picture,"output":point}));
        if let Some(starts) = presented.get(&point) {
            let path = graph.path(
                &point,
                starts.clone(),
                &[Direction::Upstream, Direction::Downstream],
            );
            sentences.push(
                graph.trace(&path, levels)["answer"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            );
        }
    }
    if reading.presentations.is_empty() {
        sentences.push(if reading.problems.is_empty() {
            "No configured computer has a current confirmed presentation on that display."
                .to_owned()
        } else {
            reading.problems.join("; ")
        });
    }
    if !reading.problems.is_empty() {
        displays.insert("problems".to_owned(), json!(reading.problems));
    }
    if presented.is_empty()
        && let Some(answer) = value["answer"].as_str()
    {
        sentences.push(answer.to_owned());
    }
    value["answer"] = json!(sentences.join(" "));
    value["display"] = Value::Object(displays);
}

impl Service {
    pub async fn find(&self, query: &str) -> Result<Option<Value>> {
        validate_point(query)?;
        let mut context = self.context().await?;
        let identities = context.graph.find(query);
        if identities.is_empty() {
            return Ok(None);
        }
        let shown = identities.iter().take(24).cloned().collect::<Vec<_>>();
        let measured = self.measure(&mut context, &shown, 0.5).await?;
        let mut lines = Vec::new();
        for identity in &shown {
            let mut line = context.graph.nodes[identity].label.clone();
            for (edges, arrow) in [
                (&context.graph.upstream, "←"),
                (&context.graph.downstream, "→"),
            ] {
                if let Some(edges) = edges.get(identity) {
                    let names = edges
                        .iter()
                        .take(8)
                        .map(|edge| context.graph.nodes[&edge.identity].label.clone())
                        .collect::<Vec<_>>();
                    line.push_str(&format!(" {arrow} {}", names.join(", ")));
                    if edges.len() > 8 {
                        line.push_str(&format!(" and {} more", edges.len() - 8));
                    }
                }
            }
            if let Some(level) = measured.get(identity) {
                line.push_str(&format!(": {}", level.text));
            }
            lines.push(line);
        }
        let mut value = json!({"matches":lines,"omitted":identities.len() - shown.len(),"measured_at":now(),"trace":"trace_signal follows any of these through the chain"});
        self.finish(&context, &mut value);
        Ok(Some(value))
    }

    async fn playing(&self, context: &mut Context, seconds: f64) -> Result<Value> {
        let sources = context
            .graph
            .nodes
            .iter()
            .filter_map(|(identity, node)| match &node.details {
                Details::Jack(node)
                    if node.direction == netaudio_core::host_audio::snapshot::Direction::Output
                        && context.graph.downstream.contains_key(identity)
                        && !context.graph.upstream.get(identity).is_some_and(|edges| {
                            edges.iter().any(|edge| {
                                matches!(
                                    context.graph.nodes[&edge.identity].details,
                                    Details::Jack(_)
                                )
                            })
                        }) =>
                {
                    Some(identity.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let measured = self.measure(context, &sources, seconds).await?;
        let mut computers = context
            .snapshots
            .keys()
            .map(|host| (host.clone(), Vec::new()))
            .collect::<BTreeMap<_, _>>();
        let mut unknown = BTreeSet::new();
        for identity in sources {
            let Details::Jack(node) = &context.graph.nodes[&identity].details else {
                continue;
            };
            let level = measured.get(&identity);
            if level.is_none_or(|level| level.state == signal::State::Unknown) {
                unknown.insert(node.host.clone());
            }
            if let Some(level) = level.filter(|level| level.state.has_signal()) {
                computers
                    .entry(node.host.clone())
                    .or_default()
                    .push(format!(
                        "{}: {}",
                        signal::plain_label(&context.graph.nodes[&identity].label),
                        level.text
                    ));
            }
        }
        let computers = computers
            .into_iter()
            .map(|(host, lines)| {
                let value = if !lines.is_empty() {
                    json!(lines)
                } else if unknown.contains(&host) {
                    json!("No signal was observed, but some source levels could not be measured")
                } else if !context.snapshots[&host].jack.available {
                    json!("JACK is unavailable; playing audio could not be measured")
                } else {
                    json!("No source signal was observed during this measurement")
                };
                (host, value)
            })
            .collect::<BTreeMap<_, _>>();
        let mut value = json!({"computers":computers,"measured_at":now()});
        self.finish(context, &mut value);
        Ok(value)
    }

    pub async fn now_playing(&self, seconds: f64) -> Result<Value> {
        let mut context = self.context().await?;
        self.playing(&mut context, seconds).await
    }

    pub async fn computers(&self) -> Result<BTreeMap<String, String>> {
        let context = self.context().await?;
        Ok(context
            .snapshots
            .into_iter()
            .flat_map(|(host, snapshot)| {
                snapshot.card_links.into_iter().map(move |(device, card)| {
                    (device, format!("{host}, as its sound card {card}"))
                })
            })
            .collect())
    }

    fn context_conditions(&self, context: &Context) -> Result<Vec<String>> {
        let records = self.model.records()?;
        let mut conditions = context.graph.problems.clone();
        for raw in context.raw.values() {
            conditions.extend(crate::mcp::views::host_audio::conditions(raw, &records));
        }
        conditions.extend(crate::mcp::views::wireless::conditions(
            &self.shure.records()?,
        ));
        conditions.extend(context.notes.clone());
        Ok(unique(conditions))
    }

    pub async fn conditions(&self) -> Result<Vec<String>> {
        let context = self.context().await?;
        self.context_conditions(&context)
    }

    pub async fn overview(&self) -> Result<Value> {
        let mut context = self.context().await?;
        let mut value = json!({});
        let conditions = self.context_conditions(&context)?;
        if !conditions.is_empty() {
            value["other_problems"] = json!(conditions);
        }
        let playing = self.playing(&mut context, 0.5).await?;
        value["playing_now"] = playing["computers"].clone();
        if let Some(notes) = playing.get("notes") {
            value["playing_notes"] = notes.clone();
        }
        let mut muted = Vec::new();
        for (host, raw) in &context.raw {
            let summary = crate::mcp::views::host_audio::view(raw, &json!({"section":"summary"}))
                .map_err(|error| anyhow::anyhow!(error.message))?;
            if host == &self.manager.host {
                value["host_audio"] = summary;
            } else {
                if !value["other_computers_audio"].is_object() {
                    value["other_computers_audio"] = json!({});
                }
                value["other_computers_audio"][host] = summary;
            }
        }
        for node in context.graph.nodes.values() {
            if let Details::Pulse(node) = &node.details
                && node.entry.mute == Some(true)
            {
                muted.push(format!(
                    "{} {} on {}",
                    node.kind.name(),
                    node.entry
                        .name
                        .as_deref()
                        .or(node.entry.application.as_deref())
                        .unwrap_or("unnamed"),
                    node.host
                ));
            }
        }
        muted.extend(
            context
                .graph
                .muted_stages(&context.graph.nodes.keys().cloned().collect::<Vec<_>>()),
        );
        if !muted.is_empty() {
            value["muted"] = json!(unique(muted));
        }
        self.finish(&context, &mut value);
        Ok(value)
    }

    pub async fn channels_carry(
        &self,
        sources: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, String>> {
        let context = self.context().await?;
        let mut carried = BTreeMap::new();
        for source in sources {
            if let Some((device, channel)) = source.split_once(':')
                && let Some(value) = Self::carried(&context, device, channel)
            {
                carried.insert(source.clone(), value);
            }
        }
        Ok(carried)
    }

    fn carried(context: &Context, device: &str, channel: &str) -> Option<String> {
        let starts = context
            .graph
            .nodes
            .iter()
            .filter_map(|(identity, node)| match &node.details {
                Details::Dante(node)
                    if node.direction == NetworkDirection::Transmitter
                        && node.device == device
                        && (node
                            .channel
                            .is_some_and(|number| channel.parse::<u32>() == Ok(number))
                            || signal::plain_label(&context.graph.nodes[identity].label)
                                .ends_with(&format!(" {channel}"))) =>
                {
                    Some(identity.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if starts.len() != 1 {
            return None;
        }
        let walk = context.graph.walk(&starts, Direction::Upstream, 80);
        let mut first = None;
        for step in walk.steps {
            let Details::Jack(node) = &context.graph.nodes[&step.identity].details else {
                continue;
            };
            let name = |port: &str| {
                let identity = format!("jack@{}:{port}", node.host);
                context
                    .graph
                    .nodes
                    .get(&identity)
                    .and_then(|node| match &node.details {
                        Details::Jack(node) => node.stage.as_ref()?.name.clone(),
                        _ => None,
                    })
                    .unwrap_or_else(|| port.split(':').next().unwrap_or(port).to_owned())
            };
            if !node.mixes.is_empty() {
                let names = unique(node.mixes.iter().map(|port| name(port)));
                let mix = format!("a mix of {}", signal::joined(&names));
                return Some(first.map_or(mix.clone(), |first| format!("{first}, {mix}")));
            }
            if first.is_none()
                && node.direction == netaudio_core::host_audio::snapshot::Direction::Output
                && !node.port.starts_with("system:")
            {
                first = Some(name(&node.port));
            }
        }
        first
    }
}
