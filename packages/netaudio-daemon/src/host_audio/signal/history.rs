use super::*;
use crate::monitoring::level_history::Period;

impl Service {
    fn history_target(
        &self,
        graph: &Graph,
        identity: &str,
        hosts: &BTreeMap<String, HostSnapshot>,
    ) -> (String, Vec<[String; 2]>, Option<String>) {
        let node = &graph.nodes[identity];
        match &node.details {
            Details::Dante(node) => {
                let keys = node
                    .server_name
                    .as_ref()
                    .zip(node.channel)
                    .map(|(server, number)| {
                        vec![[
                            format!("dante:{server}"),
                            format!("{}:{number}", node.direction.name()),
                        ]]
                    })
                    .unwrap_or_default();
                return (self.manager.host.clone(), keys, None);
            }
            Details::Wireless(node) => {
                if node.device["device_type"] != "ad4d" {
                    return (
                        self.manager.host.clone(),
                        Vec::new(),
                        Some(
                            "Its input meter is not recorded because its units are not known"
                                .to_owned(),
                        ),
                    );
                }
                return (
                    self.manager.host.clone(),
                    vec![[format!("shure:{}", node.address), node.number.to_string()]],
                    None,
                );
            }
            _ => {}
        }
        let measured = graph.stream_device(identity).unwrap_or(identity);
        let note =
            (measured != identity).then(|| format!("measured at {}", graph.nodes[measured].label));
        let targets = graph.meter_targets(&[measured.to_owned()], hosts);
        match targets.into_iter().next() {
            Some((host, ports)) => {
                let sources = ports.into_values().flatten().collect::<BTreeSet<_>>();
                (
                    host,
                    sources
                        .into_iter()
                        .map(|source| ["jack".to_owned(), source])
                        .collect(),
                    note,
                )
            }
            None => (self.manager.host.clone(), Vec::new(), note),
        }
    }

    async fn summaries(
        &self,
        context: &Context,
        identities: &[String],
        period: Period,
    ) -> Result<(
        BTreeMap<String, Value>,
        BTreeMap<String, String>,
        Vec<String>,
    )> {
        let mut requests = BTreeMap::<String, BTreeMap<String, Vec<[String; 2]>>>::new();
        let mut notes = BTreeMap::new();
        for identity in identities {
            let (host, keys, note) =
                self.history_target(&context.graph, identity, &context.snapshots);
            if let Some(note) = note {
                notes.insert(identity.clone(), note);
            }
            if keys.len() > 64 {
                notes.insert(identity.clone(), "This point exceeds the 64-source history summary budget; query its source ports separately".to_owned());
            } else if !keys.is_empty() {
                requests
                    .entry(host)
                    .or_default()
                    .insert(identity.clone(), keys);
            }
        }
        let mut summaries = BTreeMap::new();
        let mut problems = Vec::new();
        for (host, targets) in requests {
            if host == self.manager.host {
                let recorder = &context.raw[&host]["level_recorder"];
                for (identity, keys) in targets {
                    let references = keys
                        .iter()
                        .map(|key| (key[0].as_str(), key[1].as_str()))
                        .collect::<Vec<_>>();
                    let mut summary = serde_json::to_value(self.manager.level_history.summary(
                        &references,
                        period,
                        chrono::Utc::now().timestamp(),
                    )?)?;
                    if references.iter().any(|(source, _)| *source == "jack")
                        && recorder["recording"] != true
                    {
                        summary["not_recording"] = recorder["reason"].clone();
                    }
                    summaries.insert(identity, summary);
                }
            } else {
                let targets = targets.into_iter().collect::<Vec<_>>();
                for batch in targets.chunks(32) {
                    let targets = batch.iter().cloned().collect::<BTreeMap<_, _>>();
                    match self
                        .manager
                        .peers
                        .request(
                            &host,
                            Resource::History(
                                json!({"targets":targets,"start":period.start,"end":period.end}),
                            ),
                            Duration::from_secs(3),
                        )
                        .await
                    {
                        Ok((200, Value::Object(values))) => {
                            for (identity, _) in batch {
                                if let Some(value) =
                                    values.get(identity).filter(|value| value.is_object())
                                {
                                    summaries.insert(identity.clone(), value.clone());
                                } else {
                                    notes.insert(
                                        identity.clone(),
                                        "The peer returned no valid history summary".to_owned(),
                                    );
                                }
                            }
                        }
                        Ok((status, _)) => problems
                            .push(format!("History on {host} is unavailable (HTTP {status})")),
                        Err(error) => {
                            problems.push(format!("History on {host} could not be read: {error}"))
                        }
                    }
                }
            }
        }
        Ok((summaries, notes, problems))
    }

    pub(super) async fn point_history(
        &self,
        mut context: Context,
        resolved: BTreeMap<String, Vec<String>>,
        errors: BTreeMap<String, String>,
        request: crate::mcp::views::signal_history::HistoryRequest,
        status: u16,
    ) -> Result<Outcome> {
        let identities = unique(resolved.values().flatten().cloned());
        let mut live = None;
        if let Some(listen) = request.listen {
            let targets = unique(identities.iter().filter_map(|identity| {
                match &context.graph.nodes[identity].details {
                    Details::Dante(node) if node.online == Some(true) => node.record.clone(),
                    _ => None,
                }
            }));
            let mut references = Vec::new();
            for target in targets {
                match self.metering.acquire(&target) {
                    Ok(reference) => references.push(reference),
                    Err(error) => context.notes.push(format!("{target}: {error}")),
                }
            }
            let meterable =
                unique(identities.iter().filter_map(|identity| {
                    context.graph.stream_device(identity).map(str::to_owned)
                }));
            let stop = tokio::time::Instant::now() + listen;
            live = Some(
                self.measure(&mut context, &meterable, listen.as_secs_f64().min(5.0))
                    .await?,
            );
            tokio::select! {
                _ = self.cancellation.cancelled() => anyhow::bail!("Signal listening stopped with the daemon"),
                _ = tokio::time::sleep_until(stop) => {}
            }
            drop(references);
            if listen > Duration::from_secs(5) {
                context.notes.push("Finite JACK levels sampled the first five seconds; retained history covers the requested period where recording is enabled".to_owned());
            }
        }
        let (summaries, notes, problems) = self
            .summaries(&context, &identities, request.period)
            .await?;
        context.notes.extend(problems);
        let mut body = json!({"period":format!("{} to {}", presentation::time(request.period.start), presentation::time(request.period.end))});
        for (point, starts) in resolved {
            let mut lines = Vec::new();
            for identity in starts {
                let label = &context.graph.nodes[&identity].label;
                let summary = summaries.get(&identity);
                let measured = context.graph.stream_device(&identity).unwrap_or(&identity);
                let reading = live.as_ref().and_then(|live| live.get(measured));
                let text = if summary
                    .is_none_or(|summary| summary["recorded_seconds"].as_u64().unwrap_or(0) == 0)
                {
                    reading
                        .map(|reading| format!("finite sample: {}", reading.text))
                        .unwrap_or_else(|| {
                            notes.get(&identity).cloned().unwrap_or_else(|| {
                                "no level history is kept for this point".to_owned()
                            })
                        })
                } else {
                    history_text(summary.expect("checked summary"))
                };
                let note = notes
                    .get(&identity)
                    .map(|note| format!(" ({note})"))
                    .unwrap_or_default();
                lines.push(format!("{label}{note}: {text}"));
            }
            body[point] = json!(lines);
        }
        for (point, error) in errors {
            body[point] = json!(error);
        }
        self.finish(&context, &mut body);
        Ok(Outcome { status, body })
    }

    pub(super) fn recorded_changes(
        &self,
        graph: &Graph,
        path: &signal::Path,
    ) -> Result<Vec<String>> {
        if !self.manager.record_jack_levels {
            return Ok(Vec::new());
        }
        let now = chrono::Utc::now().timestamp();
        let period = Period::new(now - 900, now)?;
        let mut changes = Vec::new();
        for (direction, walk) in &path.walks {
            for step in &walk.steps {
                let Some(client) = step
                    .note
                    .as_deref()
                    .and_then(|note| note.strip_prefix("through "))
                    .map(|note| note.split(',').next().unwrap_or(note))
                else {
                    continue;
                };
                if step.repeated {
                    continue;
                }
                let (inside, outside) = if direction == "downstream" {
                    (&step.parent, &step.identity)
                } else {
                    (&step.identity, &step.parent)
                };
                let sources = graph
                    .upstream
                    .get(inside)
                    .into_iter()
                    .flatten()
                    .filter_map(|edge| match &graph.nodes[&edge.identity].details {
                        Details::Jack(node) => Some(node),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let ([source], Details::Jack(output)) =
                    (sources.as_slice(), &graph.nodes[outside].details)
                else {
                    continue;
                };
                if source.host != self.manager.host || output.host != self.manager.host {
                    continue;
                }
                if let Some((difference, count, last)) = self
                    .manager
                    .level_history
                    .paired_difference(("jack", &source.port), ("jack", &output.port), period)?
                    && difference.abs() >= 3.0
                {
                    changes.push(format!("{client}: recorded output peak was {:.0} dB {} than input (median over {count} paired signal seconds, last {}); internal routing is inferred", difference.abs(), if difference < 0.0 { "lower" } else { "higher" }, presentation::time(last)));
                }
            }
        }
        Ok(unique(changes))
    }
}

fn history_text(summary: &Value) -> String {
    let recorded = summary["recorded_seconds"].as_u64().unwrap_or(0);
    let signal = summary["signal_seconds"].as_u64();
    let mut text = signal.map_or_else(
        || format!("{recorded} recorded seconds; signal presence unknown"),
        |signal| format!("signal in {signal} of {recorded} recorded seconds"),
    );
    if let Some(peak) = summary["loudest_dbfs"].as_f64() {
        text.push_str(&format!(", loudest {peak} dBFS"));
    }
    for (field, label) in [
        ("loudest_at", "loudest at"),
        ("last_signal_at", "last signal"),
    ] {
        if let Some(value) = summary[field]
            .as_str()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        {
            text.push_str(&format!(
                ", {label} {}",
                presentation::time(value.timestamp())
            ));
        }
    }
    if let Some(note) = summary["not_recording"].as_str() {
        text.push_str(&format!("; {note}"));
    }
    if summary["peak_coverage_complete"] == false {
        text.push_str("; peak coverage is incomplete at the period boundary");
    }
    text
}
