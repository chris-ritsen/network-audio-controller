use super::*;
use netaudio_core::host_audio::measurement::{MAXIMUM_SOURCES, MAXIMUM_TARGETS, Request};
use netaudio_core::host_audio::signal::observation::{Reading, Readings};

impl Service {
    async fn host_meter(
        &self,
        host: &str,
        targets: BTreeMap<String, Vec<String>>,
        seconds: f64,
    ) -> Readings {
        let mut readings = Readings::new();
        let mut batches = Vec::new();
        let mut batch = BTreeMap::new();
        let mut source_count = 0;
        for (name, sources) in targets {
            if sources.len() > MAXIMUM_SOURCES {
                readings.insert(
                    name,
                    Reading::Unavailable {
                        error: format!(
                            "This target exceeds the {MAXIMUM_SOURCES}-source measurement budget"
                        ),
                    },
                );
                continue;
            }
            if batch.len() >= MAXIMUM_TARGETS || source_count + sources.len() > MAXIMUM_SOURCES {
                batches.push(std::mem::take(&mut batch));
                source_count = 0;
            }
            source_count += sources.len();
            batch.insert(name, sources);
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
        for targets in batches {
            let names = targets.keys().cloned().collect::<Vec<_>>();
            let request = Request {
                targets,
                seconds: seconds.clamp(0.05, 5.0),
            };
            let result = async {
                let outcome = if host == self.manager.host {
                    self.manager.measure(request).await?
                } else {
                    let (status, body) = self
                        .manager
                        .peers
                        .request(
                            host,
                            Resource::Meter(serde_json::to_value(request)?),
                            Duration::from_secs_f64(seconds.clamp(0.05, 5.0) + 4.0),
                        )
                        .await?;
                    Outcome { status, body }
                };
                ensure!(
                    outcome.status == 200,
                    "Metering on {host} failed (HTTP {}): {}",
                    outcome.status,
                    outcome.body["error"].as_str().unwrap_or("no result")
                );
                let values: Readings = serde_json::from_value(outcome.body)
                    .context("Peer returned an invalid meter response")?;
                Ok::<_, anyhow::Error>(values)
            }
            .await;
            match result {
                Ok(mut values) => {
                    for name in names {
                        readings.insert(
                            name.clone(),
                            values.remove(&name).unwrap_or(Reading::Unavailable {
                                error: "The meter returned no reading for this target".to_owned(),
                            }),
                        );
                    }
                }
                Err(error) => {
                    for name in names {
                        readings.insert(
                            name,
                            Reading::Unavailable {
                                error: error.to_string(),
                            },
                        );
                    }
                }
            }
        }
        readings
    }

    async fn dante_meter(
        &self,
        graph: &Graph,
        identities: &[String],
    ) -> (BTreeMap<String, Value>, Vec<String>) {
        let targets = identities
            .iter()
            .filter_map(|identity| match &graph.nodes[identity].details {
                Details::Dante(node) if node.online == Some(true) => node.record.clone(),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut pending = stream::iter(targets.into_iter().map(|key| async move {
            let result = self.metering.snapshot(&key).await;
            (key, result)
        }))
        .buffer_unordered(16);
        let mut levels = BTreeMap::new();
        let mut notes = Vec::new();
        while let Some((key, result)) = pending.next().await {
            match result {
                Ok(Some(sample)) => {
                    levels.insert(key, json!({"rx":sample["rx_normalized"],"tx":sample["tx_normalized"],"wall_time":sample["wall_time"]}));
                }
                Ok(None) => notes.push(format!("No current Dante meter reading for {key}")),
                Err(error) => {
                    notes.push(format!("Dante metering for {key} is unavailable: {error}"))
                }
            }
        }
        (levels, notes)
    }

    pub(super) async fn measure(
        &self,
        context: &mut Context,
        identities: &[String],
        seconds: f64,
    ) -> Result<Levels> {
        ensure!(seconds.is_finite(), "Measurement duration must be finite");
        let targets = context.graph.meter_targets(identities, &context.snapshots);
        let multiple = targets.values().any(|targets| {
            targets.len() > MAXIMUM_TARGETS
                || targets.values().map(Vec::len).sum::<usize>() > MAXIMUM_SOURCES
        });
        if multiple {
            context.notes.push(
                "Meter targets are captured in bounded batches; their measurement windows differ"
                    .to_owned(),
            );
        }
        let hosts = async {
            stream::iter(targets.into_iter().map(|(host, targets)| async move {
                let readings = self.host_meter(&host, targets, seconds).await;
                (host, readings)
            }))
            .buffer_unordered(8)
            .collect::<BTreeMap<_, _>>()
            .await
        };
        let network = self.dante_meter(&context.graph, identities);
        let (jack, (dante, notes)) = tokio::select! {
            _ = self.cancellation.cancelled() => anyhow::bail!("Signal measurement stopped with the daemon"),
            result = async { tokio::join!(hosts, network) } => result,
        };
        context.notes.extend(notes);
        let current = self.manager.snapshot()?;
        let previous = &context.raw[&self.manager.host];
        if current["jack"]["ports"] != previous["jack"]["ports"]
            || current["pulse"] != previous["pulse"]
        {
            context.notes.push("The local audio graph changed during measurement; this trace describes the graph captured before measurement".to_owned());
        }
        Ok(context.graph.levels(identities, &jack, &dante))
    }

    pub async fn points(
        &self,
        request: PointsRequest,
        period: Option<crate::mcp::views::signal_history::HistoryRequest>,
    ) -> Result<Outcome> {
        request.validate()?;
        let mut context = self.context().await?;
        let mut resolved = BTreeMap::new();
        let mut errors = BTreeMap::new();
        for point in request.points {
            let starts = resolve(&context.graph, &point);
            if starts.is_empty() {
                let suggestions = context.graph.suggestions(&point);
                errors.insert(
                    point,
                    if suggestions.is_empty() {
                        "No matching signal point".to_owned()
                    } else {
                        format!("Not found; possible names: {}", suggestions.join(", "))
                    },
                );
            } else {
                resolved.insert(point, starts);
            }
        }
        let status = if resolved.is_empty() { 404 } else { 200 };
        if let Some(period) = period {
            return self
                .point_history(context, resolved, errors, period, status)
                .await;
        }
        let identities = unique(resolved.values().flatten().cloned());
        let levels = self
            .measure(&mut context, &identities, request.seconds)
            .await?;
        let mut body = json!({"measured_at":presentation::now()});
        for (point, identities) in resolved {
            body[point] = json!(
                identities
                    .iter()
                    .map(|identity| format!(
                        "{}: {}",
                        context.graph.nodes[identity].label,
                        levels
                            .get(identity)
                            .map_or("not measured", |level| &level.text)
                    ))
                    .collect::<Vec<_>>()
            );
        }
        for (point, error) in errors {
            body[point] = json!(error);
        }
        self.finish(&context, &mut body);
        Ok(Outcome { status, body })
    }
}
