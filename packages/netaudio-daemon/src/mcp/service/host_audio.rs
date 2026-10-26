use super::*;

impl Service {
    pub(super) fn instructions(&self) -> String {
        let mut instructions = INSTRUCTIONS.to_owned();
        if !self.observations.elsewhere.is_empty() {
            instructions.push_str(" NetAudio cannot do these; name where they are done: ");
            instructions.push_str(
                &self
                    .observations
                    .elsewhere
                    .iter()
                    .map(|(task, provider)| format!("{task}: {provider}"))
                    .collect::<Vec<_>>()
                    .join("; "),
            );
            instructions.push('.');
        }
        instructions
    }

    pub(super) async fn network_overview(&self) -> Result<Value, CallError> {
        let records = self.model.records()?;
        let managed = self.managed.status()?;
        let journal = self.journal.overview().await?;
        let mut value = self.overview.read(&records, &managed, Some(&journal), || {
            let groups = views::issues_view(&journal.issues, &json!({"limit":10}), &records);
            let mut devices = Vec::new();
            for (key, device) in &records {
                if device["online"] == false {
                    continue;
                }
                let summary = views::device_summary(device, managed["fresh"] == true, &records);
                devices.push(views::compact(json!({
                    "name":summary["name"].as_str().unwrap_or(key), "product":summary["product"].as_str().or(summary["model"].as_str()),
                    "online":summary["online"], "management":summary["management"], "sample_rate_hz":summary["sample_rate_hz"],
                    "latency_ms":summary["latency_ms"], "subscription_count":summary["subscription_count"],
                    "subscription_problem_count":summary["subscription_problems"].as_array().map_or(0, Vec::len),
                })));
            }
            let online = devices
                .iter()
                .filter(|device| device["online"] == true)
                .count();
            let clocks = views::clock_status(&records, &json!({}));
            let leaders = clocks["domains"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(name, value)| (name.clone(), value["leaders"].clone()))
                .collect::<serde_json::Map<_, _>>();
            let aliases = &journal.disappeared;
            let managed_summary = views::compact(
                json!({"enabled":managed["enabled"],"state":managed["state"],"fresh":managed["fresh"],"server_count":managed["server_count"],"domain_count":managed["domain_count"],"domains":views::overview::domains(&records)}),
            );
            let mut value = json!({
                "devices":devices,"online_count":online,
                "clock_leaders":leaders, "issues":groups,"ddm":managed_summary,
                "recent_changes":views::overview::recent_changes(&journal.events),
                "renamed_devices":views::overview::renamed(&records, aliases),
                "renamed_devices_before_sequence":aliases["next_before_sequence"],
            });
            value["issue_groups"] = groups["problems"].clone();
            value["open_issue_count"] = groups["problem_count"].clone();
            let shown: u64 = groups["problems"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|problem| problem["issue_count"].as_u64())
                .sum();
            value["omitted_issue_count"] = json!(
                groups["problem_count"]
                    .as_u64()
                    .unwrap_or_default()
                    .saturating_sub(shown)
            );
            value
                .as_object_mut()
                .expect("constructed overview")
                .remove("issues");
            value
        })?;
        let online = value["online_count"].as_u64().unwrap_or_default();
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        value["as_of"] = json!(now);
        value["local_time"] = json!(now);
        value["streams"] = views::overview::streams(&self.stream_health(&records));
        if let Some(signal) = &self.signal {
            match signal.overview().await {
                Ok(Value::Object(fields)) => value
                    .as_object_mut()
                    .expect("constructed overview")
                    .extend(fields),
                Ok(_) => return Err(CallError::invalid("Host overview has an invalid shape")),
                Err(error) => value["host_audio_error"] = json!(error.to_string()),
            }
        }
        let wireless = self.shure.records()?;
        if self.signal.is_none() {
            let conditions = views::wireless::conditions(&wireless);
            if !conditions.is_empty() {
                value["other_problems"] = json!(conditions);
            }
        }
        if wireless
            .as_object()
            .is_some_and(|devices| !devices.is_empty())
        {
            value["wireless"] =
                views::wireless::view(&wireless, &records, &self.shure.correlations.snapshot()?);
        }
        match self.observations.outlets().await {
            Ok(outlets) if !outlets.is_empty() => value["power_outlets"] = json!(outlets),
            Ok(_) => {}
            Err(error) => value["power_outlets_error"] = json!(error.to_string()),
        }
        let problems = value["open_issue_count"].as_u64().unwrap_or_default();
        let other = value["other_problems"].as_array().map_or(0, Vec::len);
        let stream_problems = value["streams"]["problems"].as_array().map_or(0, Vec::len);
        value["answer"] = json!(format!(
            "{online} Dante devices are observed online; {problems} open device issues, {stream_problems} stream findings, and {other} host or wireless findings. Missing measurements remain unknown."
        ));
        Ok(views::compact(value))
    }

    pub(super) fn stream_health(&self, records: &views::Records) -> Value {
        let options = || DiagnosticOptions {
            receiver_history: false,
            clock_history: false,
            ..DiagnosticOptions::default()
        };
        let mut devices = Vec::new();
        let mut problems = Vec::new();

        for (key, device) in records
            .iter()
            .filter(|(_, device)| device["online"] == true)
        {
            match self.registry.diagnostics_snapshot(key, options()) {
                Ok(snapshot) => {
                    let view = views::diagnostics_view(&snapshot, device, records);

                    if let Some(flows) = view["receive_flows"].as_array() {
                        for flow in flows {
                            if let Some(problem) = flow["problem"].as_str() {
                                problems.push(format!(
                                    "{} flow {}: {problem}",
                                    views::text(&device["name"]),
                                    views::text(&flow["flow"])
                                ));
                            }
                        }
                    }

                    devices.push(views::diagnostics_summary(&view, device));
                }
                Err(error) => devices
                    .push(json!({"device": device["name"], "unavailable": error.to_string()})),
            }
        }

        json!({
            "devices": devices,
            "problems": problems,
            "detail": "get_device_diagnostics with a device shows its flows; missing or stale telemetry does not establish stream health",
        })
    }

    pub(super) async fn annotate_computers(
        &self,
        value: &mut Value,
        device: Option<&str>,
    ) -> Result<(), CallError> {
        if let Some(signal) = &self.signal {
            match signal.computers().await {
                Ok(computers) => {
                    if let Some(device) = device {
                        if let Some(computer) = computers.get(device) {
                            value["computer"] = json!(computer);
                        }
                    } else {
                        for (name, computer) in computers {
                            if value[&name].is_object() {
                                value[&name]["computer"] = json!(computer);
                            }
                        }
                    }
                }
                Err(error) => value["host_audio_error"] = json!(error.to_string()),
            }
        }
        if device.is_none() {
            match self.observations.outlets().await {
                Ok(outlets) => {
                    if let Some(devices) = value.as_object_mut() {
                        for (name, value) in devices {
                            if value.get("last_seen").is_some()
                                && let Some(outlet) =
                                    crate::host_audio::observations::outlet_for(name, &outlets)
                            {
                                value["power"] = json!(format!(
                                    "plugged into power outlet {} ({}), which reports {}",
                                    views::text(&outlet["outlet"]),
                                    views::text(&outlet["name"]),
                                    views::text(&outlet["state"])
                                ));
                            }
                        }
                    }
                }
                Err(error) => value["power_outlets_error"] = json!(error.to_string()),
            }
        }
        Ok(())
    }
}
