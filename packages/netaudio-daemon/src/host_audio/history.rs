use super::provider::{Observation, Status};
use crate::monitoring::journal::{
    model::{EventKind, JournalEvent, Severity},
    service::Journal,
};
use anyhow::{Context, Result};
use netaudio_core::host_audio::{changes, protocol::Provider, snapshot::Snapshot};
use serde_json::{Value, json};
use std::collections::VecDeque;

#[derive(Default)]
pub struct History {
    previous: Option<Observation>,
    recent: VecDeque<Value>,
}

pub fn provider_name(provider: Provider) -> &'static str {
    match provider {
        Provider::Jack => "jack",
        Provider::Pulse => "pulse",
    }
}

impl History {
    pub async fn observe(
        &mut self,
        host: &str,
        observation: Observation,
        journal: &Journal,
    ) -> Result<Value> {
        let provider = provider_name(observation.provider);
        let time = if observation.status == Status::Ready {
            observation
                .observed_at
                .clone()
                .context("Ready host audio observation has no timestamp")?
        } else {
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        };
        let mut changes = Vec::new();
        if observation.status == Status::Ready {
            let previous = self
                .previous
                .as_ref()
                .filter(|previous| {
                    previous.status == Status::Ready
                        && previous.generation == observation.generation
                })
                .and_then(|previous| previous.snapshot.as_ref());
            let current = observation
                .snapshot
                .as_ref()
                .context("Ready host audio provider has no snapshot")?;
            changes.extend(changes::between(previous, current, &time));
            let previous_revision = self
                .previous
                .as_ref()
                .filter(|previous| previous.generation == observation.generation)
                .map_or(0, |previous| previous.revision);
            if previous_revision.saturating_add(1) < observation.revision {
                changes.push(json!({"time":time,"kind":"observation_gap","from_revision":previous_revision,"to_revision":observation.revision}));
            }
        } else if self
            .previous
            .as_ref()
            .is_some_and(|previous| previous.status == Status::Ready)
        {
            changes.push(json!({"time":time,"kind":"server_stopped","reason":observation.error}));
        }
        let identity = format!("host_audio:{host}:{provider}");
        let mut event = JournalEvent::observed(
            &json!({"device_identity":identity,"server_name":identity,"name":host}),
            &time,
            EventKind::HostAudioChanged,
            if observation.status == Status::Unavailable {
                Severity::Warning
            } else {
                Severity::Info
            },
            serde_json::to_value(&self.previous)?,
            serde_json::to_value(&observation)?,
            if observation.status == Status::Ready {
                "host_audio_provider"
            } else {
                "host_audio_supervisor"
            },
        )?;
        event.raw.insert("changes".to_owned(), json!(changes));
        journal.host_audio(event).await?;
        self.recent.extend(changes);
        while self.recent.len() > 200 {
            self.recent.pop_front();
        }
        let value = projection(&observation, &self.recent)?;
        self.previous = Some(observation);
        Ok(value)
    }
}

pub fn projection(observation: &Observation, changes: &VecDeque<Value>) -> Result<Value> {
    let ready = observation.status == Status::Ready;
    let mut value = match &observation.snapshot {
        Some(Snapshot::Jack(snapshot)) => serde_json::to_value(snapshot)?,
        Some(Snapshot::Pulse(snapshot)) => serde_json::to_value(snapshot)?,
        None if observation.provider == Provider::Jack => {
            json!({"server":std::env::var("JACK_DEFAULT_SERVER").unwrap_or_else(|_|"default".to_owned()),"seen_running":false,"connected_at":null,"client_name":null,"sample_rate":null,"buffer_size":null,"realtime":null,"cpu_load":null,"xruns":0,"last_xrun":null,"xruns_since":null,"ports":{}})
        }
        None => {
            json!({"seen_running":false,"connected_at":null,"server":netaudio_core::host_audio::snapshot::PulseServer::default(),"sinks":[],"sources":[],"sink_inputs":[],"source_outputs":[]})
        }
    };
    if !ready {
        match observation.provider {
            Provider::Jack => {
                value["ports"] = json!({});
                for field in ["sample_rate", "buffer_size", "cpu_load", "realtime"] {
                    value[field] = Value::Null;
                }
            }
            Provider::Pulse => {
                value["server"] =
                    json!(netaudio_core::host_audio::snapshot::PulseServer::default());
                for field in ["sinks", "sources", "sink_inputs", "source_outputs"] {
                    value[field] = json!([]);
                }
            }
        }
        value["last_observed"] = observation
            .snapshot
            .as_ref()
            .map(|snapshot| match snapshot {
                Snapshot::Jack(snapshot) => serde_json::to_value(snapshot),
                Snapshot::Pulse(snapshot) => serde_json::to_value(snapshot),
            })
            .transpose()?
            .unwrap_or(Value::Null);
    }
    value["available"] = json!(ready);
    value["reason"] = json!(
        observation
            .error
            .as_deref()
            .or_else(|| (!ready).then_some("Host audio provider is connecting"))
    );
    value["changes"] = json!(changes);
    value["observation"] = json!({"status":observation.status,"generation":observation.generation,"revision":observation.revision,"observed_at":observation.observed_at,"executable_sha256":observation.executable_sha256,"process_id":observation.process_id});
    Ok(value)
}
