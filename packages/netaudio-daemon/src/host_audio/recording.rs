use super::{
    process::Process,
    provider::{Handle as ProviderHandle, Observation, Status},
};
use crate::monitoring::level_history::{LevelHistory, store::HistoryStore};
use anyhow::{Context, Result, bail, ensure};
use netaudio_core::host_audio::{
    protocol::{Event, Provider, Purpose, RequestAction},
    recording::{Configuration, Record},
    snapshot::{Direction, Snapshot},
};
use rand::RngCore;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Handle {
    state: watch::Receiver<Value>,
    retry: Arc<Notify>,
}

impl Handle {
    pub fn snapshot(&self) -> Value {
        self.state.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Value> {
        self.state.clone()
    }

    pub fn retry(&self) {
        if self.state.borrow()["recording"] != true {
            self.retry.notify_one();
        }
    }
}

pub struct Worker {
    enabled: bool,
    path: PathBuf,
    host: String,
    provider: ProviderHandle,
    store: Arc<HistoryStore>,
    history: Arc<LevelHistory>,
    state: watch::Sender<Value>,
    retry: Arc<Notify>,
}

struct Session {
    identity: String,
    sequence: u64,
    request: u64,
    started: bool,
    finished: bool,
    previous_end: Option<chrono::DateTime<chrono::FixedOffset>>,
}

impl Worker {
    pub fn new(
        enabled: bool,
        path: PathBuf,
        host: String,
        provider: ProviderHandle,
        store: Arc<HistoryStore>,
        history: Arc<LevelHistory>,
    ) -> (Handle, Self) {
        let (state, observed) = watch::channel(
            json!({"enabled":enabled,"recording":false,"ports":0,"started_at":null,"reason":if enabled {"JACK recording is initializing"} else {"JACK level recording is disabled"}}),
        );
        let retry = Arc::new(Notify::new());
        (
            Handle {
                state: observed,
                retry: retry.clone(),
            },
            Self {
                enabled,
                path,
                host,
                provider,
                store,
                history,
                state,
                retry,
            },
        )
    }

    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        if !self.enabled {
            cancellation.cancelled().await;
            return Ok(());
        }
        let mut observations = self.provider.subscribe();
        let mut attempted = None;
        loop {
            let observation = observations.borrow_and_update().clone();
            let configuration = configuration(&observation);
            match configuration {
                Ok(Some(configuration)) if attempted.as_ref() != Some(&configuration) => {
                    attempted = Some(configuration.clone());
                    if let Err(error) = self
                        .session(&configuration, &mut observations, &cancellation)
                        .await
                    {
                        self.unavailable(&format!("{error:#}"));
                        tracing::warn!(%error, "JACK recording stopped");
                    }
                    if cancellation.is_cancelled() {
                        break;
                    }
                    continue;
                }
                Ok(None) => {
                    attempted = None;
                    self.unavailable(if observation.status == Status::Ready {
                        "No connected JACK audio outputs are available to record"
                    } else {
                        "JACK is unavailable"
                    });
                }
                Err(error) => self.unavailable(&format!("{error:#}")),
                _ => {}
            }
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = self.retry.notified() => attempted = None,
                changed = observations.changed() => changed.context("JACK recorder observation source stopped")?,
            }
        }
        self.unavailable("JACK recording owner stopped");
        Ok(())
    }

    fn unavailable(&self, reason: &str) {
        let value = json!({"enabled":self.enabled,"recording":false,"ports":0,"started_at":null,"reason":reason});
        self.state.send_if_modified(|state| {
            if *state == value {
                return false;
            }
            *state = value;
            true
        });
    }

    async fn save(&self, session: &mut Session, record: &Record) -> Result<()> {
        let sequence = session
            .sequence
            .checked_add(1)
            .context("Recording sequence exhausted")?;
        self.store
            .record_host_audio(&session.identity, sequence, record, self.history.clone())
            .await?;
        session.sequence = sequence;
        if matches!(record, Record::Finished { .. }) {
            session.finished = true;
        }
        Ok(())
    }

    async fn session(
        &self,
        configuration: &Configuration,
        observations: &mut watch::Receiver<Observation>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut identity = [0; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut identity)
            .context("Generate JACK recording identity")?;
        let mut session = Session {
            identity: hex::encode(identity),
            sequence: 0,
            request: 0,
            started: false,
            finished: false,
            previous_end: None,
        };
        self.unavailable("JACK recording inputs are connecting");
        self.save(
            &mut session,
            &Record::Requested {
                configuration: configuration.clone(),
                observed_at: chrono::Utc::now().to_rfc3339(),
            },
        )
        .await?;
        let mut process =
            match Process::spawn(&self.path, Provider::Jack, Purpose::Record, &self.host).await {
                Ok(process) => process,
                Err(error) => {
                    self.save(
                        &mut session,
                        &Record::Finished {
                            observed_at: chrono::Utc::now().to_rfc3339(),
                            error: Some(format!("{error:#}")),
                        },
                    )
                    .await?;
                    return Err(error);
                }
            };
        let result = self
            .receive(
                &mut process,
                &mut session,
                configuration,
                observations,
                cancellation,
            )
            .await;
        if !session.finished {
            let sequence = process.next_sequence();
            let _ = process.send(sequence, RequestAction::Shutdown).await;
            let drain = async {
                while !session.finished {
                    let message = process.receive().await?;
                    self.accept(&mut session, configuration, message.event)
                        .await?;
                }
                Ok::<_, anyhow::Error>(())
            };
            let drained = tokio::time::timeout(Duration::from_secs(2), drain).await;
            if let Ok(Err(error)) = drained {
                tracing::warn!(%error,"JACK final recording records were unavailable");
            }
        }
        let stopped = process.stop().await;
        if !session.finished {
            self.save(&mut session, &Record::Finished { observed_at: chrono::Utc::now().to_rfc3339(), error: Some(result.as_ref().err().map_or_else(|| "The JACK recorder ended without a final record; missing coverage is unknown".to_owned(), |error| format!("{error:#}"))) }).await?;
        }
        stopped?;
        self.unavailable("JACK recording configuration changed");
        result
    }

    async fn receive(
        &self,
        process: &mut Process,
        session: &mut Session,
        configuration: &Configuration,
        observations: &mut watch::Receiver<Observation>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = self.retry.notified() => {},
                changed = observations.changed() => {
                    changed.context("JACK recorder observation source stopped")?;
                    let current = observations.borrow_and_update().clone();
                    if crate::host_audio::recording::configuration(&current)?.as_ref() != Some(configuration) { return Ok(()); }
                },
                _ = tokio::time::sleep_until(deadline) => bail!("JACK recording worker stopped producing records before its deadline"),
                message = process.receive() => {
                    let event = message?.event;
                    if let Event::Hello { .. } = event {
                        ensure!(session.request == 0, "JACK recorder repeated its identity");
                        session.request = process.next_sequence();
                        process.send(session.request, RequestAction::Record { configuration: configuration.clone() }).await?;
                    } else {
                        self.accept(session, configuration, event).await?;
                        deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                        ensure!(!session.finished, "JACK recording worker ended unexpectedly");
                    }
                },
            }
        }
    }

    async fn accept(
        &self,
        session: &mut Session,
        configuration: &Configuration,
        event: Event,
    ) -> Result<()> {
        let (sequence, record) = match event {
            Event::Recording {
                request_sequence,
                record,
            } => (request_sequence, record),
            Event::Unavailable { reason } => bail!("{reason}"),
            _ => bail!("JACK recorder returned an unexpected event"),
        };
        ensure!(
            sequence == session.request && !session.finished,
            "JACK recorder returned records for a different or finished request"
        );
        match &record {
            Record::Started {
                configuration: observed,
                observed_at,
            } => {
                ensure!(
                    !session.started && observed == configuration,
                    "JACK recorder started with a different configuration"
                );
                chrono::DateTime::parse_from_rfc3339(observed_at)?;
                session.started = true;
            }
            Record::Frame(frame) => {
                ensure!(
                    session.started
                        && frame.frames > 0
                        && frame.sample_rate == configuration.sample_rate
                        && frame.levels.keys().eq(configuration.sources.keys()),
                    "JACK recording frame has invalid sources or coverage"
                );
                let start = chrono::DateTime::parse_from_rfc3339(&frame.started_at)?;
                let end = chrono::DateTime::parse_from_rfc3339(&frame.ended_at)?;
                ensure!(
                    end > start
                        && frame.second == start.timestamp()
                        && session
                            .previous_end
                            .is_none_or(|previous| previous == start),
                    "JACK recording timestamps have a gap or overlap"
                );
                for level in frame.levels.values() {
                    ensure!(
                        level
                            .peak_amplitude
                            .is_none_or(|value| value.is_finite() && value >= 0.0)
                            && level
                                .squared_amplitude_sum
                                .is_none_or(|value| value.is_finite() && value >= 0.0)
                            && level.peak_dbfs.is_none_or(f64::is_finite)
                            && level.rms_dbfs.is_none_or(f64::is_finite),
                        "JACK recording contains an invalid level"
                    );
                }
                session.previous_end = Some(end);
            }
            Record::Finished { observed_at, .. } => {
                chrono::DateTime::parse_from_rfc3339(observed_at)?;
            }
            Record::Requested { .. } => bail!("JACK worker cannot create a recording request"),
        }
        self.save(session, &record).await?;
        if let Record::Started { observed_at, .. } = record {
            self.state.send_replace(json!({"enabled":true,"recording":true,"ports":configuration.sources.len(),"started_at":observed_at,"reason":null,"session":session.identity}));
        } else if let Record::Finished {
            error: Some(error), ..
        } = record
        {
            bail!("{error}");
        }
        Ok(())
    }
}

fn configuration(observation: &Observation) -> Result<Option<Configuration>> {
    let Some(Snapshot::Jack(snapshot)) = observation
        .snapshot
        .as_ref()
        .filter(|_| observation.status == Status::Ready)
    else {
        return Ok(None);
    };
    let sources = snapshot
        .ports
        .iter()
        .filter(|(name, port)| {
            port.direction == Direction::Output
                && port.port_type == "audio"
                && !name.starts_with("netaudio-meter")
                && port
                    .connections
                    .iter()
                    .any(|name| !name.starts_with("netaudio-meter"))
        })
        .map(|(name, port)| (name.clone(), port.identity))
        .collect::<std::collections::BTreeMap<_, _>>();
    if sources.is_empty() {
        return Ok(None);
    }
    let configuration = Configuration {
        generation: observation.generation,
        sample_rate: snapshot
            .sample_rate
            .context("JACK recording sample rate is unavailable")?,
        xruns: snapshot.xruns,
        sources,
    };
    configuration.validate().map_err(anyhow::Error::msg)?;
    Ok(Some(configuration))
}
