use super::{
    card_control, cards,
    history::{History, provider_name},
    links, measurement,
    peers::Peers,
    provider::{self, Handle, Outcome},
    reconnect, recording,
};
use crate::{
    configuration::Configuration, monitoring::journal::service::Journal, state::ReadModel,
};
use anyhow::{Context, Result, bail};
use netaudio_core::host_audio::{
    control::{Command, Plan},
    protocol::Provider,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub struct Manager {
    pub host: String,
    pub peers: Arc<Peers>,
    pub cards: cards::Inventory,
    pub links: links::Configuration,
    pub record_jack_levels: bool,
    pub level_history: Arc<crate::monitoring::level_history::LevelHistory>,
    pub history_store: Arc<crate::monitoring::level_history::store::HistoryStore>,
    recorder: recording::Handle,
    jack: Handle,
    pulse: Handle,
    card_commands: card_control::Handle,
    measurements: measurement::Handle,
    snapshot: Mutex<Value>,
    model: Arc<ReadModel>,
}

pub struct Worker {
    manager: Arc<Manager>,
    journal: Arc<Journal>,
    jack: provider::Worker,
    pulse: provider::Worker,
    card_commands: card_control::Worker,
    measurements: measurement::Worker,
    recording: recording::Worker,
}

fn boolean(configuration: &Configuration, name: &str) -> Result<bool> {
    configuration.daemon.get(name).map_or(Ok(false), |value| {
        value
            .as_bool()
            .with_context(|| format!("daemon.{name} must be true or false"))
    })
}

impl Manager {
    pub fn open(
        configuration: &Configuration,
        host: String,
        journal: Arc<Journal>,
        model: Arc<ReadModel>,
        history: (
            Arc<crate::monitoring::level_history::store::HistoryStore>,
            Arc<crate::monitoring::level_history::LevelHistory>,
        ),
    ) -> Result<Option<(Arc<Self>, Worker)>> {
        let record_jack_levels = boolean(configuration, "record_jack_levels")?;
        if !boolean(configuration, "host_audio")? {
            return Ok(None);
        }
        let executable = std::env::current_exe().context("Locate native host audio workers")?;
        let directory = executable
            .parent()
            .context("Native daemon executable has no parent directory")?;
        Ok(Some(Self::new(
            configuration.path.clone(),
            host,
            journal,
            model,
            directory.to_owned(),
            record_jack_levels,
            history,
        )?))
    }

    pub fn new(
        configuration: PathBuf,
        host: String,
        journal: Arc<Journal>,
        model: Arc<ReadModel>,
        directory: PathBuf,
        record_jack_levels: bool,
        history: (
            Arc<crate::monitoring::level_history::store::HistoryStore>,
            Arc<crate::monitoring::level_history::LevelHistory>,
        ),
    ) -> Result<(Arc<Self>, Worker)> {
        let executable = |provider| {
            directory.join(format!(
                "netaudio-host-{provider}{}",
                std::env::consts::EXE_SUFFIX
            ))
        };
        let (jack, jack_worker) = provider::Worker::new(
            Provider::Jack,
            executable("jack"),
            host.clone(),
            journal.clone(),
        );
        let (pulse, pulse_worker) = provider::Worker::new(
            Provider::Pulse,
            executable("pulse"),
            host.clone(),
            journal.clone(),
        );
        let snapshot = json!({"host":host,"jack":super::history::projection(&jack.observation(),&VecDeque::new())?,"pulse":super::history::projection(&pulse.observation(),&VecDeque::new())?,"cards":[],"card_links":{},"bridges":{},"cards_error":"Sound card discovery is initializing","card_links_error":"Sound card mappings are initializing"});
        let (card_commands, card_worker) = card_control::channel();
        let (measurements, measurement_worker) =
            measurement::Worker::new(executable("jack"), host.clone());
        let (recorder, recording_worker) = recording::Worker::new(
            record_jack_levels,
            executable("jack"),
            host.clone(),
            jack.clone(),
            history.0.clone(),
            history.1.clone(),
        );
        let manager = Arc::new(Self {
            peers: Arc::new(Peers::new(host.clone())),
            host,
            cards: cards::Inventory::new(),
            links: links::Configuration::new(configuration),
            record_jack_levels,
            history_store: history.0,
            level_history: history.1,
            recorder,
            jack,
            pulse,
            card_commands,
            measurements,
            snapshot: Mutex::new(snapshot),
            model,
        });
        Ok((
            manager.clone(),
            Worker {
                manager,
                journal,
                jack: jack_worker,
                pulse: pulse_worker,
                card_commands: card_worker,
                measurements: measurement_worker,
                recording: recording_worker,
            },
        ))
    }

    pub fn snapshot(&self) -> Result<Value> {
        let mut snapshot = self
            .snapshot
            .lock()
            .map_err(|_| anyhow::anyhow!("Host audio snapshot lock poisoned"))?
            .clone();
        snapshot["level_recorder"] = self.recorder.snapshot();
        Ok(snapshot)
    }

    pub fn provider(&self, command: &Command) -> &Handle {
        match command {
            Command::JackConnection { .. } => &self.jack,
            _ => &self.pulse,
        }
    }

    pub async fn prepare(&self, command: &Command) -> Result<Plan> {
        let provider = self.provider(command);
        let refreshed = provider.refresh().await?;
        if refreshed.status != 200 {
            bail!(
                "{}",
                refreshed.body["error"]
                    .as_str()
                    .unwrap_or("Host audio provider is unavailable")
            );
        }
        let observation = provider.observation();
        let snapshot = observation
            .snapshot
            .as_ref()
            .context("Host audio provider has no current snapshot")?;
        netaudio_core::host_audio::control::prepare(snapshot, command, &self.host)
            .map_err(anyhow::Error::msg)
    }

    pub async fn execute(&self, command: Command, precondition: Option<Value>) -> Result<Outcome> {
        self.provider(&command).execute(command, precondition).await
    }

    pub async fn measure(
        &self,
        request: netaudio_core::host_audio::measurement::Request,
    ) -> Result<Outcome> {
        self.measurements.measure(request).await
    }

    pub async fn prepare_card(
        &self,
        command: &netaudio_core::host_audio::card_links::Command,
    ) -> Result<netaudio_core::host_audio::card_links::Plan> {
        let (cards, links) = tokio::join!(self.cards.read_now(), self.links.reload());
        let cards = cards?;
        let links = links?;
        if let Some(error) = cards.error.as_ref().or(links.error.as_ref()) {
            bail!("{error}");
        }
        let observation = self.jack.observation();
        let jack = match &observation.snapshot {
            Some(netaudio_core::host_audio::snapshot::Snapshot::Jack(jack))
                if observation.status == provider::Status::Ready =>
            {
                Some(jack)
            }
            _ => None,
        };
        let records = self.model.records()?;
        let devices = records.values().map(AsRef::as_ref).collect::<Vec<_>>();
        netaudio_core::host_audio::card_links::prepare(
            command,
            &netaudio_core::host_audio::card_links::Context {
                host: &self.host,
                cards: &cards.cards,
                bridges: &cards.bridges,
                jack,
                mappings: &links.mappings,
                configuration_revision: links
                    .revision
                    .as_deref()
                    .context("Sound card configuration has no revision")?,
                devices: &devices,
            },
        )
        .map_err(anyhow::Error::msg)
    }

    pub async fn execute_card(
        &self,
        command: netaudio_core::host_audio::card_links::Command,
        precondition: Option<Value>,
    ) -> Result<Outcome> {
        self.card_commands.execute(command, precondition).await
    }

    pub async fn read(&self, host: Option<&str>) -> Result<Outcome> {
        if let Some(host) = host.filter(|host| {
            super::peers::normalized_host(host) != super::peers::normalized_host(&self.host)
        }) {
            let (status, body) = self
                .peers
                .request(
                    host,
                    super::peers::Resource::Snapshot,
                    Duration::from_secs(4),
                )
                .await?;
            return Ok(Outcome { status, body });
        }
        let (jack, pulse, cards, links) = tokio::join!(
            self.jack.refresh(),
            self.pulse.refresh(),
            self.cards.read_now(),
            self.links.reload()
        );
        jack?;
        pulse?;
        self.recorder.retry();
        let cards = cards?;
        let links = links?;
        let mut snapshot = self.snapshot()?;
        for handle in [&self.jack, &self.pulse] {
            let observation = handle.observation();
            let name = provider_name(observation.provider);
            let history: VecDeque<_> = snapshot[name]["changes"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into();
            snapshot[name] = super::history::projection(&observation, &history)?;
        }
        snapshot["cards"] = json!(cards.cards);
        snapshot["bridges"] = json!(cards.bridges);
        snapshot["cards_error"] = json!(cards.error);
        snapshot["card_links"] = json!(links.mappings);
        snapshot["card_links_error"] = json!(links.error);
        Ok(Outcome {
            status: 200,
            body: snapshot,
        })
    }

    fn update(&self, component: &str, update: impl FnOnce(&mut Value)) -> Result<()> {
        let mut snapshot = self
            .snapshot
            .lock()
            .map_err(|_| anyhow::anyhow!("Host audio snapshot lock poisoned"))?;
        update(&mut snapshot);
        self.model.observe(
            "host_audio",
            snapshot.clone(),
            json!({"event":"host_audio_changed","component":component}),
        )
    }
}

impl Worker {
    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        let lifetime = cancellation.child_token();
        let _cancel_on_drop = lifetime.clone().drop_guard();
        let manager = self.manager;
        let mut jack = manager.jack.subscribe();
        let mut pulse = manager.pulse.subscribe();
        let mut cards = manager.cards.subscribe();
        let mut links = manager.links.subscribe();
        let mut peers = manager.peers.subscribe();
        let mut recorder = manager.recorder.subscribe();
        let observer = reconnect::Observer::new(manager.jack.clone(), manager.pulse.clone());
        let mut tasks = JoinSet::new();
        let provider_lifetime = CancellationToken::new();
        let _cancel_providers_on_drop = provider_lifetime.clone().drop_guard();
        let mut providers = JoinSet::new();
        providers.spawn(self.jack.run(provider_lifetime.clone()));
        providers.spawn(self.pulse.run(provider_lifetime.clone()));
        tasks.spawn(self.measurements.run(lifetime.clone()));
        tasks.spawn(self.recording.run(lifetime.clone()));
        tasks.spawn(self.card_commands.run(
            manager.clone(),
            self.journal.clone(),
            lifetime.clone(),
        ));
        tasks.spawn(manager.cards.clone().run(lifetime.clone()));
        tasks.spawn(manager.links.clone().run(lifetime.clone()));
        let source_manager = manager.clone();
        let source_cancellation = lifetime.clone();
        tasks.spawn(async move {
            let result = match observer {
                Ok(observer) => observer.run(source_cancellation.clone()).await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                tracing::error!(%error,"Host audio server source observer stopped");
                source_manager.update("observer", |snapshot| {
                    snapshot["source_observer_error"] = json!(format!("{error:#}"))
                })?;
                source_cancellation.cancelled().await;
            }
            Ok::<_, anyhow::Error>(())
        });
        let mut jack_history = History::default();
        let mut pulse_history = History::default();
        let result = async { loop {
            tokio::select! {
                biased;
                _ = lifetime.cancelled() => break Ok(()),
                ended = providers.join_next() => break match ended {Some(Ok(Ok(())))=>Err(anyhow::anyhow!("Host audio provider stopped unexpectedly")),Some(Ok(Err(error)))=>Err(error),Some(Err(error))=>Err(error.into()),None=>Err(anyhow::anyhow!("Host audio providers ended"))},
                ended = tasks.join_next() => break match ended {Some(Ok(Ok(())))=>Err(anyhow::anyhow!("Host audio owner stopped unexpectedly")),Some(Ok(Err(error)))=>Err(error),Some(Err(error))=>Err(error.into()),None=>Err(anyhow::anyhow!("Host audio owners ended"))},
                changed = jack.changed() => {
                    changed.context("JACK provider observation stream stopped")?;
                    let observed = jack.borrow_and_update().clone();
                    let previous = manager.snapshot()?["jack"]["ports"].clone();
                    let value = jack_history.observe(&manager.host,observed,&self.journal).await?;
                    if value["ports"] != previous {manager.cards.refresh();}
                    manager.update("jack",|snapshot|snapshot["jack"]=value)?;
                },
                changed = pulse.changed() => {
                    changed.context("PulseAudio provider observation stream stopped")?;
                    let observed = pulse.borrow_and_update().clone();
                    let value = pulse_history.observe(&manager.host,observed,&self.journal).await?;
                    manager.update("pulse",|snapshot|snapshot["pulse"]=value)?;
                },
                changed = cards.changed() => {
                    changed.context("Sound card observation stream stopped")?;
                    let value = cards.borrow_and_update().clone();
                    manager.update("cards",|snapshot| {
                        snapshot["cards"]=json!(value.cards);
                        snapshot["bridges"]=json!(value.bridges);
                        snapshot["cards_error"]=json!(value.error);
                        snapshot["cards_observation"]=json!({"revision":value.revision,"observed_at":value.observed_at});
                    })?;
                },
                changed = links.changed() => {
                    changed.context("Sound card mapping observation stream stopped")?;
                    let value = links.borrow_and_update().clone();
                    manager.update("cards",|snapshot| {
                        snapshot["card_links"]=json!(value.mappings);
                        snapshot["card_links_error"]=json!(value.error);
                    })?;
                },
                changed = recorder.changed() => {
                    changed.context("JACK recorder status stream stopped")?;
                    let value = recorder.borrow_and_update().clone();
                    manager.update("level_recorder", |snapshot| snapshot["level_recorder"] = value)?;
                },
                changed = peers.changed() => {
                    changed.context("Host audio peer observation stream stopped")?;
                    peers.borrow_and_update();
                    manager.update("peers",|snapshot|snapshot["peers"]=json!(manager.peers.hosts()))?;
                }
            }
        } }.await;
        lifetime.cancel();
        let shutdown = async {
            let mut failure = None;
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result
                    .map_err(anyhow::Error::from)
                    .and_then(|result| result)
                {
                    tracing::error!(%error, "Host audio consumer shutdown failed");
                    failure.get_or_insert(error);
                }
            }
            provider_lifetime.cancel();
            while let Some(result) = providers.join_next().await {
                if let Err(error) = result
                    .map_err(anyhow::Error::from)
                    .and_then(|result| result)
                {
                    tracing::error!(%error, "Host audio provider shutdown failed");
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), Err)
        };
        tokio::time::timeout(Duration::from_secs(15), shutdown)
            .await
            .context("Host audio shutdown deadline expired")??;
        result
    }
}
