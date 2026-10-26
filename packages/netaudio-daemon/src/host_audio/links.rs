use crate::configuration::file;
use crate::resources::{FileWatch, resource_paths};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Snapshot {
    pub mappings: BTreeMap<String, String>,
    pub revision: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct UpdateError {
    pub conflict: bool,
    pub installed: bool,
    pub message: String,
}

#[derive(Clone)]
pub struct Configuration {
    path: PathBuf,
    observations: watch::Sender<Snapshot>,
    acquisition: Arc<Mutex<()>>,
}

impl Configuration {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            acquisition: Arc::new(Mutex::new(())),
            observations: watch::channel(Snapshot {
                error: Some("Sound card mappings are initializing".to_owned()),
                ..Snapshot::default()
            })
            .0,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.observations.subscribe()
    }

    pub fn snapshot(&self) -> Snapshot {
        self.observations.borrow().clone()
    }

    fn publish(&self, snapshot: Snapshot) {
        self.observations.send_if_modified(|previous| {
            if *previous == snapshot {
                return false;
            }
            *previous = snapshot;
            true
        });
    }

    pub async fn reload(&self) -> Result<Snapshot> {
        let configuration = self.clone();
        tokio::task::spawn_blocking(move || configuration.read_and_publish()).await?
    }

    fn read_and_publish(&self) -> Result<Snapshot> {
        let _acquisition = self
            .acquisition
            .lock()
            .map_err(|_| anyhow::anyhow!("Sound card configuration lock poisoned"))?;
        let result = (|| parse(&file::read(&self.path)?))();
        let snapshot = result.unwrap_or_else(|error| Snapshot {
            mappings: BTreeMap::new(),
            revision: None,
            error: Some(format!("{error:#}")),
        });
        self.publish(snapshot.clone());
        Ok(snapshot)
    }

    pub fn update(
        &self,
        expected: &str,
        card: &str,
        device: Option<&str>,
    ) -> std::result::Result<Snapshot, UpdateError> {
        let mut installed = false;
        let mut conflict = false;
        let result = (|| {
            let _acquisition = self
                .acquisition
                .lock()
                .map_err(|_| anyhow::anyhow!("Sound card configuration lock poisoned"))?;
            let _lock = file::Lock::acquire(&self.path)?;
            let original = file::read(&self.path)?;
            let snapshot = parse(&original)?;
            if snapshot.revision.as_deref() != Some(expected) {
                conflict = true;
                anyhow::bail!("Sound card configuration changed; read it again before retrying");
            }
            let mut document: toml_edit::DocumentMut = original.parse()?;
            if document.get("host_audio").is_none() {
                document.insert(
                    "host_audio",
                    toml_edit::Item::Table(toml_edit::Table::new()),
                );
            }
            let host = document
                .get_mut("host_audio")
                .and_then(toml_edit::Item::as_table_like_mut)
                .context("host_audio must be a table")?;
            if host.get("cards").is_none() {
                host.insert("cards", toml_edit::Item::Table(toml_edit::Table::new()));
            }
            let mappings = host
                .get_mut("cards")
                .and_then(toml_edit::Item::as_table_like_mut)
                .context("host_audio.cards must be a table")?;
            let retired: Vec<_> = mappings
                .iter()
                .filter(|(_, value)| value.as_str() == Some(card))
                .map(|(name, _)| name.to_owned())
                .collect();
            for name in retired {
                mappings.remove(&name);
            }
            if let Some(device) = device {
                mappings.insert(device, toml_edit::value(card));
            }
            let updated = document.to_string();
            let snapshot = parse(&updated)?;
            if file::read(&self.path)? != original {
                conflict = true;
                anyhow::bail!("Configuration changed while preparing the sound card mapping");
            }
            file::atomic_write(&self.path, updated.as_bytes(), &mut installed)?;
            let observed = parse(&file::read(&self.path)?)?;
            ensure!(
                observed == snapshot,
                "Saved sound card mapping was changed before readback"
            );
            self.publish(observed.clone());
            Ok::<_, anyhow::Error>(observed)
        })();
        match result {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => Err(UpdateError {
                conflict,
                installed,
                message: format!("{error:#}"),
            }),
        }
    }

    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        let result = async {
            let mut observer = FileWatch::new()?;
            observer.update(resource_paths(&self.path))?;
            self.reload().await?;
            if observer.update(resource_paths(&self.path))? {
                self.reload().await?;
            }
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok::<(),anyhow::Error>(()),
                    _ = observer.changed.notified() => {
                        observer.update(resource_paths(&self.path))?;
                        self.reload().await?;
                        if observer.update(resource_paths(&self.path))? { self.reload().await?; }
                    }
                }
            }
        }
        .await;
        if let Err(error) = result {
            self.observations.send_modify(|snapshot| {
                snapshot.error = Some(format!("Sound card mapping observer stopped: {error:#}"));
            });
        }
        Ok(())
    }
}

fn parse(text: &str) -> Result<Snapshot> {
    let document: toml::Table = toml::from_str(text)
        .map_err(|_| anyhow::anyhow!("Saved configuration is not valid TOML"))?;
    let mut mappings = BTreeMap::new();
    if let Some(host) = document.get("host_audio") {
        let host = host.as_table().context("host_audio must be a table")?;
        if let Some(cards) = host.get("cards") {
            for (device, card) in cards
                .as_table()
                .context("host_audio.cards must be a table")?
            {
                let card = card
                    .as_str()
                    .filter(|card| !card.is_empty())
                    .context("Each sound card mapping must name an ALSA card")?;
                ensure!(
                    !device.is_empty(),
                    "Each sound card mapping must name a Dante device"
                );
                mappings.insert(device.clone(), card.to_owned());
            }
        }
    }
    Ok(Snapshot {
        mappings,
        revision: Some(hex::encode(Sha256::digest(text.as_bytes()))),
        error: None,
    })
}
