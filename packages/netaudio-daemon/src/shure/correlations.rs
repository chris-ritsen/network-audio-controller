use crate::resources::{FileWatch, resource_paths};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub mappings: BTreeMap<String, String>,
    pub error: Option<String>,
}

pub struct Configuration {
    path: PathBuf,
    snapshot: Mutex<Snapshot>,
}

impl Configuration {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            snapshot: Mutex::new(Snapshot {
                mappings: BTreeMap::new(),
                error: Some("Wireless correlations are initializing".to_owned()),
            }),
        }
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|_| anyhow::anyhow!("Wireless correlation lock poisoned"))?
            .clone())
    }

    fn publish(&self, model: &crate::state::ReadModel, snapshot: Snapshot) -> Result<()> {
        let mut previous = self
            .snapshot
            .lock()
            .map_err(|_| anyhow::anyhow!("Wireless correlation lock poisoned"))?;
        if *previous == snapshot {
            return Ok(());
        }
        *previous = snapshot.clone();
        model.observe(
            "shure_correlations",
            json!(snapshot),
            json!({"event": "shure_correlations_changed", "correlations": snapshot}),
        )
    }

    pub(super) async fn reload(&self, model: &crate::state::ReadModel) -> Result<()> {
        let path = self.path.clone();
        let loaded = tokio::task::spawn_blocking(move || read(path)).await?;
        let snapshot = match loaded {
            Ok(mappings) => Snapshot {
                mappings,
                error: None,
            },
            Err(error) => {
                tracing::error!(%error, "Wireless correlations are unavailable");
                Snapshot {
                    mappings: BTreeMap::new(),
                    error: Some(error.to_string()),
                }
            }
        };
        self.publish(model, snapshot)
    }

    pub async fn run(
        &self,
        model: Arc<crate::state::ReadModel>,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let result = async {
            let mut observer = FileWatch::new()?;
            observer.update(resource_paths(&self.path))?;
            self.reload(&model).await?;
            if observer.update(resource_paths(&self.path))? {
                self.reload(&model).await?;
            }
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok::<(), anyhow::Error>(()),
                    _ = observer.changed.notified() => {
                        observer.update(resource_paths(&self.path))?;
                        self.reload(&model).await?;
                        if observer.update(resource_paths(&self.path))? {
                            self.reload(&model).await?;
                        }
                    }
                }
            }
        }
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "Wireless correlation observer stopped");
            self.publish(
                &model,
                Snapshot {
                    mappings: BTreeMap::new(),
                    error: Some(error.to_string()),
                },
            )?;
        }
        Ok(())
    }
}

pub fn normalize(value: &str) -> Option<String> {
    let normalized = value.replace([':', '-', '.'], "").to_ascii_lowercase();
    (normalized.len() == 12 && normalized.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some(normalized)
}

fn read(path: PathBuf) -> Result<BTreeMap<String, String>> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error).context("Open wireless correlation configuration"),
    };
    ensure!(
        file.metadata()?.is_file(),
        "Wireless configuration is not a regular file"
    );
    let mut bytes = Vec::new();
    file.take(8 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 8 * 1024 * 1024,
        "Wireless configuration exceeds its read budget"
    );
    let document: toml::Table = toml::from_str(std::str::from_utf8(&bytes)?)?;
    let Some(shure) = document.get("shure") else {
        return Ok(BTreeMap::new());
    };
    let shure = shure.as_table().context("shure must be a table")?;
    let Some(correlations) = shure.get("correlations") else {
        return Ok(BTreeMap::new());
    };
    let correlations = correlations
        .as_table()
        .context("shure.correlations must be a table")?;
    let mut mappings = BTreeMap::new();
    for (wireless, dante) in correlations {
        let wireless =
            normalize(wireless).context("Wireless correlation requires a complete MAC address")?;
        let dante = dante
            .as_str()
            .and_then(normalize)
            .context("Dante correlation requires a complete MAC address")?;
        ensure!(
            mappings.insert(wireless, dante).is_none(),
            "Wireless correlation contains a duplicate MAC address"
        );
    }
    Ok(mappings)
}

pub fn linked_device<'a>(
    mac: &str,
    device: &serde_json::Value,
    records: &'a BTreeMap<String, Arc<serde_json::Value>>,
    correlations: &crate::shure::correlations::Snapshot,
) -> Option<&'a serde_json::Value> {
    if correlations.error.is_some() {
        return None;
    }
    let normalized = crate::shure::correlations::normalize(mac)?;
    let explicit = correlations.mappings.get(&normalized);
    let mut matches = records.values().filter(|record| {
        if let Some(target) = explicit {
            return record["mac_address"]
                .as_str()
                .and_then(crate::shure::correlations::normalize)
                .as_ref()
                == Some(target);
        }
        let Some(protocol) = device["device_type"].as_str() else {
            return false;
        };
        ["model", "product_name", "dante_model"]
            .iter()
            .any(|field| {
                record[*field]
                    .as_str()
                    .is_some_and(|model| model.eq_ignore_ascii_case(protocol))
            })
    });
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}
