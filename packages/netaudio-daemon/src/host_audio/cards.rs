#[cfg(target_os = "linux")]
mod hotplug;

use anyhow::Result;
#[cfg(target_os = "linux")]
use anyhow::{Context, ensure};
#[cfg(target_os = "linux")]
use netaudio_core::host_audio::cards;
use netaudio_core::host_audio::cards::{Bridge, Card};
use serde::Serialize;
use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Snapshot {
    pub cards: Vec<Card>,
    pub bridges: BTreeMap<String, Bridge>,
    pub error: Option<String>,
    pub observed_at: Option<String>,
    pub revision: u64,
}

#[derive(Clone)]
pub struct Inventory {
    observations: watch::Sender<Snapshot>,
    changed: Arc<Notify>,
    acquisition: Arc<tokio::sync::Mutex<()>>,
}

impl Default for Inventory {
    fn default() -> Self {
        Self::new()
    }
}

impl Inventory {
    pub fn new() -> Self {
        Self {
            observations: watch::channel(Snapshot {
                error: Some("Sound card discovery is initializing".to_owned()),
                ..Snapshot::default()
            })
            .0,
            changed: Arc::new(Notify::new()),
            acquisition: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.observations.subscribe()
    }

    pub fn snapshot(&self) -> Snapshot {
        self.observations.borrow().clone()
    }

    pub fn refresh(&self) {
        self.changed.notify_one();
    }

    async fn reload(&self) -> Result<()> {
        let _acquisition = self.acquisition.lock().await;
        let result = tokio::task::spawn_blocking(|| {
            read(&PathBuf::from("/proc"), Path::new("/sys/class/sound"))
        })
        .await?;
        self.observations.send_if_modified(|snapshot| {
            match result {
                Ok((cards, bridges)) => {
                    if snapshot.cards == cards
                        && snapshot.bridges == bridges
                        && snapshot.error.is_none()
                    {
                        return false;
                    }
                    snapshot.cards = cards;
                    snapshot.bridges = bridges;
                    snapshot.error = None;
                    snapshot.observed_at = Some(
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    );
                }
                Err(error) => {
                    let error = format!("{error:#}");
                    if snapshot.error.as_ref() == Some(&error) {
                        return false;
                    }
                    snapshot.error = Some(error);
                }
            }
            snapshot.revision += 1;
            true
        });
        Ok(())
    }

    pub async fn read_now(&self) -> Result<Snapshot> {
        self.reload().await?;
        Ok(self.snapshot())
    }

    #[cfg(target_os = "linux")]
    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        let mut observer = hotplug::Observer::new();
        self.reload().await?;
        loop {
            if let Err(error) = &observer {
                let error = format!("Sound card event source is unavailable: {error:#}");
                self.observations.send_if_modified(|snapshot| {
                    if snapshot.error.as_ref() == Some(&error) {
                        return false;
                    }
                    snapshot.error = Some(error);
                    snapshot.revision += 1;
                    true
                });
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = self.changed.notified() => {
                    if observer.is_err() {
                        observer = hotplug::Observer::new();
                    }
                    self.reload().await?;
                },
                event = async {
                    match &mut observer {
                        Ok(observer) => observer.changed().await,
                        Err(_) => std::future::pending().await,
                    }
                } => {
                    match event {
                        Ok(()) => self.reload().await?,
                        Err(error) => observer = Err(error),
                    }
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        self.observations.send_modify(|snapshot| {
            snapshot.error =
                Some("ALSA sound card discovery is available only on Linux".to_owned());
            snapshot.revision += 1;
        });
        cancellation.cancelled().await;
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
fn read(_processes: &Path, _sound: &Path) -> Result<(Vec<Card>, BTreeMap<String, Bridge>)> {
    anyhow::bail!("ALSA sound card discovery is available only on Linux")
}

#[cfg(target_os = "linux")]
fn read(processes: &Path, sound: &Path) -> Result<(Vec<Card>, BTreeMap<String, Bridge>)> {
    let directory = processes.join("asound");
    let Some(listing) = text(&directory.join("cards"))? else {
        return Ok((Vec::new(), BTreeMap::new()));
    };
    let mut cards = cards::parse_listing(&listing);
    for card in &mut cards {
        let directory = directory.join(format!("card{}", card.index));
        card.usb_id = text(&directory.join("usbid"))?;
        for entry in
            std::fs::read_dir(&directory).with_context(|| format!("Read sound card {}", card.id))?
        {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str().and_then(|name| name.strip_prefix("pcm")) else {
                continue;
            };
            if let Some(number) = name
                .strip_suffix('p')
                .and_then(|number| number.parse().ok())
            {
                card.playback_devices.push(number);
            } else if let Some(number) = name
                .strip_suffix('c')
                .and_then(|number| number.parse().ok())
            {
                card.capture_devices.push(number);
            }
        }
        card.playback_devices.sort_unstable();
        card.capture_devices.sort_unstable();
        if card.usb_id.is_some() {
            usb(card, sound)?;
        }
    }
    let mut bridges = BTreeMap::new();
    for entry in std::fs::read_dir(processes).context("Read audio bridge process identities")? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.bytes().all(|character| character.is_ascii_digit()))
        {
            continue;
        }
        let path = entry.path().join("cmdline");
        let bytes = match bytes(&path) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => continue,
            Err(error)
                if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                    matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                    )
                }) =>
            {
                continue;
            }
            Err(error) => return Err(error).context("Read audio bridge process arguments"),
        };
        let arguments: Vec<_> = bytes
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part).into_owned())
            .collect();
        if let Some((client, bridge)) = cards::bridge(&arguments, &cards)
            && let Some(previous) = bridges.insert(client.clone(), bridge.clone())
        {
            ensure!(
                previous == bridge,
                "Several audio bridges claim JACK client {client}"
            );
        }
    }
    Ok((cards, bridges))
}

#[cfg(target_os = "linux")]
fn usb(card: &mut Card, sound: &Path) -> Result<()> {
    let path = sound.join(format!("card{}", card.index)).join("device");
    let path = match std::fs::canonicalize(path) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("Resolve USB sound card path"),
    };
    for directory in path.ancestors() {
        if text(&directory.join("idVendor"))?.is_some() {
            card.usb_vendor = text(&directory.join("manufacturer"))?;
            card.usb_product = text(&directory.join("product"))?;
            card.usb_serial = text(&directory.join("serial"))?;
            card.usb_path = directory
                .file_name()
                .map(|name| name.to_string_lossy().into_owned());
            break;
        }
        if directory.file_name().is_some_and(|name| name == "devices") {
            break;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        file.metadata()?.is_file(),
        "Audio inventory resource is not a regular file"
    );
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 1024 * 1024,
        "Audio inventory resource exceeds its read budget"
    );
    Ok(Some(bytes))
}

#[cfg(target_os = "linux")]
fn text(path: &Path) -> Result<Option<String>> {
    bytes(path)?
        .map(|bytes| {
            String::from_utf8(bytes)
                .map(|value| value.trim().to_owned())
                .context("Audio inventory text must be UTF-8")
        })
        .transpose()
        .map(|value| value.filter(|value| !value.is_empty()))
}
