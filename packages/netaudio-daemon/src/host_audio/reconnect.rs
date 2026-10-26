use super::provider::Handle;
use anyhow::{Context, Result};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub struct Observer {
    watcher: RecommendedWatcher,
    directories: BTreeSet<PathBuf>,
    targets: BTreeSet<PathBuf>,
    changed: Arc<Notify>,
    pending: Arc<Mutex<Pending>>,
    jack: Handle,
    pulse: Handle,
}

#[derive(Default)]
struct Pending {
    jack: bool,
    pulse: bool,
    error: Option<String>,
    replaced: BTreeSet<PathBuf>,
}

impl Observer {
    pub fn new(jack: Handle, pulse: Handle) -> Result<Self> {
        let runtime = std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from);
        let pulse_socket = std::env::var("PULSE_SERVER")
            .ok()
            .and_then(|value| value.strip_prefix("unix:").map(PathBuf::from))
            .or_else(|| runtime.as_ref().map(|path| path.join("pulse/native")));
        let server = std::env::var("JACK_DEFAULT_SERVER").unwrap_or_else(|_| "default".to_owned());
        let jack_prefix = format!("jack_{server}_");
        let shared = PathBuf::from("/dev/shm");
        let mut targets = BTreeSet::new();
        #[cfg(target_os = "linux")]
        targets.insert(shared.clone());
        if let Some(runtime) = &runtime {
            targets.insert(runtime.clone());
        }
        if let Some(parent) = pulse_socket.as_deref().and_then(Path::parent) {
            targets.insert(parent.to_owned());
        }
        let changed = Arc::new(Notify::new());
        let notify = changed.clone();
        let pending = Arc::new(Mutex::new(Pending::default()));
        let received = pending.clone();
        let observed_targets = targets.clone();
        let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let Ok(mut pending) = received.lock() else {
                return;
            };
            match event {
                Ok(event) if !matches!(event.kind, EventKind::Access(_)) => {
                    let rescan = event.need_rescan();
                    pending.jack |= rescan;
                    pending.pulse |= rescan;
                    let replaced = matches!(
                        event.kind,
                        EventKind::Create(_)
                            | EventKind::Remove(_)
                            | EventKind::Modify(notify::event::ModifyKind::Name(_))
                    );
                    let mut structural = false;
                    for path in event.paths {
                        let name = path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or_default();
                        pending.jack |= rescan
                            || (path.parent() == Some(shared.as_path())
                                && name.starts_with(&jack_prefix))
                            || (runtime.as_deref() == path.parent()
                                && name.starts_with("pipewire-"));
                        pending.pulse |= rescan
                            || pulse_socket
                                .as_ref()
                                .is_some_and(|socket| socket == &path || socket.starts_with(&path));
                        if observed_targets
                            .iter()
                            .any(|target| target.starts_with(&path))
                        {
                            structural = true;
                            if replaced {
                                pending.replaced.insert(path);
                            }
                        }
                    }
                    if rescan {
                        pending.replaced.extend(observed_targets.iter().cloned());
                    }
                    if pending.jack || pending.pulse || structural || rescan {
                        notify.notify_one();
                    }
                }
                Err(error) => {
                    pending.error = Some(error.to_string());
                    notify.notify_one();
                }
                _ => {}
            }
        })?;
        let mut observer = Self {
            watcher,
            directories: BTreeSet::new(),
            targets,
            changed,
            pending,
            jack,
            pulse,
        };
        observer.update(&BTreeSet::new())?;
        Ok(observer)
    }

    fn update(&mut self, replaced: &BTreeSet<PathBuf>) -> Result<()> {
        let mut directories = BTreeSet::new();
        for target in &self.targets {
            for path in target.ancestors() {
                if path.is_dir() {
                    directories.insert(path.to_owned());
                }
            }
        }
        let retired: Vec<_> = self
            .directories
            .iter()
            .filter(|path| {
                !directories.contains(*path)
                    || replaced.iter().any(|ancestor| path.starts_with(ancestor))
            })
            .cloned()
            .collect();
        for path in retired {
            match self.watcher.unwatch(&path) {
                Ok(()) => {}
                Err(error) if matches!(error.kind, notify::ErrorKind::WatchNotFound) => {}
                Err(error) => return Err(error.into()),
            }
            self.directories.remove(&path);
        }
        for path in directories.difference(&self.directories) {
            self.watcher
                .watch(path, RecursiveMode::NonRecursive)
                .with_context(|| {
                    format!("Observe host audio server directory {}", path.display())
                })?;
        }
        self.directories = directories;
        Ok(())
    }

    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = self.changed.notified() => {
                    let pending = std::mem::take(&mut *self.pending.lock().map_err(|_|anyhow::anyhow!("Host audio source observer lock poisoned"))?);
                    if let Some(error) = pending.error { anyhow::bail!("Host audio source observer failed: {error}"); }
                    self.update(&pending.replaced)?;
                    if pending.jack { self.jack.server_changed(); }
                    if pending.pulse { self.pulse.server_changed(); }
                }
            }
        }
    }
}
