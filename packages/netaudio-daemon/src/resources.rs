use anyhow::{Context, Result};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

pub(crate) fn resource_paths(path: &Path) -> BTreeSet<PathBuf> {
    let mut paths = BTreeSet::new();
    resolve_observed_path(path, &mut paths, 0);
    paths
}

fn resolve_observed_path(path: &Path, paths: &mut BTreeSet<PathBuf>, depth: usize) -> PathBuf {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            component => {
                resolved.push(component);
                paths.insert(resolved.clone());
                if depth < 40
                    && let Ok(target) = resolved.read_link()
                {
                    let target = if target.is_absolute() {
                        target
                    } else {
                        resolved.parent().unwrap_or(Path::new(".")).join(target)
                    };
                    resolved = resolve_observed_path(&target, paths, depth + 1);
                }
            }
        }
    }
    paths.insert(resolved.clone());
    resolved
}

pub(crate) struct FileWatch {
    watcher: RecommendedWatcher,
    paths: Arc<Mutex<BTreeSet<PathBuf>>>,
    directories: BTreeSet<PathBuf>,
    error: Arc<Mutex<Option<String>>>,
    replaced: Arc<Mutex<BTreeSet<PathBuf>>>,
    pub changed: Arc<Notify>,
}

impl FileWatch {
    pub fn new() -> Result<Self> {
        let paths = Arc::new(Mutex::new(BTreeSet::new()));
        let error = Arc::new(Mutex::new(None));
        let changed = Arc::new(Notify::new());
        let replaced = Arc::new(Mutex::new(BTreeSet::new()));
        let observed_replacement = Arc::clone(&replaced);
        let observed_paths = Arc::clone(&paths);
        let observed_error = Arc::clone(&error);
        let observed_change = Arc::clone(&changed);
        let watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| match event {
                Ok(event) if !matches!(event.kind, EventKind::Access(_)) => {
                    if let Ok(paths) = observed_paths.lock()
                        && (event.need_rescan()
                            || event.paths.iter().any(|path| paths.contains(path)))
                    {
                        if let Ok(mut replaced) = observed_replacement.lock() {
                            if event.need_rescan() {
                                replaced.extend(paths.iter().cloned());
                            } else if matches!(
                                event.kind,
                                EventKind::Create(_)
                                    | EventKind::Remove(_)
                                    | EventKind::Modify(notify::event::ModifyKind::Name(_))
                            ) {
                                replaced.extend(
                                    event
                                        .paths
                                        .iter()
                                        .filter(|path| paths.contains(*path))
                                        .cloned(),
                                );
                            }
                        }
                        observed_change.notify_one();
                    }
                }
                Err(error) => {
                    if let Ok(mut stored) = observed_error.lock() {
                        *stored = Some(error.to_string());
                    }
                    observed_change.notify_one();
                }
                _ => {}
            })?;
        Ok(Self {
            watcher,
            paths,
            directories: BTreeSet::new(),
            error,
            replaced,
            changed,
        })
    }

    pub fn update(&mut self, files: BTreeSet<PathBuf>) -> Result<bool> {
        if let Some(error) = self
            .error
            .lock()
            .map_err(|_| anyhow::anyhow!("file observer lock poisoned"))?
            .take()
        {
            anyhow::bail!("file observer failed: {error}");
        }
        let mut observed = files.clone();
        let mut directories = BTreeSet::new();
        for file in files {
            for parent in file
                .ancestors()
                .skip(1)
                .filter(|path| !path.as_os_str().is_empty())
            {
                observed.insert(parent.to_owned());
                if parent.is_dir() {
                    directories.insert(parent.to_owned());
                }
            }
        }
        let replaced = std::mem::take(
            &mut *self
                .replaced
                .lock()
                .map_err(|_| anyhow::anyhow!("directory replacement lock poisoned"))?,
        );
        let retired: Vec<_> = self
            .directories
            .iter()
            .filter(|directory| {
                replaced.iter().any(|path| directory.starts_with(path))
                    || !directories.contains(*directory)
            })
            .cloned()
            .collect();
        let mut changed = !retired.is_empty();
        for directory in retired {
            match self.watcher.unwatch(&directory) {
                Ok(()) => {}
                Err(error) if matches!(error.kind, notify::ErrorKind::WatchNotFound) => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("release resource directory {}", directory.display())
                    });
                }
            }
            self.directories.remove(&directory);
        }
        for directory in directories.difference(&self.directories) {
            self.watcher
                .watch(directory, RecursiveMode::NonRecursive)
                .with_context(|| format!("watch resource directory {}", directory.display()))?;
            changed = true;
        }
        let mut paths = self
            .paths
            .lock()
            .map_err(|_| anyhow::anyhow!("file observer lock poisoned"))?;
        changed |= *paths != observed;
        *paths = observed;
        self.directories = directories;
        Ok(changed)
    }
}
