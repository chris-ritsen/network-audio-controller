use anyhow::{Context, Result};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::{Map, Value};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

enum Cached {
    Ready(Arc<Map<String, Value>>),
    Invalid(String),
}

struct Inner {
    path: PathBuf,
    cache: RwLock<Cached>,
    mutation: Mutex<()>,
}

pub struct Preferences {
    inner: Arc<Inner>,
    watcher: Mutex<RecommendedWatcher>,
}

impl Preferences {
    pub fn open(path: PathBuf) -> Result<Self> {
        let parent = path.parent().context("preferences path has no parent")?;
        std::fs::create_dir_all(parent).context("create preferences directory")?;
        let inner = Arc::new(Inner {
            path: path.clone(),
            cache: RwLock::new(Cached::Invalid(
                "preferences have not been loaded".to_owned(),
            )),
            mutation: Mutex::new(()),
        });
        let weak = Arc::downgrade(&inner);
        let mut watcher = notify::recommended_watcher(
            move |event: notify::Result<notify::Event>| {
                let Some(inner) = weak.upgrade() else {
                    return;
                };

                match event {
                    Ok(event)
                        if !matches!(event.kind, EventKind::Access(_))
                            && event
                                .paths
                                .iter()
                                .any(|path| Some(path.as_path()) == inner.path.parent())
                            && matches!(
                                event.kind,
                                EventKind::Remove(_)
                                    | EventKind::Modify(notify::event::ModifyKind::Name(_))
                            ) =>
                    {
                        inner.invalidate("preferences directory was removed or moved; restart the daemon after restoring it".to_owned());
                        tracing::error!("preferences directory was removed or moved");
                    }
                    Ok(event)
                        if !matches!(event.kind, EventKind::Access(_))
                            && (event.need_rescan()
                                || event.paths.iter().any(|path| path == &inner.path)) =>
                    {
                        if let Err(error) = inner.reload() {
                            tracing::error!(%error, "preferences reload failed");
                        }
                    }
                    Err(error) => {
                        inner.invalidate(format!("preferences watch failed: {error}"));
                        tracing::error!(%error, "preferences watch failed");
                    }
                    _ => {}
                }
            },
        )?;
        watcher.watch(parent, RecursiveMode::NonRecursive)?;
        inner.reload()?;

        Ok(Self {
            inner,
            watcher: Mutex::new(watcher),
        })
    }

    pub fn snapshot(&self) -> Result<Arc<Map<String, Value>>> {
        let cache = self
            .inner
            .cache
            .read()
            .map_err(|_| anyhow::anyhow!("preferences cache lock poisoned"))?;

        match &*cache {
            Cached::Ready(value) => Ok(Arc::clone(value)),
            Cached::Invalid(error) => Err(anyhow::anyhow!("{error}")),
        }
    }

    pub fn update<T>(
        &self,
        update: impl FnOnce(&mut Map<String, Value>) -> Result<T>,
    ) -> Result<T> {
        let guard = self
            .inner
            .mutation
            .lock()
            .map_err(|_| anyhow::anyhow!("preferences mutation lock poisoned"))?;
        let parent = self
            .inner
            .path
            .parent()
            .context("preferences path has no parent")?;
        let lock_path = parent.join(".preferences.lock");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let lock = options.open(lock_path).context("open preferences lock")?;
        fs4::FileExt::lock(&lock).context("lock preferences")?;
        let mut document = read_document(&self.inner.path)?;
        let previous = document.clone();
        let result = update(&mut document)?;

        if document != previous {
            let mut temporary = tempfile::Builder::new()
                .prefix(".preferences-")
                .tempfile_in(parent)?;
            serde_json::to_writer(&mut temporary, &document)?;
            temporary.write_all(b"\n")?;
            temporary.as_file().sync_all()?;
            temporary
                .persist(&self.inner.path)
                .context("replace preferences")?;

            #[cfg(unix)]
            File::open(parent)?.sync_all()?;
        }

        *self
            .inner
            .cache
            .write()
            .map_err(|_| anyhow::anyhow!("preferences cache lock poisoned"))? =
            Cached::Ready(Arc::new(document));
        drop(lock);
        drop(guard);
        Ok(result)
    }

    pub fn set(&self, key: String, value: Value) -> Result<()> {
        self.update(|document| {
            document.insert(key, value);
            Ok(())
        })
    }

    pub fn reload(&self) -> Result<()> {
        self.inner.reload()
    }
}

impl Inner {
    fn reload(&self) -> Result<()> {
        let guard = self
            .mutation
            .lock()
            .map_err(|_| anyhow::anyhow!("preferences mutation lock poisoned"))?;
        let result = read_document(&self.path);
        let mut cache = self
            .cache
            .write()
            .map_err(|_| anyhow::anyhow!("preferences cache lock poisoned"))?;

        match result {
            Ok(value) => *cache = Cached::Ready(Arc::new(value)),
            Err(error) => {
                *cache = Cached::Invalid(error.to_string());
                return Err(error);
            }
        }

        drop(cache);
        drop(guard);
        Ok(())
    }

    fn invalidate(&self, error: String) {
        if let Ok(mut cache) = self.cache.write() {
            *cache = Cached::Invalid(error);
        }
    }
}

impl Drop for Preferences {
    fn drop(&mut self) {
        if let Ok(mut watcher) = self.watcher.lock()
            && let Some(parent) = self.inner.path.parent()
            && let Err(error) = watcher.unwatch(parent)
        {
            tracing::warn!(%error, "preferences watcher cleanup failed");
        }
    }
}

fn read_document(path: &Path) -> Result<Map<String, Value>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Map::new()),
        Err(error) => return Err(error).context("open preferences"),
    };

    serde_json::from_reader(file).context("preferences must be a JSON object")
}
