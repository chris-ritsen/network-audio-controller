pub(crate) mod file;

use crate::transport::CertificatePaths;
use anyhow::{Context, Result, bail, ensure};
use std::env;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use toml::{Table, Value};

pub const DEFAULT_HTTP_PORT: u16 = 4_780;
pub const DEFAULT_TLS_PORT: u16 = 4_781;
const CONFIGURATION_ENVIRONMENT: &str = "NETAUDIO_CONFIG";
const CONFIGURATION_FILE: &str = "config.toml";
const PREFERENCES_FILE: &str = "preferences.json";

pub struct Configuration {
    pub path: PathBuf,
    pub document: Table,
    pub profile: Table,
    pub profile_name: Option<String>,
    pub daemon: Table,
}

#[derive(Clone, Debug)]
pub struct TlsSettings {
    pub primary: CertificatePaths,
    pub named: Option<CertificatePaths>,
    pub port: u16,
    pub generated: bool,
}

impl Configuration {
    pub fn load(path: Option<&Path>, profile: Option<&str>) -> Result<Self> {
        let explicit = path.is_some();
        let path = match path {
            Some(path) => resolve_path(path)?,
            None => default_config_path()?,
        };
        let document: Table = match fs::read_to_string(&path) {
            Ok(content) => toml::from_str(&content)
                .with_context(|| format!("Failed to parse config {}", path.display()))?,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && !explicit
                    && profile.is_none() =>
            {
                Table::new()
            }
            Err(error) => {
                return Err(error).with_context(|| format!("read config {}", path.display()));
            }
        };
        let profile_name = selected_profile_name(&document, profile)?.map(str::to_owned);
        let profile = selected_profile(&document, profile_name.as_deref())?.clone();
        let mut daemon = table(&document, "daemon")?.cloned().unwrap_or_default();

        if let Some(overrides) = table(&profile, "daemon")? {
            daemon.extend(overrides.clone());
        }

        Ok(Self {
            path,
            document,
            profile,
            profile_name,
            daemon,
        })
    }

    pub fn directory(&self) -> Result<&Path> {
        self.path
            .parent()
            .context("configuration path has no parent directory")
    }

    pub fn preferences_path(&self) -> Result<PathBuf> {
        Ok(self.directory()?.join(PREFERENCES_FILE))
    }

    pub fn mcp_enabled(&self) -> Result<bool> {
        self.daemon.get("mcp").map_or(Ok(false), |value| {
            value.as_bool().context("daemon.mcp must be true or false")
        })
    }

    pub fn preset_directory(&self) -> Result<PathBuf> {
        match self.profile.get("preset_directory") {
            Some(value) => {
                let path = value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .context("preset_directory must be a nonempty path")?;
                resolve_path(Path::new(path))
            }
            None => Ok(self.directory()?.join("presets")),
        }
    }

    pub fn level_history_path(&self) -> Result<PathBuf> {
        if let Some(path) = self.daemon.get("level_history_path") {
            let path = PathBuf::from(
                path.as_str()
                    .context("[daemon] level_history_path must be a path")?,
            );
            return Ok(if path.is_absolute() {
                path
            } else {
                self.directory()?.join(path)
            });
        }

        Ok(dirs::home_dir()
            .context("home directory unavailable")?
            .join(".local/share/netaudio/level_history.sqlite"))
    }

    pub fn event_journal_path(&self) -> Result<PathBuf> {
        if let Some(path) = self.daemon.get("event_journal_path") {
            let path = PathBuf::from(
                path.as_str()
                    .context("[daemon] event_journal_path must be a path")?,
            );
            return Ok(if path.is_absolute() {
                path
            } else {
                self.directory()?.join(path)
            });
        }

        Ok(dirs::home_dir()
            .context("home directory unavailable")?
            .join(".local/share/netaudio/event_journal.sqlite"))
    }

    pub fn shure_memory_path(&self) -> Result<PathBuf> {
        if let Some(value) = self.daemon.get("shure_memory_path") {
            let path = PathBuf::from(
                value
                    .as_str()
                    .context("daemon.shure_memory_path must be a path")?,
            );
            return Ok(if path.is_absolute() {
                path
            } else {
                self.directory()?.join(path)
            });
        }
        Ok(dirs::home_dir()
            .context("Home directory unavailable")?
            .join(".local/share/netaudio/shure_devices.json"))
    }

    pub fn tls(&self) -> Result<Option<TlsSettings>> {
        let mut settings = self.daemon.clone();

        for (key, variable) in [
            ("tls_certificate", "NETAUDIO_TLS_CERTIFICATE"),
            ("tls_key", "NETAUDIO_TLS_KEY"),
            ("tls_named_certificate", "NETAUDIO_TLS_NAMED_CERTIFICATE"),
            ("tls_named_key", "NETAUDIO_TLS_NAMED_KEY"),
            ("tls_port", "NETAUDIO_TLS_PORT"),
            ("no_ssl", "NETAUDIO_NO_SSL"),
        ] {
            let Some(value) = env::var_os(variable).filter(|value| !value.is_empty()) else {
                continue;
            };
            let value = value
                .into_string()
                .map_err(|_| anyhow::anyhow!("{variable} must be UTF-8"))?;
            let value = match key {
                "no_ssl" => Value::Boolean(matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )),
                "tls_port" => Value::Integer(
                    value
                        .parse()
                        .with_context(|| format!("invalid {variable}"))?,
                ),
                _ => Value::String(value),
            };
            settings.insert(key.to_owned(), value);
        }

        let no_ssl = match settings.get("no_ssl") {
            None => false,
            Some(Value::Boolean(value)) => *value,
            Some(_) => bail!("[daemon] no_ssl must be a boolean"),
        };

        if no_ssl {
            ensure!(
                ![
                    "tls_certificate",
                    "tls_key",
                    "tls_named_certificate",
                    "tls_named_key",
                    "tls_port"
                ]
                .iter()
                .any(|name| settings.contains_key(*name)),
                "[daemon] no_ssl cannot be combined with TLS settings"
            );
            return Ok(None);
        }

        let primary =
            certificate_paths(&settings, "tls_certificate", "tls_key", self.directory()?)?;
        let named = certificate_paths(
            &settings,
            "tls_named_certificate",
            "tls_named_key",
            self.directory()?,
        )?;
        let generated = primary.is_none();
        let primary = primary.unwrap_or_else(|| {
            let identity = self
                .path
                .parent()
                .expect("validated configuration directory")
                .join("tls")
                .join("daemon.pem");
            CertificatePaths {
                certificate: identity.clone(),
                key: identity,
            }
        });
        let port = match settings.get("tls_port") {
            None => DEFAULT_TLS_PORT,
            Some(Value::Integer(port)) => u16::try_from(*port)
                .ok()
                .filter(|port| *port != 0)
                .context("[daemon] tls_port must be an integer from 1 through 65535")?,
            Some(_) => bail!("[daemon] tls_port must be an integer from 1 through 65535"),
        };

        Ok(Some(TlsSettings {
            primary,
            named,
            port,
            generated,
        }))
    }
}

pub fn http_address(port: u16, encrypted: bool) -> SocketAddr {
    let address = if encrypted {
        Ipv4Addr::LOCALHOST
    } else {
        Ipv4Addr::UNSPECIFIED
    };
    SocketAddr::new(IpAddr::V4(address), port)
}

pub fn default_config_path() -> Result<PathBuf> {
    let paths = config_search_paths()?;
    let selected = paths
        .iter()
        .find(|path| path.exists())
        .or_else(|| paths.first())
        .context("no configuration path is available")?;
    Ok(selected.clone())
}

pub fn config_search_paths() -> Result<Vec<PathBuf>> {
    if let Some(path) = env::var_os(CONFIGURATION_ENVIRONMENT).filter(|value| !value.is_empty()) {
        return Ok(vec![resolve_path(Path::new(&path))?]);
    }

    let home = dirs::home_dir().context("home directory is unavailable")?;
    let mut paths = vec![home.join(".netaudio").join(CONFIGURATION_FILE)];

    if let Some(directory) = env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty()) {
        paths.push(
            expand_path(Path::new(&directory))?
                .join("netaudio")
                .join(CONFIGURATION_FILE),
        );
    }

    paths.push(
        home.join(".config")
            .join("netaudio")
            .join(CONFIGURATION_FILE),
    );

    #[cfg(target_os = "macos")]
    paths.push(
        home.join("Library")
            .join("Application Support")
            .join("netaudio")
            .join(CONFIGURATION_FILE),
    );

    #[cfg(windows)]
    {
        let roaming = env::var_os("APPDATA")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData").join("Roaming"));
        paths.push(roaming.join("netaudio").join(CONFIGURATION_FILE));
    }

    let mut result = Vec::new();

    for path in paths {
        let path = resolve_path(&path)?;

        if !result.contains(&path) {
            result.push(path);
        }
    }

    Ok(result)
}

fn selected_profile<'a>(document: &'a Table, requested: Option<&str>) -> Result<&'a Table> {
    let Some(name) = selected_profile_name(document, requested)? else {
        return Ok(document);
    };
    document["profiles"][name]
        .as_table()
        .with_context(|| format!("Capture profile {name:?} must be a TOML table"))
}

fn selected_profile_name<'a>(
    document: &'a Table,
    requested: Option<&'a str>,
) -> Result<Option<&'a str>> {
    let Some(profiles) = table(document, "profiles")? else {
        return Ok(None);
    };
    let active = requested.or_else(|| document.get("active_profile").and_then(Value::as_str));

    if let Some(active) = active.filter(|name| !name.is_empty()) {
        profiles
            .get(active)
            .with_context(|| format!("Capture profile {active:?} not found"))?
            .as_table()
            .with_context(|| format!("Capture profile {active:?} must be a TOML table"))?;
        return Ok(Some(active));
    }

    if profiles.get("default").and_then(Value::as_table).is_some() {
        return Ok(Some("default"));
    }

    match profiles.iter().next() {
        Some((name, value)) => value
            .as_table()
            .with_context(|| format!("Capture profile {name:?} must be a TOML table"))
            .map(|_| Some(name.as_str())),
        None => Ok(None),
    }
}

fn certificate_paths(
    settings: &Table,
    certificate: &str,
    key: &str,
    base: &Path,
) -> Result<Option<CertificatePaths>> {
    match (settings.get(certificate), settings.get(key)) {
        (None, None) => Ok(None),
        (Some(certificate_value), Some(key_value)) => Ok(Some(CertificatePaths {
            certificate: configured_path(certificate_value, certificate, base)?,
            key: configured_path(key_value, key, base)?,
        })),
        _ => bail!("[daemon] {certificate} and {key} must be configured together"),
    }
}

fn configured_path(value: &Value, name: &str, base: &Path) -> Result<PathBuf> {
    let value = value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("[daemon] {name} must be a file path"))?;
    let path = Path::new(value);

    if path.is_absolute() || path.starts_with("~") {
        expand_path(path)
    } else {
        expand_path(&base.join(path))
    }
}

fn expand_path(path: &Path) -> Result<PathBuf> {
    let path = if let Ok(relative) = path.strip_prefix("~") {
        dirs::home_dir()
            .context("home directory is unavailable")?
            .join(relative)
    } else {
        path.to_owned()
    };
    std::path::absolute(path).context("resolve absolute configuration path")
}

pub(crate) fn resolve_path(path: &Path) -> Result<PathBuf> {
    let absolute = expand_path(path)?;

    match absolute.canonicalize() {
        Ok(canonical) => Ok(canonical),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(absolute),
        Err(error) => Err(error).context("resolve configuration path"),
    }
}

fn table<'a>(document: &'a Table, name: &str) -> Result<Option<&'a Table>> {
    document
        .get(name)
        .map(|value| {
            value
                .as_table()
                .with_context(|| format!("{name} must be a TOML table"))
        })
        .transpose()
}
