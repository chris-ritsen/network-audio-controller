use super::{Error, client};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub use netaudio_core::managed_api::inventory::PERMISSIONS;

#[derive(Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default)]
    pub servers: BTreeMap<String, Server>,
    #[serde(default)]
    pub contexts: BTreeMap<String, Context>,
    pub default_context: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub url: Option<String>,
    pub credential_file: Option<PathBuf>,
    pub trust_certificate_file: Option<PathBuf>,
    #[serde(default, deserialize_with = "optional_boolean")]
    pub enabled: Option<bool>,
    #[serde(default = "default_interval", deserialize_with = "interval")]
    pub refresh_interval: f64,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub server: String,
    pub domain_id: String,
    pub domain_name: Option<String>,
    pub operation_permissions: Option<BTreeSet<String>>,
}

impl Settings {
    pub fn load(document: &toml::Table, directory: &Path) -> Result<Self, Error> {
        let Some(value) = document.get("ddm") else {
            return Ok(Self::default());
        };
        let mut settings: Self = value.clone().try_into().map_err(|_| {
            Error::Invalid("DDM configuration contains an unknown or invalid field")
        })?;

        for (name, server) in &mut settings.servers {
            validate_name(name)?;

            if !(1.0..=3600.0).contains(&server.refresh_interval) {
                return Err(Error::Invalid(
                    "DDM refresh_interval must be between 1 and 3600 seconds",
                ));
            }

            if let Some(url) = &server.url {
                client::endpoint(url)?;
            }

            if let Some(path) = &mut server.credential_file {
                *path = resolve_path(path, directory)?;
            }

            if let Some(path) = &mut server.trust_certificate_file {
                *path = resolve_path(path, directory)?;
            }

            if server.enabled != Some(false) {
                match (&server.url, &server.credential_file) {
                    (None, None) if server.enabled != Some(true) => {}
                    (Some(_), Some(_)) => {}
                    _ => {
                        return Err(Error::Invalid(
                            "An enabled DDM server requires a URL and a credential file",
                        ));
                    }
                }
            }
        }

        let mut targets = BTreeSet::new();

        for (name, context) in &settings.contexts {
            validate_name(name)?;
            validate_name(&context.server)?;

            if !settings.servers.contains_key(&context.server) {
                return Err(Error::Invalid(
                    "DDM context references an unknown server profile",
                ));
            }

            if context.domain_id.trim().is_empty() {
                return Err(Error::Invalid(
                    "DDM context domain_id must be a non-empty string",
                ));
            }

            if !targets.insert((&context.server, &context.domain_id)) {
                return Err(Error::Invalid(
                    "DDM contexts must not duplicate a server and domain target",
                ));
            }

            if context
                .operation_permissions
                .as_ref()
                .is_some_and(|permissions| {
                    permissions
                        .iter()
                        .any(|permission| !PERMISSIONS.contains(&permission.as_str()))
                })
            {
                return Err(Error::Invalid(
                    "DDM context contains an unknown operation permission",
                ));
            }
        }

        if settings
            .default_context
            .as_ref()
            .is_some_and(|name| !settings.contexts.contains_key(name))
        {
            return Err(Error::Invalid(
                "DDM default_context references an unknown context",
            ));
        }

        Ok(settings)
    }

    pub fn selected_server(&self, context: Option<&str>) -> Result<(&str, &Server), Error> {
        let name = context
            .or(self.default_context.as_deref())
            .ok_or(Error::Invalid("No DDM context is selected"))?;
        let context = self
            .contexts
            .get(name)
            .ok_or(Error::Invalid("Unknown DDM context"))?;
        self.servers
            .get(&context.server)
            .map(|server| (context.server.as_str(), server))
            .ok_or(Error::Invalid(
                "DDM context references an unknown server profile",
            ))
    }
}

impl Server {
    pub fn enabled(&self) -> bool {
        self.enabled != Some(false) && self.url.is_some() && self.credential_file.is_some()
    }
}

pub fn resolve_path(path: &Path, directory: &Path) -> Result<PathBuf, Error> {
    if path.as_os_str().is_empty() {
        return Err(Error::Invalid(
            "Managed service file path must not be empty",
        ));
    }

    if let Ok(relative) = path.strip_prefix("~") {
        return dirs::home_dir()
            .map(|home| home.join(relative))
            .ok_or(Error::Invalid("Home directory is unavailable"));
    }

    Ok(if path.is_absolute() {
        path.to_owned()
    } else {
        directory.join(path)
    })
}

pub fn validate_name(value: &str) -> Result<(), Error> {
    if value
        .bytes()
        .next()
        .is_none_or(|byte| !byte.is_ascii_alphanumeric())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
    {
        return Err(Error::Invalid(
            "DDM names must start with a letter or digit and contain only letters, digits, ._- characters",
        ));
    }

    Ok(())
}

fn default_interval() -> f64 {
    10.0
}

fn optional_boolean<'de, Deserializer>(
    deserializer: Deserializer,
) -> Result<Option<bool>, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;

    match value {
        None => Ok(None),
        Some(serde_json::Value::Bool(value)) => Ok(Some(value)),
        Some(serde_json::Value::String(value)) => {
            match value.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => Ok(Some(true)),
                "0" | "false" | "no" | "off" => Ok(Some(false)),
                _ => Err(serde::de::Error::custom("enabled must be a boolean")),
            }
        }
        _ => Err(serde::de::Error::custom("enabled must be a boolean")),
    }
}

fn interval<'de, Deserializer>(deserializer: Deserializer) -> Result<f64, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let value = match value {
        serde_json::Value::Number(value) => value.as_f64(),
        serde_json::Value::String(value) => value.trim().parse().ok(),
        _ => None,
    };
    value.ok_or_else(|| serde::de::Error::custom("refresh_interval must be a number"))
}
