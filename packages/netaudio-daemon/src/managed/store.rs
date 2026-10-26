use super::Error;
use super::configuration::{Settings, validate_name};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use toml_edit::{DocumentMut, Item, Table, TableLike, value};

#[derive(Clone)]
pub struct Store {
    path: PathBuf,
    profile: Option<String>,
}

#[derive(Clone)]
pub struct Snapshot {
    pub settings: Settings,
    pub revision: [u8; 32],
}

pub enum Change {
    Login {
        name: String,
        url: String,
        credential: String,
    },
    Logout {
        name: String,
    },
    Edit {
        current_name: String,
        name: String,
        url: String,
    },
    Remove {
        name: String,
    },
    Select {
        name: String,
    },
    Context {
        server: String,
        name: String,
        domain_id: String,
        domain_name: Option<String>,
    },
}

impl Store {
    pub fn new(configuration: &crate::configuration::Configuration) -> Self {
        Self {
            path: configuration.path.clone(),
            profile: configuration.profile_name.clone(),
        }
    }

    pub(super) fn resource_paths(&self) -> BTreeSet<PathBuf> {
        super::resources::resource_paths(&self.path)
    }

    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let text = read(&self.path)?;
        self.parse(&text)
    }

    fn parse(&self, text: &str) -> Result<Snapshot, Error> {
        let document: toml::Table = toml::from_str(text)
            .map_err(|_| Error::Invalid("Saved configuration is not valid TOML"))?;
        let profile = match &self.profile {
            Some(name) => document
                .get("profiles")
                .and_then(toml::Value::as_table)
                .and_then(|profiles| profiles.get(name))
                .and_then(toml::Value::as_table)
                .ok_or(Error::Conflict(
                    "The selected configuration profile no longer exists",
                ))?,
            None => &document,
        };
        Ok(Snapshot {
            settings: Settings::load(profile, self.directory()?)?,
            revision: Sha256::digest(text.as_bytes()).into(),
        })
    }

    pub fn update(&self, expected: [u8; 32], change: Change) -> Result<Snapshot, Error> {
        let directory = self.directory()?;
        let _lock =
            crate::configuration::file::Lock::acquire(&self.path).map_err(configuration_error)?;
        let original = read(&self.path)?;
        let snapshot = self.parse(&original)?;
        if snapshot.revision != expected {
            return Err(Error::Conflict(
                "Saved configuration changed; read it again before retrying",
            ));
        }
        let mut document: DocumentMut = original
            .parse()
            .map_err(|_| Error::Invalid("Saved configuration is not valid TOML"))?;
        let mut owned_credentials = owned_credentials(&document, directory)?;
        let mut credential_file = None;
        let mut installed = false;
        let result = (|| {
            let profile = self.profile_mut(&mut document)?;
            let settings = &snapshot.settings;
            let managed = table_mut(profile, "ddm")?;
            match change {
                Change::Login {
                    name,
                    url,
                    credential,
                } => {
                    validate_name(&name)?;
                    super::client::endpoint(&url)?;
                    netaudio_core::managed_api::validate_credential(&credential)
                        .map_err(Error::Credential)?;
                    if settings
                        .servers
                        .get(&name)
                        .is_some_and(|server| server.url.as_deref() != Some(&url))
                    {
                        return Err(Error::Invalid(
                            "That profile belongs to a different server; choose a new profile name",
                        ));
                    }
                    let path = write_credential(directory, &name, &credential)?;
                    credential_file = Some(path.clone());
                    let servers = table_mut(managed, "servers")?;
                    let server = table_mut(servers, &name)?;
                    server.insert("url", value(url));
                    server.insert("credential_file", value(path_text(&path)?));
                    server.insert("enabled", value(true));
                    server.remove("credential");
                }
                Change::Logout { name } => {
                    require_server(settings, &name)?;
                    let server = table_mut(table_mut(managed, "servers")?, &name)?;
                    server.remove("credential");
                    server.remove("credential_file");
                    server.insert("enabled", value(false));
                }
                Change::Edit {
                    current_name,
                    name,
                    url,
                } => {
                    require_server(settings, &current_name)?;
                    validate_name(&name)?;
                    super::client::endpoint(&url)?;
                    if name != current_name && settings.servers.contains_key(&name) {
                        return Err(Error::Invalid("A profile with that name already exists"));
                    }
                    let changed_address =
                        settings.servers[&current_name].url.as_deref() != Some(&url);
                    let servers = table_mut(managed, "servers")?;
                    let mut item = servers
                        .remove(&current_name)
                        .ok_or(Error::Invalid("Select a saved server profile"))?;
                    let server = item
                        .as_table_like_mut()
                        .ok_or(Error::Invalid("Server profile must be a TOML table"))?;
                    server.insert("url", value(url));
                    if changed_address {
                        server.remove("credential_file");
                        server.remove("credential");
                        server.remove("trust_certificate_file");
                        server.insert("enabled", value(false));
                    }
                    servers.insert(&name, item);
                    update_contexts(
                        managed,
                        settings,
                        &current_name,
                        (!changed_address).then_some(name.as_str()),
                    )?;
                }
                Change::Remove { name } => {
                    require_server(settings, &name)?;
                    table_mut(managed, "servers")?.remove(&name);
                    update_contexts(managed, settings, &name, None)?;
                }
                Change::Select { name } => {
                    if !settings.contexts.contains_key(&name) {
                        return Err(Error::Invalid("Select a saved DDM context"));
                    }
                    managed.insert("default_context", value(name));
                }
                Change::Context {
                    server,
                    name,
                    domain_id,
                    domain_name,
                } => {
                    require_server(settings, &server)?;
                    validate_name(&name)?;
                    if !settings.servers[&server].enabled() {
                        return Err(Error::Invalid("The selected DDM server is not configured"));
                    }
                    if settings.contexts.get(&name).is_some_and(|context| {
                        context.server != server || context.domain_id != domain_id
                    }) {
                        return Err(Error::Invalid("A context with that name already exists"));
                    }
                    let contexts = table_mut(managed, "contexts")?;
                    let context = table_mut(contexts, &name)?;
                    context.insert("server", value(server));
                    context.insert("domain_id", value(domain_id));
                    match domain_name {
                        Some(name) => {
                            context.insert("domain_name", value(name));
                        }
                        None => {
                            context.remove("domain_name");
                        }
                    }
                    managed.insert("default_context", value(name));
                }
            }
            if let Some(path) = &credential_file {
                owned_credentials.insert(path.clone());
            }
            let mut owned_paths = toml_edit::Array::new();
            for path in &owned_credentials {
                owned_paths.push(path_text(path)?);
            }
            if owned_paths.is_empty() {
                document.remove("managed_credential_files");
            } else {
                document.insert("managed_credential_files", value(owned_paths));
            }
            let updated = document.to_string();
            let snapshot = self.parse(&updated)?;
            if Sha256::digest(read(&self.path)?.as_bytes()).as_slice() != expected {
                return Err(Error::Conflict(
                    "Saved configuration changed while preparing the update",
                ));
            }
            atomic_write(&self.path, updated.as_bytes(), &mut installed)?;
            retire_credentials(directory, &owned_credentials, &updated)?;
            Ok(snapshot)
        })();
        if result.is_err()
            && !installed
            && let Some(path) = credential_file
            && let Err(error) = fs::remove_file(path)
        {
            tracing::error!(%error, "Unreferenced managed credential file could not be removed");
        }
        result
    }

    fn directory(&self) -> Result<&Path, Error> {
        self.path.parent().ok_or(Error::Persistence(
            "Configuration path has no parent directory",
        ))
    }

    fn profile_mut<'a>(
        &self,
        document: &'a mut DocumentMut,
    ) -> Result<&'a mut dyn TableLike, Error> {
        match &self.profile {
            None => Ok(document.as_table_mut()),
            Some(name) => {
                let profiles = table_mut(document.as_table_mut(), "profiles")?;
                profiles
                    .get_mut(name)
                    .and_then(Item::as_table_like_mut)
                    .ok_or(Error::Conflict(
                        "The selected configuration profile no longer exists",
                    ))
            }
        }
    }
}

impl Snapshot {
    pub fn connections(&self) -> Value {
        json!({
            "default_context": self.settings.default_context,
            "servers": self.settings.servers.iter().map(|(name, server)| json!({
                "name": name, "url": server.url, "configured": server.enabled(),
            })).collect::<Vec<_>>(),
            "contexts": self.settings.contexts.iter().map(|(name, context)| json!({
                "name": name, "server": context.server, "domain_id": context.domain_id,
                "domain_name": context.domain_name,
            })).collect::<Vec<_>>(),
        })
    }
}

fn require_server(settings: &Settings, name: &str) -> Result<(), Error> {
    if settings.servers.contains_key(name) {
        Ok(())
    } else {
        Err(Error::Invalid("Select a saved DDM server"))
    }
}

fn update_contexts(
    managed: &mut dyn TableLike,
    settings: &Settings,
    server: &str,
    replacement: Option<&str>,
) -> Result<(), Error> {
    for (name, context) in &settings.contexts {
        if context.server != server {
            continue;
        }
        let contexts = table_mut(managed, "contexts")?;
        if let Some(replacement) = replacement {
            table_mut(contexts, name)?.insert("server", value(replacement));
        } else {
            contexts.remove(name);
            if settings.default_context.as_deref() == Some(name) {
                managed.remove("default_context");
            }
        }
    }
    Ok(())
}

fn table_mut<'a>(
    parent: &'a mut dyn TableLike,
    name: &str,
) -> Result<&'a mut dyn TableLike, Error> {
    if !parent.contains_key(name) {
        parent.insert(name, Item::Table(Table::new()));
    }
    parent
        .get_mut(name)
        .and_then(Item::as_table_like_mut)
        .ok_or(Error::Invalid(
            "DDM configuration section must be a TOML table",
        ))
}

fn configuration_error(error: crate::configuration::file::Error) -> Error {
    match error {
        crate::configuration::file::Error::Busy => Error::Busy,
        crate::configuration::file::Error::Invalid(message) => Error::Invalid(message),
        crate::configuration::file::Error::Persistence(message) => Error::Persistence(message),
    }
}

fn read(path: &Path) -> Result<String, Error> {
    crate::configuration::file::read(path).map_err(configuration_error)
}

fn create_directory(path: &Path) -> Result<(), Error> {
    crate::configuration::file::create_directory(path).map_err(configuration_error)
}

fn write_credential(directory: &Path, name: &str, credential: &str) -> Result<PathBuf, Error> {
    let directory = directory.join("credentials");
    create_directory(&directory)?;
    if !fs::symlink_metadata(&directory)
        .map_err(|_| Error::Persistence("Managed credential directory could not be inspected"))?
        .file_type()
        .is_dir()
    {
        return Err(Error::Persistence(
            "Managed credential directory must not be a symbolic link",
        ));
    }
    let mut file = tempfile::Builder::new()
        .prefix(&format!("{name}-"))
        .suffix(".credential")
        .tempfile_in(&directory)
        .map_err(|_| Error::Persistence("Credential file could not be created"))?;
    writeln!(file, "{credential}")
        .map_err(|_| Error::Persistence("Credential file could not be written"))?;
    file.as_file()
        .sync_all()
        .map_err(|_| Error::Persistence("Credential file could not be flushed"))?;
    let (_, path) = file
        .keep()
        .map_err(|_| Error::Persistence("Credential file could not be saved"))?;
    if let Err(error) = sync_directory(&directory) {
        if let Err(cleanup) = fs::remove_file(&path) {
            tracing::error!(%cleanup, "Unreferenced managed credential file could not be removed");
        }
        return Err(error);
    }
    Ok(path)
}

fn owned_credentials(document: &DocumentMut, directory: &Path) -> Result<BTreeSet<PathBuf>, Error> {
    let Some(owned) = document.get("managed_credential_files") else {
        return Ok(BTreeSet::new());
    };
    let paths = owned
        .as_array()
        .ok_or(Error::Invalid("managed_credential_files must be an array"))?;
    let mut owned = BTreeSet::new();
    for item in paths {
        let path = PathBuf::from(
            item.as_str()
                .ok_or(Error::Invalid("Owned credential paths must be strings"))?,
        );
        if path.parent() != Some(directory.join("credentials").as_path())
            || path.extension().and_then(|extension| extension.to_str()) != Some("credential")
        {
            return Err(Error::Invalid(
                "Owned credentials must stay in the managed credential directory",
            ));
        }
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                owned.insert(path);
            }
            Ok(_) => {
                return Err(Error::Persistence(
                    "Owned credential must be a regular file",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(Error::Persistence(
                    "Owned credential could not be inspected",
                ));
            }
        }
    }
    Ok(owned)
}

fn retire_credentials(
    directory: &Path,
    owned: &BTreeSet<PathBuf>,
    current: &str,
) -> Result<(), Error> {
    if owned.is_empty() {
        return Ok(());
    }
    let current: toml::Table = toml::from_str(current)
        .map_err(|_| Error::Persistence("Current credential references could not be read"))?;
    let references = credential_paths(&toml::Value::Table(current), directory);
    let mut referenced = BTreeSet::new();
    for path in references {
        if let Some(path) = credential_identity(&path)? {
            referenced.insert(path);
        }
    }
    let credential_directory = directory.join("credentials");
    if !fs::symlink_metadata(&credential_directory)
        .map_err(|_| Error::Persistence("Managed credential directory could not be inspected"))?
        .file_type()
        .is_dir()
    {
        return Err(Error::Persistence(
            "Managed credential directory must not be a symbolic link",
        ));
    }
    let mut removed = false;
    for path in owned {
        let Some(identity) = credential_identity(path)? else {
            continue;
        };
        if referenced.contains(&identity) {
            continue;
        }
        if !fs::symlink_metadata(path)
            .map_err(|_| Error::Persistence("Retired credential could not be inspected"))?
            .file_type()
            .is_file()
        {
            return Err(Error::Persistence(
                "Retired managed credential must be a regular file",
            ));
        }
        fs::remove_file(path)
            .map_err(|_| Error::Persistence("Retired managed credential could not be removed"))?;
        removed = true;
    }
    if removed {
        sync_directory(&credential_directory)?;
    }
    Ok(())
}

fn credential_identity(path: &Path) -> Result<Option<PathBuf>, Error> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(Some(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(Error::Persistence(
            "Credential references could not be resolved; retirement was stopped",
        )),
    }
}

fn credential_paths(value: &toml::Value, directory: &Path) -> BTreeSet<PathBuf> {
    let mut paths = BTreeSet::new();
    match value {
        toml::Value::Table(table) => {
            if let Some(path) = table.get("credential_file").and_then(toml::Value::as_str) {
                let path = Path::new(path);
                paths.insert(if path.is_absolute() {
                    path.into()
                } else {
                    directory.join(path)
                });
            }
            for value in table.values() {
                paths.extend(credential_paths(value, directory));
            }
        }
        toml::Value::Array(values) => {
            for value in values {
                paths.extend(credential_paths(value, directory));
            }
        }
        _ => {}
    }
    paths
}

fn atomic_write(path: &Path, bytes: &[u8], installed: &mut bool) -> Result<(), Error> {
    crate::configuration::file::atomic_write(path, bytes, installed).map_err(configuration_error)
}

fn sync_directory(path: &Path) -> Result<(), Error> {
    crate::configuration::file::sync_directory(path).map_err(configuration_error)
}

fn path_text(path: &Path) -> Result<&str, Error> {
    path.to_str()
        .ok_or(Error::Persistence("Credential file path must be UTF-8"))
}
