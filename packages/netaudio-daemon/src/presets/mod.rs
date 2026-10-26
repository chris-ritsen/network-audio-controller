pub(crate) mod audit;
pub mod capture;
pub(crate) mod diff;
mod error;
pub mod loading;
pub(crate) mod operations;
mod preview;
pub(crate) mod report;
pub mod storage;

pub use error::Error;

use netaudio_core::presets::{Document, MAXIMUM_BYTES, MAXIMUM_DEVICES};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct ParsedPreset {
    pub document: Document,
    pub digest: String,
}

pub struct Service {
    directory: PathBuf,
    work: Arc<Semaphore>,
    operation: Arc<tokio::sync::Mutex<()>>,
}

impl Service {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            work: Arc::new(Semaphore::new(4)),
            operation: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub async fn parse(&self, parameters: &Value) -> Result<ParsedPreset, Error> {
        let content = parameters["xml"]
            .as_str()
            .filter(|content| !content.trim().is_empty())
            .ok_or("Select a preset XML file first.")?;

        if content.len() > MAXIMUM_BYTES {
            return Err("Preset files must be no larger than 4 MiB.".into());
        }

        let content = content.to_owned();
        self.blocking(move || parse_content(&content)).await
    }

    pub async fn preview(
        &self,
        parameters: &Value,
        records: &BTreeMap<String, Arc<Value>>,
    ) -> Result<Value, Error> {
        let identifiers = device_identifiers(&parameters["devices"], true)?;
        let parsed = self.parse(parameters).await?;
        Self::preview_parsed(&parsed, &identifiers, records)
    }

    pub fn preview_parsed(
        parsed: &ParsedPreset,
        identifiers: &[String],
        records: &BTreeMap<String, Arc<Value>>,
    ) -> Result<Value, Error> {
        if identifiers
            .iter()
            .any(|identifier| !records.contains_key(identifier))
        {
            return Err(
                "The selected inventory has changed. Review the device selection again.".into(),
            );
        }

        Ok(preview::presentation(parsed, identifiers, records))
    }

    pub async fn list(&self) -> Result<Value, Error> {
        let directory = self.directory.clone();
        self.blocking(move || storage::list(&directory)).await
    }

    pub async fn exists(&self, reference: &str) -> Result<bool, Error> {
        let path = storage::path(&self.directory, reference)?;
        self.blocking(move || match std::fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::Internal(error.into())),
        })
        .await
    }

    pub async fn read(&self, reference: &str) -> Result<storage::StoredPreset, Error> {
        storage::path(&self.directory, reference)?;
        let directory = self.directory.clone();
        let reference = reference.to_owned();
        self.blocking(move || storage::read(&directory, &reference))
            .await
    }

    pub async fn write(
        &self,
        reference: &str,
        content: String,
        replace: bool,
    ) -> Result<PathBuf, Error> {
        storage::path(&self.directory, reference)?;

        if content.len() > MAXIMUM_BYTES {
            return Err("Preset files must be no larger than 4 MiB.".into());
        }

        let directory = self.directory.clone();
        let reference = reference.to_owned();
        self.blocking(move || storage::write(&directory, &reference, &content, replace))
            .await
    }

    async fn blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce() -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = Arc::clone(&self.work)
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
        .map_err(|error| Error::Internal(anyhow::anyhow!("preset worker stopped: {error}")))?
    }
}

fn parse_content(content: &str) -> Result<ParsedPreset, Error> {
    if content.len() > MAXIMUM_BYTES {
        return Err("Preset files must be no larger than 4 MiB.".into());
    }

    let document = Document::parse(content)?;

    if document.devices().is_empty() {
        return Err("A preset must contain between 1 and 128 named devices.".into());
    }

    Ok(ParsedPreset {
        document,
        digest: hex::encode(Sha256::digest(content.as_bytes())),
    })
}

pub fn device_identifiers(value: &Value, allow_empty: bool) -> Result<Vec<String>, String> {
    let values = value
        .as_array()
        .filter(|values| values.len() <= MAXIMUM_DEVICES && (allow_empty || !values.is_empty()))
        .ok_or("Select between 1 and 128 devices.")?;
    let mut seen = BTreeSet::new();
    let mut identifiers = Vec::new();

    for value in values {
        let identifier = value
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or("Devices must be identified by their inventory IDs.")?;

        if !seen.insert(identifier) {
            return Err("A device may only be selected once.".into());
        }

        identifiers.push(identifier.into());
    }

    Ok(identifiers)
}

pub fn matches_record(
    configuration: &serde_json::Map<String, Value>,
    identifier: &str,
    record: &Value,
) -> bool {
    let mut specified = false;

    if let Some(identity) = configuration
        .get("device_identity")
        .and_then(Value::as_object)
    {
        for (field, current) in [
            ("server_name", Some(identifier)),
            (
                "inventory_id",
                record
                    .get("inventory_id")
                    .map_or(Some(identifier), Value::as_str),
            ),
            ("mac_address", record["mac_address"].as_str()),
        ] {
            if let Some(value) = identity
                .get(field)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                specified = true;

                if current != Some(value) {
                    return false;
                }
            }
        }
    }

    specified || record.get("name") == configuration.get("name")
}
