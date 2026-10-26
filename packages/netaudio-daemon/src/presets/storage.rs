use super::{Error, ParsedPreset, parse_content};
use anyhow::Context;
use chrono::{DateTime, Local};
use netaudio_core::presets::MAXIMUM_BYTES;
use serde_json::{Value, json};
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

const MAXIMUM_LIST_BYTES: usize = 8 * 1024 * 1024;
const LIST_DEADLINE: Duration = Duration::from_secs(30);

pub struct StoredPreset {
    pub content: String,
    pub parsed: ParsedPreset,
}

pub fn reference(name: &str) -> Result<String, Error> {
    let mut result = String::new();
    let mut separator = false;

    for character in name.trim().chars() {
        if character.is_alphanumeric() || matches!(character, '_' | '.' | '-') {
            result.push(character);
            separator = false;
        } else if !separator {
            result.push('-');
            separator = true;
        }
    }

    let result = result.trim_matches(['-', '.']);

    if result.is_empty() {
        return Err(Error::Invalid(
            "the preset name needs at least one letter or digit".into(),
        ));
    }

    Ok(result.into())
}

pub fn path(directory: &Path, name: &str) -> Result<PathBuf, Error> {
    if reference(name)? != name {
        return Err(Error::Invalid(format!(
            "{name:?} is not a saved preset name; list_presets shows them"
        )));
    }

    Ok(directory.join(format!("{name}.xml")))
}

pub(super) fn read(directory: &Path, name: &str) -> Result<StoredPreset, Error> {
    let path = path(directory, name)?;
    let (content, _) = read_content(&path).map_err(|error| {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == ErrorKind::NotFound)
        {
            Error::Invalid(format!(
                "there is no saved preset {name:?}; list_presets shows the saved names"
            ))
        } else {
            Error::Internal(error)
        }
    })?;
    let parsed = parse_content(&content)?;
    Ok(StoredPreset { content, parsed })
}

pub(super) fn list(directory: &Path) -> Result<Value, Error> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => {
            return Err(anyhow::Error::new(error)
                .context("read preset directory")
                .into());
        }
    };
    let mut presets = Vec::new();
    let mut unreadable = Vec::new();
    let mut bytes = 0;
    let deadline = Instant::now() + LIST_DEADLINE;

    for entry in entries.into_iter().flatten() {
        if Instant::now() >= deadline {
            return Err(Error::Conflict(
                "Preset listing exceeded its 30-second work limit; no partial list was returned."
                    .into(),
            ));
        }

        let entry = entry.context("read preset directory entry")?;
        let path = entry.path();

        if path.extension().is_none_or(|extension| extension != "xml") {
            continue;
        }

        let result = (|| -> anyhow::Result<Value> {
            let name = path
                .file_stem()
                .and_then(|name| name.to_str())
                .context("preset filename is not UTF-8")?;
            self::path(directory, name)?;
            let (content, modified) = read_content(&path)?;
            let parsed = parse_content(&content)?;
            let modified: DateTime<Local> = modified.into();
            Ok(json!({
                "preset": name,
                "name": parsed.document.name,
                "devices": parsed.document.devices().keys().collect::<Vec<_>>(),
                "saved": modified.format("%Y-%m-%d %H:%M:%S").to_string(),
                "path": path.to_str().context("preset path is not UTF-8")?,
            }))
        })();

        match result {
            Ok(preset) => {
                bytes += serde_json::to_vec(&preset)
                    .context("encode preset listing")?
                    .len();
                presets.push(preset);
            }
            Err(error) => {
                let problem = format!("{}: {error:#}", entry.file_name().to_string_lossy());
                bytes += serde_json::to_vec(&problem)
                    .context("encode preset listing error")?
                    .len();
                unreadable.push(problem);
            }
        }

        if bytes > MAXIMUM_LIST_BYTES {
            return Err(Error::Conflict(
                "Preset listing exceeds 8 MiB; no partial list was returned.".into(),
            ));
        }
    }

    presets.sort_by(|left, right| left["preset"].as_str().cmp(&right["preset"].as_str()));
    unreadable.sort();
    Ok(json!({
        "directory": directory.to_str().context("preset directory is not UTF-8")?,
        "presets": presets,
        "unreadable": if unreadable.is_empty() { Value::Null } else { json!(unreadable) },
    }))
}

pub(super) fn write(
    directory: &Path,
    name: &str,
    content: &str,
    replace: bool,
) -> Result<PathBuf, Error> {
    let path = path(directory, name)?;
    parse_content(content)?;
    fs::create_dir_all(directory).context("create preset directory")?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".preset-")
        .tempfile_in(directory)
        .context("create temporary preset")?;
    temporary
        .write_all(content.as_bytes())
        .context("write preset")?;
    temporary.as_file().sync_all().context("flush preset")?;
    let result = if replace {
        temporary.persist(&path)
    } else {
        temporary.persist_noclobber(&path)
    };

    match result {
        Ok(_) => {}
        Err(error) if error.error.kind() == ErrorKind::AlreadyExists => {
            return Err(Error::Conflict(format!(
                "a preset named {name:?} is already saved; choose another name or set replace=true"
            )));
        }
        Err(error) => {
            return Err(anyhow::Error::new(error.error)
                .context("install saved preset")
                .into());
        }
    }

    #[cfg(unix)]
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .context("preset was installed but its directory could not be flushed")?;

    Ok(path)
}

fn read_content(path: &Path) -> anyhow::Result<(String, SystemTime)> {
    let metadata = fs::symlink_metadata(path).context("inspect preset file")?;
    anyhow::ensure!(metadata.is_file(), "preset is not a regular file");
    let file = File::open(path).context("open preset file")?;
    let metadata = file.metadata().context("inspect opened preset file")?;
    anyhow::ensure!(metadata.is_file(), "preset is not a regular file");
    anyhow::ensure!(
        metadata.len() <= MAXIMUM_BYTES as u64,
        "Preset files must be no larger than 4 MiB."
    );
    let mut content = String::new();
    file.take(MAXIMUM_BYTES as u64 + 1)
        .read_to_string(&mut content)
        .context("read preset XML")?;
    anyhow::ensure!(
        content.len() <= MAXIMUM_BYTES,
        "Preset files must be no larger than 4 MiB."
    );
    Ok((
        content,
        metadata.modified().context("read preset file time")?,
    ))
}
