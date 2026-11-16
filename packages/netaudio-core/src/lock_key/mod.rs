mod cabinet;
mod installer;
mod known;
mod scan;

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};

use serde::Serialize;

pub use installer::scratch_directory;
pub use known::Release;
use scan::{Found, Scanner};

const CHUNK_LENGTH: usize = 1 << 20;
const LIBRARY_NAMES: [&str; 2] = ["libDanteController.dylib", "DanteController.dll"];
const LIBRARY_PATHS: [&str; 3] = [
    "Contents/MacOS/libDanteController.dylib",
    "libDanteController.dylib",
    "DanteController.dll",
];
const MAXIMUM_DEPTH: usize = 8;
const MAXIMUM_FILE_LENGTH: u64 = 1 << 30;
const MAXIMUM_FILES: usize = 50_000;
const WINDOWS_INSTALLATION: &str = "Audinate/Dante Controller";

#[derive(Debug, Default, Serialize)]
pub struct Search {
    pub failures: Vec<String>,
    pub key: Option<String>,
    pub release: Option<Release>,
    pub source: Option<PathBuf>,
}

#[derive(Clone, Copy)]
enum Reading {
    Contents,
    Packed,
}

fn read(file: &Path, reading: Reading) -> io::Result<Found> {
    let mut input = File::open(file)?;

    match reading {
        Reading::Contents => {
            let mut chunk = vec![0_u8; CHUNK_LENGTH];
            let mut scanner = Scanner::default();

            loop {
                match input.read(&mut chunk)? {
                    0 => return Ok(scanner.found()),
                    length => scanner.accept(&chunk[..length]),
                }
            }
        }
        Reading::Packed => installer::scan(&mut input),
    }
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn program_directories(drive: Option<&Path>) -> Vec<PathBuf> {
    if let Some(drive) = drive {
        return vec![
            drive.join("Program Files"),
            drive.join("Program Files (x86)"),
        ];
    }

    ["ProgramFiles", "ProgramW6432", "ProgramFiles(x86)"]
        .into_iter()
        .filter_map(std::env::var_os)
        .map(PathBuf::from)
        .chain([
            PathBuf::from(r"C:\Program Files"),
            PathBuf::from(r"C:\Program Files (x86)"),
        ])
        .collect()
}

pub fn installations() -> Vec<PathBuf> {
    let mut directories: Vec<PathBuf> = if cfg!(target_os = "macos") {
        std::iter::once(PathBuf::from("/Applications"))
            .chain(home().map(|home| home.join("Applications")))
            .map(|applications| applications.join("Dante Controller.app"))
            .collect()
    } else if cfg!(windows) {
        program_directories(None)
            .into_iter()
            .map(|programs| programs.join(WINDOWS_INSTALLATION))
            .collect()
    } else {
        std::env::var_os("WINEPREFIX")
            .map(PathBuf::from)
            .or_else(|| home().map(|home| home.join(".wine")))
            .map(|prefix| program_directories(Some(&prefix.join("drive_c"))))
            .unwrap_or_default()
            .into_iter()
            .map(|programs| programs.join(WINDOWS_INSTALLATION))
            .collect()
    };
    let mut seen = BTreeSet::new();
    directories.retain(|directory| directory.is_dir() && seen.insert(directory.clone()));
    directories
}

fn targets(location: &Path) -> Vec<(PathBuf, Reading)> {
    if location.is_dir() {
        return LIBRARY_PATHS
            .iter()
            .map(|path| location.join(path))
            .filter(|path| path.is_file())
            .map(|path| (path, Reading::Contents))
            .collect();
    }

    if !location.is_file() {
        return Vec::new();
    }

    let named = location
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| LIBRARY_NAMES.contains(&name));

    if named {
        return vec![(location.to_owned(), Reading::Contents)];
    }

    let beside: Vec<(PathBuf, Reading)> = location
        .parent()
        .into_iter()
        .flat_map(|parent| LIBRARY_NAMES.iter().map(move |name| parent.join(name)))
        .filter(|path| path.is_file())
        .map(|path| (path, Reading::Contents))
        .collect();

    if beside.is_empty() {
        vec![(location.to_owned(), Reading::Packed)]
    } else {
        beside
    }
}

fn files(directory: &Path, depth: usize, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        if found.len() >= MAXIMUM_FILES {
            return;
        }

        let Ok(kind) = entry.file_type() else {
            continue;
        };

        if kind.is_file() {
            if entry
                .metadata()
                .is_ok_and(|metadata| metadata.len() <= MAXIMUM_FILE_LENGTH)
            {
                found.push(entry.path());
            }
        } else if kind.is_dir() && depth < MAXIMUM_DEPTH {
            files(&entry.path(), depth + 1, found);
        }
    }
}

fn describe(count: usize, place: &str) -> String {
    match count {
        0 => format!("no lock key found in {place}"),
        count => format!("{count} possible lock keys found in {place}"),
    }
}

fn settle(found: Found, place: &str, search: &mut Search) -> bool {
    let keys = found.keys();

    if keys.len() == 1 {
        search.key = keys.iter().next().cloned();
        return true;
    }

    search.failures.push(describe(keys.len(), place));
    search.failures.extend(found.problems);
    false
}

fn identify(input: &mut dyn Read, search: &mut Search) {
    match known::identify(input) {
        Ok(release) => search.release = release,
        Err(error) => search
            .failures
            .push(format!("could not identify the version: {error}")),
    }
}

fn search_files(files: &[(PathBuf, Reading)], place: &Path, search: &mut Search) -> bool {
    let mut all = Found::default();
    let mut sources = Vec::new();

    for (file, reading) in files {
        match read(file, *reading) {
            Ok(found) => {
                if !found.is_empty() {
                    let keys: BTreeSet<String> = found
                        .terminated
                        .union(&found.unterminated)
                        .cloned()
                        .collect();
                    sources.push((file, keys));
                }

                all.extend(found);
            }
            Err(error) => search
                .failures
                .push(format!("could not read {}: {error}", file.display())),
        }
    }

    if !settle(all, &place.display().to_string(), search) {
        return false;
    }

    search.source = sources
        .into_iter()
        .find(|(_, keys)| search.key.as_ref().is_some_and(|key| keys.contains(key)))
        .map(|(file, _)| file.clone());

    if let Some(mut input) = search
        .source
        .as_ref()
        .and_then(|source| File::open(source).ok())
    {
        identify(&mut input, search);
    }

    true
}

pub fn search(location: Option<&Path>) -> Search {
    let mut search = Search::default();
    let locations = match location {
        Some(location) => vec![location.to_owned()],
        None => installations(),
    };

    if locations.is_empty() {
        search
            .failures
            .push("Dante Controller is not installed".to_owned());
        return search;
    }

    for location in &locations {
        for target in targets(location) {
            if search_files(std::slice::from_ref(&target), &target.0, &mut search) {
                search.failures.clear();
                return search;
            }
        }
    }

    for location in locations.iter().filter(|location| location.is_dir()) {
        let mut found = Vec::new();
        files(location, 0, &mut found);
        let found: Vec<(PathBuf, Reading)> = found
            .into_iter()
            .map(|file| (file, Reading::Contents))
            .collect();

        if search_files(&found, location, &mut search) {
            search.failures.clear();
            return search;
        }
    }

    if search.failures.is_empty() {
        search
            .failures
            .push("Dante Controller is not installed".to_owned());
    }

    search
}

pub fn search_installer<R: Read + Seek>(input: &mut R, name: &str) -> Search {
    let mut search = Search::default();

    match installer::scan(input) {
        Ok(found) => {
            if settle(found, name, &mut search) {
                match input.rewind() {
                    Ok(()) => identify(input, &mut search),
                    Err(error) => search
                        .failures
                        .push(format!("could not identify the version: {error}")),
                }
            }
        }
        Err(error) => search
            .failures
            .push(format!("could not read {name}: {error}")),
    }

    search
}
