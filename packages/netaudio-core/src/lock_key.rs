use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::lock::KEY_LENGTH;

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
const MINIMUM_DISTINCT_STEPS: usize = 8;
const RUN_LENGTH: usize = KEY_LENGTH * 2;
const WINDOWS_INSTALLATION: &str = "Audinate/Dante Controller";

#[derive(Debug, Default, Serialize)]
pub struct Search {
    pub failures: Vec<String>,
    pub key: Option<String>,
    pub source: Option<PathBuf>,
}

#[derive(Default)]
struct Scanner {
    preceded_by_terminator: bool,
    previous: Option<u8>,
    run: Vec<u8>,
    run_length: usize,
    terminated: BTreeSet<String>,
    unterminated: BTreeSet<String>,
}

impl Scanner {
    fn accept(&mut self, chunk: &[u8]) {
        for &byte in chunk {
            if byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) {
                if self.run_length == 0 {
                    self.preceded_by_terminator = self.previous == Some(0);
                    self.run.clear();
                }

                if self.run_length < RUN_LENGTH {
                    self.run.push(byte);
                }

                self.run_length += 1;
            } else {
                self.finish(byte == 0);
            }

            self.previous = Some(byte);
        }
    }

    fn finish(&mut self, followed_by_terminator: bool) {
        if self.run_length == RUN_LENGTH && !is_table(&self.run) {
            let key = String::from_utf8_lossy(&self.run[..KEY_LENGTH]).into_owned();

            if self.preceded_by_terminator && followed_by_terminator {
                self.terminated.insert(key);
            } else {
                self.unterminated.insert(key);
            }
        }

        self.run_length = 0;
    }

    fn found(mut self) -> Found {
        self.finish(false);
        Found {
            terminated: self.terminated,
            unterminated: self.unterminated,
        }
    }
}

#[derive(Default)]
pub struct Found {
    pub terminated: BTreeSet<String>,
    pub unterminated: BTreeSet<String>,
}

impl Found {
    fn extend(&mut self, other: Found) {
        self.terminated.extend(other.terminated);
        self.unterminated.extend(other.unterminated);
    }

    fn is_empty(&self) -> bool {
        self.terminated.is_empty() && self.unterminated.is_empty()
    }

    fn keys(self) -> BTreeSet<String> {
        if self.terminated.is_empty() {
            self.unterminated
        } else {
            self.terminated
        }
    }
}

fn is_table(run: &[u8]) -> bool {
    let bytes: Vec<i16> = run
        .chunks_exact(2)
        .map(|pair| i16::from(nibble(pair[0]) << 4 | nibble(pair[1])))
        .collect();
    let steps: BTreeSet<i16> = bytes.windows(2).map(|pair| pair[1] - pair[0]).collect();
    steps.len() < MINIMUM_DISTINCT_STEPS
}

fn nibble(character: u8) -> u8 {
    match character {
        b'0'..=b'9' => character - b'0',
        _ => character - b'a' + 10,
    }
}

pub fn candidates(file: &Path) -> io::Result<Found> {
    let mut input = File::open(file)?;
    let mut chunk = vec![0_u8; CHUNK_LENGTH];
    let mut scanner = Scanner::default();

    loop {
        let length = input.read(&mut chunk)?;

        if length == 0 {
            return Ok(scanner.found());
        }

        scanner.accept(&chunk[..length]);
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

fn libraries(location: &Path) -> Vec<PathBuf> {
    if location.is_dir() {
        return LIBRARY_PATHS
            .iter()
            .map(|path| location.join(path))
            .filter(|path| path.is_file())
            .collect();
    }

    if !location.is_file() {
        return Vec::new();
    }

    let named = location
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| LIBRARY_NAMES.contains(&name));
    let beside: Vec<PathBuf> = location
        .parent()
        .into_iter()
        .flat_map(|parent| LIBRARY_NAMES.iter().map(move |name| parent.join(name)))
        .filter(|path| path.is_file())
        .collect();

    if named || beside.is_empty() {
        vec![location.to_owned()]
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

fn describe(count: usize, place: &Path) -> String {
    match count {
        0 => format!("no lock key found in {}", place.display()),
        count => format!("{count} possible lock keys found in {}", place.display()),
    }
}

fn search_files(files: &[PathBuf], place: &Path, search: &mut Search) -> bool {
    let mut all = Found::default();
    let mut sources = Vec::new();

    for file in files {
        match candidates(file) {
            Ok(found) => {
                if !found.is_empty() {
                    sources.push((file, found.terminated.clone(), found.unterminated.clone()));
                }

                all.extend(found);
            }
            Err(error) => search
                .failures
                .push(format!("could not read {}: {error}", file.display())),
        }
    }

    let keys = all.keys();

    if keys.len() == 1 {
        let key = keys.into_iter().next();
        search.source = sources
            .into_iter()
            .find(|(_, terminated, unterminated)| {
                key.as_ref()
                    .is_some_and(|key| terminated.contains(key) || unterminated.contains(key))
            })
            .map(|(file, _, _)| file.clone());
        search.key = key;
        return true;
    }

    search.failures.push(describe(keys.len(), place));
    false
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
        for library in libraries(location) {
            if search_files(std::slice::from_ref(&library), &library, &mut search) {
                search.failures.clear();
                return search;
            }
        }
    }

    for location in locations.iter().filter(|location| location.is_dir()) {
        let mut found = Vec::new();
        files(location, 0, &mut found);

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
