use super::{MINUTE_SLOTS, RETENTION_SECONDS, SECOND_SLOTS, SECONDS_PER_MINUTE};
use anyhow::{Context, Result, ensure};
use flate2::read::ZlibDecoder;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;

const LAYOUT: &str = "1 900 1440";
const TIMESTAMP_BYTES: usize = size_of::<i64>();
const PEAK_BYTES: usize = size_of::<f32>();
const COUNT_BYTES: usize = size_of::<u16>();
const SECOND_BYTES: usize = TIMESTAMP_BYTES + PEAK_BYTES + size_of::<u8>();
const MINUTE_BYTES: usize = TIMESTAMP_BYTES + PEAK_BYTES + COUNT_BYTES * 2;
const RAW_BYTES: usize = SECOND_SLOTS * SECOND_BYTES + MINUTE_SLOTS * MINUTE_BYTES;

struct RawTrack {
    second_stamps: Vec<i64>,
    second_peaks: Vec<f32>,
    second_signal: Vec<u8>,
    minute_stamps: Vec<i64>,
    minute_peaks: Vec<f32>,
    minute_signal: Vec<u16>,
    minute_recorded: Vec<u16>,
}

struct Source {
    first_second: i64,
    coverage: RawTrack,
    membership: Vec<(i64, BTreeSet<String>)>,
}

impl Source {
    fn contains(&self, point: &str, second: i64) -> bool {
        let index = self
            .membership
            .partition_point(|(start, _)| *start <= second);
        index
            .checked_sub(1)
            .is_some_and(|index| self.membership[index].1.contains(point))
    }
}

pub(super) struct LegacyTrack {
    source: Arc<Source>,
    point: String,
    values: Option<RawTrack>,
}

pub(super) struct Reading {
    pub peak: Option<f32>,
    pub recorded: u64,
    pub signal: u64,
}

impl LegacyTrack {
    pub fn first_second(&self) -> i64 {
        self.source.first_second
    }

    pub fn second(&self, second: i64) -> Option<Reading> {
        let slot = second.rem_euclid(SECOND_SLOTS as i64) as usize;

        if let Some(values) = &self.values
            && values.second_stamps[slot] == second
        {
            return Some(Reading {
                peak: finite(values.second_peaks[slot]),
                recorded: 1,
                signal: u64::from(values.second_signal[slot]),
            });
        }

        (self.source.coverage.second_stamps[slot] == second
            && self.source.contains(&self.point, second))
        .then_some(Reading {
            peak: None,
            recorded: 1,
            signal: 0,
        })
    }

    pub fn minute(&self, minute: i64) -> Option<Reading> {
        let slot = minute.rem_euclid(MINUTE_SLOTS as i64) as usize;
        let mut recorded = if self.source.coverage.minute_stamps[slot] == minute
            && self.source.contains(
                &self.point,
                minute * SECONDS_PER_MINUTE + SECONDS_PER_MINUTE - 1,
            ) {
            u64::from(self.source.coverage.minute_recorded[slot])
        } else {
            0
        };

        if let Some(values) = &self.values
            && values.minute_stamps[slot] == minute
        {
            recorded = recorded.max(u64::from(values.minute_recorded[slot]));
            return Some(Reading {
                peak: finite(values.minute_peaks[slot]),
                recorded,
                signal: u64::from(values.minute_signal[slot]),
            });
        }

        (recorded != 0).then_some(Reading {
            peak: None,
            recorded,
            signal: 0,
        })
    }
}

fn finite(value: f32) -> Option<f32> {
    value.is_finite().then_some(value)
}

pub(super) fn load(
    connection: &Connection,
    now: i64,
) -> Result<BTreeMap<String, BTreeMap<String, LegacyTrack>>> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'layout'",
            [],
            |row| row.get(0),
        )
        .optional()?;

    if exists.is_none() {
        return Ok(BTreeMap::new());
    }

    let layout: String = connection.query_row("SELECT layout FROM layout", [], |row| row.get(0))?;
    ensure!(
        layout == LAYOUT,
        "unsupported saved Python signal history layout {layout}"
    );
    let mut sources = BTreeMap::new();
    let mut statement =
        connection.prepare("SELECT name, first_second, members, track FROM sources")?;
    let mut rows = statement.query([])?;

    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let first_second: i64 = row.get(1)?;
        let membership: String = row.get(2)?;
        let raw: Vec<u8> = row.get(3)?;
        let coverage = decode(&raw, now)?;

        if coverage
            .minute_stamps
            .iter()
            .copied()
            .max()
            .unwrap_or_default()
            < (now - RETENTION_SECONDS).div_euclid(SECONDS_PER_MINUTE)
        {
            continue;
        }

        let membership: Vec<(i64, BTreeSet<String>)> = serde_json::from_str(&membership)?;
        ensure!(
            first_second >= 0
                && membership.iter().all(|(start, _)| *start >= first_second)
                && membership.windows(2).all(|pair| pair[0].0 <= pair[1].0),
            "invalid saved signal history membership"
        );
        sources.insert(
            name,
            Arc::new(Source {
                first_second,
                coverage,
                membership,
            }),
        );
    }

    let mut restored: BTreeMap<String, BTreeMap<String, LegacyTrack>> = BTreeMap::new();

    for (name, source) in &sources {
        let points: BTreeSet<_> = source
            .membership
            .iter()
            .flat_map(|(_, members)| members)
            .collect();

        for point in points {
            restored.entry(name.clone()).or_default().insert(
                point.clone(),
                LegacyTrack {
                    source: Arc::clone(source),
                    point: point.clone(),
                    values: None,
                },
            );
        }
    }

    let mut statement = connection.prepare("SELECT source, point, track FROM tracks")?;
    let mut rows = statement.query([])?;

    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let point: String = row.get(1)?;
        let Some(source) = sources.get(&name) else {
            continue;
        };
        let raw: Vec<u8> = row.get(2)?;
        let values = decode(&raw, now)?;
        restored.entry(name).or_default().insert(
            point.clone(),
            LegacyTrack {
                source: Arc::clone(source),
                point,
                values: Some(values),
            },
        );
    }

    Ok(restored)
}

fn decode(compressed: &[u8], now: i64) -> Result<RawTrack> {
    let mut raw = Vec::with_capacity(RAW_BYTES);
    ZlibDecoder::new(compressed)
        .take(RAW_BYTES as u64 + 1)
        .read_to_end(&mut raw)?;
    ensure!(
        raw.len() == RAW_BYTES,
        "saved signal track has the wrong size"
    );
    let little_endian = valid_stamps(
        &raw[..SECOND_SLOTS * TIMESTAMP_BYTES],
        SECOND_SLOTS,
        true,
        now,
    ) && valid_stamps(
        &raw[SECOND_SLOTS * SECOND_BYTES
            ..SECOND_SLOTS * SECOND_BYTES + MINUTE_SLOTS * TIMESTAMP_BYTES],
        MINUTE_SLOTS,
        true,
        now.div_euclid(SECONDS_PER_MINUTE),
    );
    let big_endian = valid_stamps(
        &raw[..SECOND_SLOTS * TIMESTAMP_BYTES],
        SECOND_SLOTS,
        false,
        now,
    ) && valid_stamps(
        &raw[SECOND_SLOTS * SECOND_BYTES
            ..SECOND_SLOTS * SECOND_BYTES + MINUTE_SLOTS * TIMESTAMP_BYTES],
        MINUTE_SLOTS,
        false,
        now.div_euclid(SECONDS_PER_MINUTE),
    );
    ensure!(
        little_endian != big_endian,
        "saved signal track has invalid or ambiguous timestamp byte order"
    );
    let mut bytes = raw.as_slice();
    let second_stamps = integers(&mut bytes, SECOND_SLOTS, little_endian)?;
    let second_peaks = peaks(&mut bytes, SECOND_SLOTS, little_endian)?;
    let second_signal = bytes[..SECOND_SLOTS].to_vec();
    bytes = &bytes[SECOND_SLOTS..];
    let minute_stamps = integers(&mut bytes, MINUTE_SLOTS, little_endian)?;
    let minute_peaks = peaks(&mut bytes, MINUTE_SLOTS, little_endian)?;
    let minute_signal = counts(&mut bytes, MINUTE_SLOTS, little_endian)?;
    let minute_recorded = counts(&mut bytes, MINUTE_SLOTS, little_endian)?;
    ensure!(
        second_signal.iter().all(|value| *value <= 1)
            && minute_signal
                .iter()
                .zip(&minute_recorded)
                .all(|(signal, recorded)| signal <= recorded)
            && minute_recorded
                .iter()
                .all(|value| *value <= SECONDS_PER_MINUTE as u16),
        "saved signal track has invalid coverage counts"
    );
    Ok(RawTrack {
        second_stamps,
        second_peaks,
        second_signal,
        minute_stamps,
        minute_peaks,
        minute_signal,
        minute_recorded,
    })
}

fn valid_stamps(bytes: &[u8], slots: usize, little: bool, now: i64) -> bool {
    bytes
        .as_chunks::<TIMESTAMP_BYTES>()
        .0
        .iter()
        .enumerate()
        .all(|(slot, bytes)| {
            let value = if little {
                i64::from_le_bytes(*bytes)
            } else {
                i64::from_be_bytes(*bytes)
            };
            value == 0
                || (value > 0 && value <= now + RETENTION_SECONDS && value as usize % slots == slot)
        })
}

fn integers(bytes: &mut &[u8], length: usize, little: bool) -> Result<Vec<i64>> {
    let (values, remainder) = bytes
        .split_at_checked(length * TIMESTAMP_BYTES)
        .context("truncated signal timestamps")?;
    *bytes = remainder;
    values
        .as_chunks::<TIMESTAMP_BYTES>()
        .0
        .iter()
        .map(|bytes| {
            let value = *bytes;
            Ok(if little {
                i64::from_le_bytes(value)
            } else {
                i64::from_be_bytes(value)
            })
        })
        .collect()
}

fn peaks(bytes: &mut &[u8], length: usize, little: bool) -> Result<Vec<f32>> {
    let (values, remainder) = bytes
        .split_at_checked(length * PEAK_BYTES)
        .context("truncated signal peaks")?;
    *bytes = remainder;
    values
        .as_chunks::<PEAK_BYTES>()
        .0
        .iter()
        .map(|bytes| {
            let value = *bytes;
            let value = if little {
                f32::from_le_bytes(value)
            } else {
                f32::from_be_bytes(value)
            };
            ensure!(
                value.is_finite() || value == f32::NEG_INFINITY,
                "invalid saved signal peak"
            );
            Ok(value)
        })
        .collect()
}

fn counts(bytes: &mut &[u8], length: usize, little: bool) -> Result<Vec<u16>> {
    let (values, remainder) = bytes
        .split_at_checked(length * COUNT_BYTES)
        .context("truncated signal counts")?;
    *bytes = remainder;
    values
        .as_chunks::<COUNT_BYTES>()
        .0
        .iter()
        .map(|bytes| {
            let value = *bytes;
            Ok(if little {
                u16::from_le_bytes(value)
            } else {
                u16::from_be_bytes(value)
            })
        })
        .collect()
}
