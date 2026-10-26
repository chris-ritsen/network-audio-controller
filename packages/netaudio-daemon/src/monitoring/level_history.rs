use anyhow::{Result, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};
use tokio::sync::Notify;

mod legacy;
pub mod store;

pub const SECOND_SLOTS: usize = 900;
pub const MINUTE_SLOTS: usize = 1440;
pub const RETENTION_SECONDS: i64 = MINUTE_SLOTS as i64 * SECONDS_PER_MINUTE;
const SECONDS_PER_MINUTE: i64 = 60;
const TIMELINE_BUCKETS: i64 = 20;
const TIMELINE_STEPS: [i64; 13] = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600];
const UNRECORDED: i64 = i64::MIN;

#[derive(Clone, Copy, PartialEq)]
struct Second {
    stamp: i64,
    peak: Option<f32>,
    signal: bool,
}

impl Default for Second {
    fn default() -> Self {
        Self {
            stamp: UNRECORDED,
            peak: None,
            signal: false,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
struct Minute {
    stamp: i64,
    peak: Option<f32>,
    peak_second: i64,
    signal: u64,
    recorded: u64,
}

impl Default for Minute {
    fn default() -> Self {
        Self {
            stamp: UNRECORDED,
            peak: None,
            peak_second: UNRECORDED,
            signal: 0,
            recorded: 0,
        }
    }
}

struct Track {
    seconds: Box<[Second]>,
    minutes: Box<[Minute]>,
    first_second: i64,
    newest_second: i64,
    dirty_seconds: BTreeSet<usize>,
    dirty_minutes: BTreeSet<usize>,
}

impl Track {
    fn new(second: i64) -> Self {
        Self {
            seconds: vec![Second::default(); SECOND_SLOTS].into_boxed_slice(),
            minutes: vec![Minute::default(); MINUTE_SLOTS].into_boxed_slice(),
            first_second: second,
            newest_second: second,
            dirty_seconds: BTreeSet::new(),
            dirty_minutes: BTreeSet::new(),
        }
    }

    fn record(&mut self, second: i64, peak: Option<f32>, signal: bool) -> bool {
        self.first_second = self.first_second.min(second);
        self.newest_second = self.newest_second.max(second);
        let second_slot = second.rem_euclid(SECOND_SLOTS as i64) as usize;
        let reading = &mut self.seconds[second_slot];
        let previous_second = *reading;

        if reading.stamp < second {
            *reading = Second {
                stamp: second,
                peak,
                signal,
            };
        } else if reading.stamp == second {
            reading.peak = maximum(reading.peak, peak);
            reading.signal |= signal;
        }

        if *reading != previous_second {
            self.dirty_seconds.insert(second_slot);
        }

        let minute = second.div_euclid(SECONDS_PER_MINUTE);
        let minute_slot = minute.rem_euclid(MINUTE_SLOTS as i64) as usize;
        let aggregate = &mut self.minutes[minute_slot];

        if aggregate.stamp > minute {
            return *reading != previous_second;
        }

        let previous_minute = *aggregate;
        if aggregate.stamp != minute {
            *aggregate = Minute {
                stamp: minute,
                ..Minute::default()
            };
        }

        let bit = 1 << second.rem_euclid(SECONDS_PER_MINUTE);
        aggregate.recorded |= bit;

        if signal {
            aggregate.signal |= bit;
        }

        if peak.is_some_and(|peak| aggregate.peak.is_none_or(|previous| peak > previous)) {
            aggregate.peak = peak;
            aggregate.peak_second = second;
        }

        if *aggregate != previous_minute {
            self.dirty_minutes.insert(minute_slot);
        }

        *reading != previous_second || *aggregate != previous_minute
    }

    fn second(&self, second: i64) -> Option<Second> {
        let reading = self.seconds[second.rem_euclid(SECOND_SLOTS as i64) as usize];
        (reading.stamp == second).then_some(reading)
    }

    fn minute(&self, minute: i64) -> Option<Minute> {
        let reading = self.minutes[minute.rem_euclid(MINUTE_SLOTS as i64) as usize];
        (reading.stamp == minute).then_some(reading)
    }
}

fn maximum(first: Option<f32>, second: Option<f32>) -> Option<f32> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.max(second)),
        (first, second) => first.or(second),
    }
}

#[derive(Default)]
struct Source {
    tracks: BTreeMap<String, Track>,
}

#[derive(Default)]
pub struct LevelHistory {
    sources: Mutex<BTreeMap<String, Source>>,
    legacy: Mutex<BTreeMap<String, BTreeMap<String, legacy::LegacyTrack>>>,
    changed: Notify,
}

#[derive(Clone, Copy)]
pub struct Period {
    pub start: i64,
    pub end: i64,
}

impl Period {
    pub fn new(start: i64, end: i64) -> Result<Self> {
        ensure!(
            start >= 0
                && end > start
                && DateTime::<Utc>::from_timestamp(start, 0).is_some()
                && DateTime::<Utc>::from_timestamp(end, 0).is_some(),
            "invalid signal history period"
        );
        ensure!(
            end - start <= RETENTION_SECONDS,
            "levels are kept for 24 hours"
        );
        Ok(Self { start, end })
    }
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct Summary {
    pub start: String,
    pub end: String,
    pub resolution_seconds: i64,
    pub recorded_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loudest_dbfs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loudest_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_signal_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_signal_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline: Option<Timeline>,
    pub peak_coverage_complete: bool,
    pub legacy_saved_coverage: bool,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct Timeline {
    pub start: String,
    pub step_seconds: i64,
    pub peaks_dbfs: String,
}

#[derive(Default)]
struct Bucket {
    start: i64,
    peak: Option<f32>,
    peak_second: Option<i64>,
    recorded: u64,
    signal: u64,
    first_signal: Option<i64>,
    last_signal: Option<i64>,
    incomplete_peak: bool,
    legacy_saved_coverage: bool,
}

impl Bucket {
    fn note_peak(&mut self, peak: Option<f32>, second: i64) {
        if peak.is_some_and(|peak| self.peak.is_none_or(|previous| peak > previous)) {
            self.peak = peak;
            self.peak_second = Some(second);
        }
    }

    fn note_signal(&mut self, first: i64, last: i64) {
        self.first_signal = Some(self.first_signal.map_or(first, |old| old.min(first)));
        self.last_signal = Some(self.last_signal.map_or(last, |old| old.max(last)));
    }
}

impl LevelHistory {
    pub fn paired_difference(
        &self,
        before: (&str, &str),
        after: (&str, &str),
        period: Period,
    ) -> Result<Option<(f64, usize, i64)>> {
        let sources = self.lock()?;
        let track = |key: (&str, &str)| {
            sources
                .get(key.0)
                .and_then(|source| source.tracks.get(key.1))
        };
        let (Some(before), Some(after)) = (track(before), track(after)) else {
            return Ok(None);
        };
        let mut differences = Vec::new();
        let mut last = None;
        for second in period.start.max(period.end - SECOND_SLOTS as i64)..period.end {
            if let (Some(first), Some(second_reading)) =
                (before.second(second), after.second(second))
                && first.signal
                && second_reading.signal
                && let (Some(first), Some(second_peak)) = (first.peak, second_reading.peak)
            {
                differences.push(f64::from(second_peak - first));
                last = Some(second);
            }
        }
        let Some(last) = last else {
            return Ok(None);
        };
        differences.sort_by(f64::total_cmp);
        Ok(Some((
            differences[differences.len() / 2],
            differences.len(),
            last,
        )))
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<String, Source>>> {
        self.sources
            .lock()
            .map_err(|_| anyhow::anyhow!("signal history lock poisoned"))
    }

    pub fn record_meter(&self, name: &str, sample: &Value) -> Result<()> {
        let second = sample["wall_time"]
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .ok_or_else(|| anyhow::anyhow!("meter sample has no valid observation time"))?
            .floor() as i64;
        let mut sources = self.lock()?;
        let source = sources.entry(format!("dante:{name}")).or_default();
        let mut changed = false;

        for direction in ["tx", "rx"] {
            let readings = sample[format!("{direction}_normalized")]
                .as_object()
                .into_iter()
                .flatten();

            for (channel, reading) in readings {
                let (peak, signal) = match reading["state"].as_str() {
                    Some("clipping") => (Some(0.0), true),
                    Some("signal_present" | "below_threshold") => {
                        let Some(peak) = reading["dbfs"].as_f64().filter(|value| value.is_finite())
                        else {
                            continue;
                        };
                        (Some(peak as f32), reading["state"] == "signal_present")
                    }
                    Some("muted" | "mute_or_floor") => (None, false),
                    _ => continue,
                };
                changed |= source
                    .tracks
                    .entry(format!("{direction}:{channel}"))
                    .or_insert_with(|| Track::new(second))
                    .record(second, peak, signal);
            }
        }

        if changed {
            self.changed.notify_one();
        }
        Ok(())
    }

    pub fn record_value(
        &self,
        source: &str,
        point: &str,
        second: i64,
        peak: Option<f32>,
        signal: bool,
    ) -> Result<()> {
        ensure!(
            !source.is_empty()
                && !point.is_empty()
                && second >= 0
                && peak.is_none_or(f32::is_finite),
            "Invalid signal history observation"
        );
        let mut sources = self.lock()?;
        let changed = sources
            .entry(source.to_owned())
            .or_default()
            .tracks
            .entry(point.to_owned())
            .or_insert_with(|| Track::new(second))
            .record(second, peak, signal);
        if changed {
            self.changed.notify_one();
        }
        Ok(())
    }

    pub fn summary(&self, keys: &[(&str, &str)], period: Period, now: i64) -> Result<Summary> {
        let sources = self.lock()?;
        let legacy = self
            .legacy
            .lock()
            .map_err(|_| anyhow::anyhow!("saved signal history lock poisoned"))?;
        let tracks: Vec<_> = keys
            .iter()
            .map(|(source, point)| QueryTrack {
                current: sources
                    .get(*source)
                    .and_then(|source| source.tracks.get(*point)),
                saved: legacy.get(*source).and_then(|source| source.get(*point)),
            })
            .collect();
        let resolution = if period.start > now - SECOND_SLOTS as i64 {
            1
        } else {
            SECONDS_PER_MINUTE
        };
        let buckets = if resolution == 1 {
            second_buckets(&tracks, period)
        } else {
            minute_buckets(&tracks, period)
        };
        let recorded: u64 = buckets.iter().map(|bucket| bucket.recorded).sum();
        let signal: u64 = buckets.iter().map(|bucket| bucket.signal).sum();
        let loudest = buckets
            .iter()
            .filter_map(|bucket| Some((bucket.peak?, bucket.peak_second?)))
            .max_by(|first, second| {
                first
                    .0
                    .total_cmp(&second.0)
                    .then_with(|| second.1.cmp(&first.1))
            });
        let first_signal = buckets.iter().find_map(|bucket| bucket.first_signal);
        let last_signal = buckets.iter().rev().find_map(|bucket| bucket.last_signal);
        Ok(Summary {
            start: iso_second(period.start),
            end: iso_second(period.end),
            resolution_seconds: resolution,
            recorded_seconds: recorded,
            recorded_since: (recorded == 0)
                .then(|| {
                    tracks
                        .iter()
                        .filter_map(QueryTrack::first_second)
                        .min()
                        .map(iso_second)
                })
                .flatten(),
            signal_seconds: (recorded != 0).then_some(signal),
            loudest_dbfs: loudest
                .map(|(peak, _)| (f64::from(peak) * 10.0).round_ties_even() / 10.0),
            loudest_at: loudest.map(|(_, second)| iso_second(second)),
            first_signal_at: first_signal.map(iso_second),
            last_signal_at: last_signal.map(iso_second),
            timeline: (signal != 0).then(|| timeline(&buckets, resolution, period)),
            peak_coverage_complete: buckets.iter().all(|bucket| !bucket.incomplete_peak),
            legacy_saved_coverage: buckets.iter().any(|bucket| bucket.legacy_saved_coverage),
        })
    }
}

struct QueryTrack<'a> {
    current: Option<&'a Track>,
    saved: Option<&'a legacy::LegacyTrack>,
}

impl QueryTrack<'_> {
    fn first_second(&self) -> Option<i64> {
        [
            self.current.map(|track| track.first_second),
            self.saved.map(legacy::LegacyTrack::first_second),
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

fn second_buckets(tracks: &[QueryTrack<'_>], period: Period) -> Vec<Bucket> {
    (period.start..period.end)
        .map(|second| {
            let mut bucket = Bucket {
                start: second,
                ..Bucket::default()
            };

            for track in tracks {
                let reading = track.current.and_then(|track| track.second(second));
                let (peak, signal) = if let Some(reading) = reading {
                    (reading.peak, reading.signal)
                } else if let Some(reading) = track.saved.and_then(|track| track.second(second)) {
                    bucket.legacy_saved_coverage = true;
                    (reading.peak, reading.signal != 0)
                } else {
                    continue;
                };
                bucket.recorded = 1;
                bucket.note_peak(peak, second);

                if signal {
                    bucket.signal = 1;
                    bucket.note_signal(second, second);
                }
            }

            bucket
        })
        .collect()
}

fn minute_buckets(tracks: &[QueryTrack<'_>], period: Period) -> Vec<Bucket> {
    let first_minute = period.start.div_euclid(SECONDS_PER_MINUTE);
    let last_minute = (period.end - 1).div_euclid(SECONDS_PER_MINUTE);
    (first_minute..=last_minute)
        .map(|minute| {
            let origin = minute * SECONDS_PER_MINUTE;
            let start = period.start.max(origin);
            let end = period.end.min(origin + SECONDS_PER_MINUTE);
            let mask = ((1_u64 << (end - origin)) - 1) & !((1_u64 << (start - origin)) - 1);
            let mut bucket = Bucket {
                start,
                ..Bucket::default()
            };
            let mut recorded = 0;
            let mut signal = 0;

            for track in tracks {
                let Some(current) = track.current else {
                    continue;
                };
                let Some(reading) = current.minute(minute) else {
                    continue;
                };
                recorded |= reading.recorded & mask;
                signal |= reading.signal & mask;

                if reading.peak.is_none() || (start..end).contains(&reading.peak_second) {
                    bucket.note_peak(reading.peak, reading.peak_second);
                    continue;
                }

                for second in start..end {
                    if reading.recorded & (1 << (second - origin)) == 0 {
                        continue;
                    }

                    match current.second(second) {
                        Some(reading) => bucket.note_peak(reading.peak, second),
                        None => bucket.incomplete_peak = true,
                    }
                }
            }

            bucket.recorded = u64::from(recorded.count_ones());
            bucket.signal = u64::from(signal.count_ones());

            if signal != 0 {
                bucket.note_signal(
                    origin + i64::from(signal.trailing_zeros()),
                    origin + i64::from(u64::BITS - 1 - signal.leading_zeros()),
                );
            }

            for saved in tracks
                .iter()
                .filter_map(|track| track.saved.and_then(|track| track.minute(minute)))
            {
                bucket.legacy_saved_coverage = true;

                if start != origin || end != origin + SECONDS_PER_MINUTE {
                    bucket.incomplete_peak = true;
                    continue;
                }

                bucket.recorded = bucket.recorded.max(saved.recorded);
                bucket.signal = bucket.signal.max(saved.signal);
                bucket.note_peak(saved.peak, origin);

                if saved.signal != 0 {
                    bucket.note_signal(origin, origin + SECONDS_PER_MINUTE - 1);
                }
            }

            bucket
        })
        .collect()
}

fn timeline(buckets: &[Bucket], resolution: i64, period: Period) -> Timeline {
    let span = period.end - period.start;
    let step = TIMELINE_STEPS
        .into_iter()
        .find(|step| *step >= resolution && span <= step * TIMELINE_BUCKETS)
        .unwrap_or(TIMELINE_STEPS[TIMELINE_STEPS.len() - 1]);
    let mut groups = vec![(false, false, None); ((span + step - 1) / step) as usize];

    for bucket in buckets {
        let index = ((bucket.start - period.start) / step) as usize;
        let group = &mut groups[index];
        group.0 |= bucket.recorded != 0;
        group.1 |= bucket.signal != 0;
        group.2 = maximum(group.2, bucket.peak);
    }

    let marks = groups
        .into_iter()
        .map(|(recorded, signal, peak)| {
            if !recorded {
                "?".to_owned()
            } else if !signal {
                "·".to_owned()
            } else {
                peak.map_or_else(
                    || "?".to_owned(),
                    |peak| format!("{:.0}", peak.round_ties_even()),
                )
            }
        })
        .collect::<Vec<_>>();
    Timeline {
        start: iso_second(period.start),
        step_seconds: step,
        peaks_dbfs: marks.join(" "),
    }
}

pub fn iso_second(second: i64) -> String {
    DateTime::<Utc>::from_timestamp(second, 0)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_default()
}
