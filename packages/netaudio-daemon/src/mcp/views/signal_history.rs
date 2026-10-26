use super::{Records, channel_label, channels, local_time, text};
use crate::monitoring::level_history::{
    LevelHistory, Period, RETENTION_SECONDS, Summary, iso_second,
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use serde_json::{Map, Value, json};
use std::time::Duration;

const DEFAULT_HISTORY_SECONDS: i64 = 60;
const MAXIMUM_LISTEN_SECONDS: i64 = 30;

pub struct HistoryRequest {
    pub period: Period,
    pub listen: Option<Duration>,
}

impl HistoryRequest {
    pub fn parse(arguments: &Value, now: i64) -> Result<Option<Self>> {
        let listening = arguments.get("listen_seconds");
        let over = arguments.get("over_last_seconds");
        let ending = arguments.get("ending_at");

        if listening.is_none() && over.is_none() && ending.is_none() {
            return Ok(None);
        }

        if let Some(listening) = listening {
            ensure!(
                over.is_none() && ending.is_none(),
                "listen_seconds cannot be combined with over_last_seconds or ending_at"
            );
            let seconds = listening
                .as_i64()
                .filter(|value| (1..=MAXIMUM_LISTEN_SECONDS).contains(value))
                .context("listen_seconds must be an integer from 1 through 30")?;
            return Ok(Some(Self {
                period: Period::new(now, now + seconds)?,
                listen: Some(Duration::from_secs(seconds as u64)),
            }));
        }

        let end = ending
            .map(|value| parse_time(value.as_str().context("ending_at must be an ISO time")?))
            .transpose()?
            .unwrap_or(now);
        ensure!(
            end <= now + 1,
            "ending_at is in the future; use listen_seconds to wait for new sound"
        );
        let seconds = over
            .map(|value| {
                value
                    .as_i64()
                    .filter(|value| (1..=RETENTION_SECONDS).contains(value))
                    .context("over_last_seconds must be an integer from 1 through 86400")
            })
            .transpose()?
            .unwrap_or(DEFAULT_HISTORY_SECONDS);
        let start = end.checked_sub(seconds).context("invalid history period")?;
        ensure!(
            start >= now - RETENTION_SECONDS,
            "levels are kept for 24 hours"
        );
        Ok(Some(Self {
            period: Period::new(start, end)?,
            listen: None,
        }))
    }
}

fn parse_time(value: &str) -> Result<i64> {
    let value = value.trim();

    if let Ok(moment) = DateTime::parse_from_rfc3339(value) {
        return Ok(moment.timestamp());
    }

    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M"] {
        if let Ok(moment) = NaiveDateTime::parse_from_str(value, format) {
            return Local
                .from_local_datetime(&moment)
                .single()
                .map(|moment| moment.timestamp())
                .context(
                    "ending_at is ambiguous or does not exist locally; include an explicit offset",
                );
        }
    }

    bail!("ending_at must be an ISO time, such as 2026-10-02T23:00")
}

pub fn signal_history(
    history: &LevelHistory,
    records: &Records,
    selected: Option<&Value>,
    request: &HistoryRequest,
    debug: bool,
) -> Result<Value> {
    let now = chrono::Utc::now().timestamp();
    let mut result = Map::new();
    let mut silent = Vec::new();
    let mut unrecorded = Vec::new();
    result.insert(
        "period".to_owned(),
        json!(format!(
            "{} to {}",
            display_time(&iso_second(request.period.start)),
            display_time(&iso_second(request.period.end)),
        )),
    );
    let mut records: Vec<_> = records.values().collect();
    records.sort_by_key(|record| text(&record["name"]));

    for record in records {
        if selected.is_some_and(|selected| selected["server_name"] != record["server_name"])
            || (selected.is_none() && record["online"] != true)
        {
            continue;
        }

        let source = format!("dante:{}", text(&record["server_name"]));
        let mut device = Map::new();
        let mut observed = false;
        let mut heard_anything = false;
        let mut missing = false;
        let mut saved_coverage = false;

        for (direction, inventory) in [("rx", "receivers"), ("tx", "transmitters")] {
            let channels = &record["channels"][inventory];
            let mut channels: Vec<_> = channels.as_object().into_iter().flatten().collect();
            channels.sort_by_key(|(number, _)| number.parse::<u64>().unwrap_or_default());
            let mut heard = Vec::new();
            let mut quiet = Vec::new();
            let mut saved_quiet = Vec::new();
            let mut unknown = Vec::new();
            let mut summaries = Map::new();

            for (number, channel) in channels {
                let point = format!("{direction}:{number}");
                let summary = history.summary(&[(&source, &point)], request.period, now)?;
                saved_coverage |= summary.legacy_saved_coverage;
                let name = text(&channel_label(channel));
                let label = if name.is_empty() || name == *number {
                    number.clone()
                } else {
                    format!("{number} {name}")
                };

                if summary.recorded_seconds == 0 {
                    missing = true;
                    unknown.push(number.parse::<u64>()?);
                } else {
                    observed = true;

                    if summary.signal_seconds.unwrap_or_default() != 0 {
                        heard_anything = true;
                        heard.push(format!(
                            "{label}: {}",
                            history_text(&summary, selected.is_some())
                        ));
                    } else if summary.legacy_saved_coverage {
                        saved_quiet.push(number.parse::<u64>()?);
                    } else {
                        quiet.push(number.parse::<u64>()?);
                    }
                }

                if debug {
                    summaries.insert(number.clone(), serde_json::to_value(summary)?);
                }
            }

            if !heard.is_empty() {
                device.insert(direction.to_owned(), json!(heard));
            }

            if selected.is_some() || debug {
                if !saved_quiet.is_empty() {
                    device.insert(
                        format!("{direction}_saved_silent_or_unavailable"),
                        json!(channels::number_ranges(saved_quiet)),
                    );
                }

                if !quiet.is_empty() {
                    device.insert(
                        format!("{direction}_silent"),
                        json!(channels::number_ranges(quiet)),
                    );
                }

                if !unknown.is_empty() {
                    device.insert(
                        format!("{direction}_not_recorded"),
                        json!(channels::number_ranges(unknown)),
                    );
                }
            }

            if debug {
                device.insert(format!("{direction}_history"), Value::Object(summaries));
            }
        }

        let name = text(&record["name"]);

        if !observed {
            unrecorded.push(name.clone());
        } else if !heard_anything && !saved_coverage {
            silent.push(name.clone());
        }

        if missing && selected.is_none() && observed {
            device.insert(
                "coverage".to_owned(),
                json!("some channels have no measurements in this period"),
            );
        }

        if saved_coverage {
            device.insert("saved_coverage_note".to_owned(), json!(
                "Includes saved Python history whose coverage may combine missing and silent readings. Minute timestamps are bounds and combined counts are lower bounds."
            ));
        }

        if !device.is_empty() {
            result.insert(name, Value::Object(device));
        }
    }

    if !silent.is_empty() {
        result.insert("no_signal".to_owned(), json!(silent));
    }

    if !unrecorded.is_empty() {
        result.insert("not_recorded".to_owned(), json!(unrecorded));
    }

    Ok(Value::Object(result))
}

fn history_text(summary: &Summary, full: bool) -> String {
    let signal = summary.signal_seconds.unwrap_or_default();
    let mut result = format!(
        "signal in {signal} of {} recorded seconds",
        summary.recorded_seconds
    );

    if let Some(peak) = summary.loudest_dbfs {
        result.push_str(&format!(", loudest {peak} dBFS"));

        if full && let Some(moment) = &summary.loudest_at {
            result.push_str(&format!(" at {}", display_time(moment)));
        }
    }

    if full {
        if let Some(moment) = &summary.last_signal_at {
            result.push_str(&format!(", last signal {}", display_time(moment)));
        }

        if let Some(timeline) = &summary.timeline {
            result.push_str(&format!(
                "; peak dBFS every {} s from {}: {}",
                timeline.step_seconds,
                display_time(&timeline.start),
                timeline.peaks_dbfs,
            ));
        }
    }

    if !summary.peak_coverage_complete {
        result.push_str("; peak unavailable for part of a minute at the period boundary");
    }

    result
}

fn display_time(value: &str) -> String {
    local_time(&json!(value)).unwrap_or_else(|| value.to_owned())
}
