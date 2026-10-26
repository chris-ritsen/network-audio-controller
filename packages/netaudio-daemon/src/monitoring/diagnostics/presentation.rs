use super::{ClockObservations, ConnectionHealthUpdate, Diagnostics, HISTORY_LIMIT, Series};
use crate::devices::registry::DiagnosticOptions;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Serialize)]
#[serde(untagged)]
enum Receiver<'a> {
    Retained(&'a ConnectionHealthUpdate),
    Projected(Value),
}

#[derive(Serialize)]
#[serde(untagged)]
enum Clock<'a> {
    Retained(&'a ClockObservations),
    Projected(Value),
}

#[derive(Serialize)]
pub struct Snapshot<'a> {
    device: &'a str,
    device_identity: &'a Option<String>,
    receiver: Option<Receiver<'a>>,
    clock_state: Option<&'a Value>,
    clock_state_observed_at: Option<&'a Value>,
    retention_limit: usize,
    timestamp_provenance: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    clock: Option<Option<Clock<'a>>>,
}

impl Diagnostics {
    pub fn snapshot<'a>(
        &'a self,
        device: &'a str,
        clock_state: Option<&'a Value>,
        clock_state_observed_at: Option<&'a Value>,
        options: &DiagnosticOptions,
    ) -> Result<Snapshot<'a>> {
        let receiver = self
            .receiver
            .as_ref()
            .map(|receiver| -> Result<_> {
                if options.points {
                    let mut value = serde_json::to_value(receiver.without_history())?;
                    let paths = value["paths"]
                        .as_array_mut()
                        .context("receiver paths are not an array")?;
                    for (path, value) in receiver.paths.iter().zip(paths) {
                        value["late_packets"]
                            .as_object_mut()
                            .context("late packet series is not an object")?
                            .remove("history");
                        series_points(&path.latency, &mut value["latency"])?;
                    }
                    Ok(Receiver::Projected(value))
                } else if options.receiver_history {
                    Ok(Receiver::Retained(receiver))
                } else {
                    Ok(Receiver::Projected(serde_json::to_value(
                        receiver.summary(),
                    )?))
                }
            })
            .transpose()?;
        let clock = if options.include_clock {
            Some(if !self.clock_observed {
                None
            } else if options.points {
                let mut value = serde_json::to_value(ClockObservations {
                    warning_enabled: self.clock.warning_enabled,
                    variation: self.clock.variation.clone(),
                    heartbeat: self.clock.heartbeat.without_history(),
                    conmon: self.clock.conmon.without_history(),
                    diagnostics: self.clock.diagnostics.clone(),
                })?;
                series_points(&self.clock.heartbeat, &mut value["heartbeat"])?;
                series_points(&self.clock.conmon, &mut value["conmon"])?;
                Some(Clock::Projected(value))
            } else if options.clock_history {
                Some(Clock::Retained(&self.clock))
            } else {
                Some(Clock::Projected(self.clock_snapshot(false)?))
            })
        } else {
            None
        };
        Ok(Snapshot {
            device,
            device_identity: &self.identity,
            receiver,
            clock_state,
            clock_state_observed_at,
            retention_limit: HISTORY_LIMIT,
            timestamp_provenance: "local_receive_time",
            clock,
        })
    }
}

fn series_points(series: &Series, value: &mut Value) -> Result<()> {
    let points: Vec<_> = series
        .history
        .iter()
        .filter(|sample| sample.display_epoch == series.display_epoch)
        .filter_map(|sample| {
            sample.value.map(|value| {
                json!([
                    (sample.observed_monotonic * 10.0).round_ties_even() / 10.0,
                    value,
                ])
            })
        })
        .collect();
    let fields = value
        .as_object_mut()
        .context("diagnostic series is not an object")?;
    fields.remove("history");
    if points
        .iter()
        .any(|point| point[1].as_f64().is_some_and(|value| value != 0.0))
    {
        fields.insert("points".to_owned(), json!(points));
    }
    Ok(())
}
