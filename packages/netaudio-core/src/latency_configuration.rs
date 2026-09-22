use serde_json::{Map, Value};

pub fn requested_nanoseconds(milliseconds: f64) -> Result<u32, crate::protocol::NetaudioError> {
    if !milliseconds.is_finite()
        || !(0.0..=crate::commands::MAX_LATENCY_MILLISECONDS).contains(&milliseconds)
    {
        return Err(crate::protocol::NetaudioError::InvalidLatency);
    }

    Ok((milliseconds * 1_000_000.0).round() as u32)
}

#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LatencyControl {
    requested_milliseconds: f64,
    settings: Option<Map<String, Value>>,
    acknowledged: Option<bool>,
}

#[derive(serde::Serialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LatencyOutcome {
    Rejected,
    Unavailable,
    Confirmed,
    Unverified,
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LatencyCompletion {
    pub state: LatencyOutcome,
    pub requested_latency_ns: u32,
    pub configured_latency_ns: Option<u32>,
    pub configured_latency_ms: Option<f64>,
    pub effective_state_confirmed: bool,
}

impl LatencyControl {
    pub fn resolve(self) -> Result<LatencyCompletion, crate::spec::SpecError> {
        use LatencyOutcome::*;
        let requested = requested_nanoseconds(self.requested_milliseconds)?;
        let projection = configuration(&self.settings.unwrap_or_default())
            .map_err(|message| crate::spec::SpecError::InvalidJson(message.into()))?;
        let configured = projection.state.configured_latency_ns.flatten();
        let state = match (self.acknowledged, configured) {
            (Some(false), _) => Rejected,
            (None, _) | (_, None) => Unavailable,
            (Some(true), Some(value)) if value == requested => Confirmed,
            _ => Unverified,
        };

        Ok(LatencyCompletion {
            effective_state_confirmed: state == Confirmed,
            state,
            requested_latency_ns: requested,
            configured_latency_ns: configured,
            configured_latency_ms: configured.map(milliseconds),
        })
    }
}

const STANDARD_LATENCIES_NS: [u32; 6] =
    [150_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000];

fn milliseconds(value: u32) -> f64 {
    f64::from(value) / 1_000_000.0
}

#[derive(Default, serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LatencyState {
    #[serde(skip_serializing_if = "Option::is_none")]
    active_latency_ns: Option<Option<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_latency_ms: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    configured_latency_ns: Option<Option<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    configured_latency_ms: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_latency_ns: Option<Option<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_latency_ms: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_latency_ns: Option<Option<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_latency_ms: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_latency_ns: Option<Option<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_latency_ms: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_options_ms: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_options_ns: Option<Vec<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_options_source: Option<LatencyOptionsSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_latency_is_standard_choice: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_latency_within_reported_range: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    configured_latency_is_standard_choice: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    configured_latency_within_reported_range: Option<bool>,
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LatencyOptionsSource {
    DeviceReportsNoUsableRange,
    ControllerFixedSetFilteredByReportedRange,
}

#[derive(Default, serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LatencyControls {
    #[serde(skip_serializing_if = "Option::is_none")]
    active_latency: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    configured_latency: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_latency: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_latency: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_latency: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency: Option<Option<f64>>,
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LatencyConfiguration {
    state: LatencyState,
    controls: LatencyControls,
}

pub fn configuration(settings: &Map<String, Value>) -> Result<LatencyConfiguration, &'static str> {
    let mut state = LatencyState::default();
    let mut controls = LatencyControls::default();

    for (field, ns, ms, control) in [
        (
            "active",
            &mut state.active_latency_ns,
            &mut state.active_latency_ms,
            &mut controls.active_latency,
        ),
        (
            "configured",
            &mut state.configured_latency_ns,
            &mut state.configured_latency_ms,
            &mut controls.configured_latency,
        ),
        (
            "default",
            &mut state.default_latency_ns,
            &mut state.default_latency_ms,
            &mut controls.default_latency,
        ),
        (
            "min",
            &mut state.min_latency_ns,
            &mut state.min_latency_ms,
            &mut controls.min_latency,
        ),
        (
            "max",
            &mut state.max_latency_ns,
            &mut state.max_latency_ms,
            &mut controls.max_latency,
        ),
    ] {
        let key = format!("{field}_latency_ns");
        let Some(value) = settings.get(&key) else {
            continue;
        };
        let nanoseconds = if value.is_null() {
            None
        } else {
            Some(
                value
                    .as_u64()
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or("latency must be null or an unsigned 32-bit nanosecond value")?,
            )
        };
        *ns = Some(nanoseconds);
        *ms = Some(nanoseconds.map(milliseconds));
        *control = *ms;
    }

    let active = state.active_latency_ns.flatten();
    let configured = state.configured_latency_ns.flatten();

    if state.active_latency_ns.is_some() || state.configured_latency_ns.is_some() {
        controls.latency = Some(active.or(configured).map(milliseconds));
    }

    if let (Some(minimum), Some(maximum)) = (
        state.min_latency_ns.flatten(),
        state.max_latency_ns.flatten(),
    ) {
        let mut choices = STANDARD_LATENCIES_NS
            .into_iter()
            .filter(|choice| {
                minimum > 0 && maximum > minimum && minimum <= *choice && *choice <= maximum
            })
            .collect::<Vec<_>>();
        let fixed = choices.is_empty();

        if fixed {
            choices.extend(
                active
                    .filter(|value| *value > 0)
                    .or(configured.filter(|value| *value > 0)),
            );
        }

        state.latency_options_ms = Some(choices.iter().copied().map(milliseconds).collect());
        state.latency_options_source = Some(if fixed {
            LatencyOptionsSource::DeviceReportsNoUsableRange
        } else {
            LatencyOptionsSource::ControllerFixedSetFilteredByReportedRange
        });

        for (observed, standard_choice, within_range) in [
            (
                active,
                &mut state.active_latency_is_standard_choice,
                &mut state.active_latency_within_reported_range,
            ),
            (
                configured,
                &mut state.configured_latency_is_standard_choice,
                &mut state.configured_latency_within_reported_range,
            ),
        ] {
            if let Some(observed) = observed {
                let standard = choices.contains(&observed);
                *standard_choice = Some(standard);
                *within_range = Some(if fixed {
                    standard
                } else {
                    minimum <= observed && observed <= maximum
                });
            }
        }

        state.latency_options_ns = Some(choices);
    }

    Ok(LatencyConfiguration { state, controls })
}
