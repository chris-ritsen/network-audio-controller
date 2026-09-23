use serde::{Deserialize, Serialize};

use crate::network::{redundancy_control, RedundancyControlRequest};
use crate::responses::{audio_capability_control, AudioCapabilityControl};

#[derive(Serialize)]
pub struct AudioValueChoice {
    pub value: u32,
    pub label: &'static str,
    pub aliases: &'static [&'static str],
}

pub fn sample_rate_pullup_choices() -> [AudioValueChoice; 5] {
    [
        AudioValueChoice {
            value: 0,
            label: "none",
            aliases: &["none"],
        },
        AudioValueChoice {
            value: 1,
            label: "+4.1667%",
            aliases: &["+4.1667%", "4.1667%"],
        },
        AudioValueChoice {
            value: 2,
            label: "+0.1%",
            aliases: &["+0.1%", "0.1%"],
        },
        AudioValueChoice {
            value: 3,
            label: "-0.1%",
            aliases: &["-0.1%"],
        },
        AudioValueChoice {
            value: 4,
            label: "-4.0%",
            aliases: &["-4.0%", "-4%"],
        },
    ]
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SettingsCapabilityFacts {
    pub multicast_prefix: Option<String>,
    pub properties: Option<Vec<serde_json::Value>>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SettingsCapabilities {
    pub aes67_multicast_prefix: bool,
}

pub fn settings_capabilities(facts: SettingsCapabilityFacts) -> SettingsCapabilities {
    let prefix_observed = facts
        .multicast_prefix
        .is_some_and(|value| !value.is_empty());
    let prefix_advertised = facts.properties.into_iter().flatten().any(|entry| {
        entry.get("property_id").and_then(serde_json::Value::as_u64)
            == Some(u64::from(
                crate::responses::DEVICE_SETTINGS_INFO_AES67_MULTICAST_PREFIX,
            ))
    });

    SettingsCapabilities {
        aes67_multicast_prefix: prefix_observed || prefix_advertised,
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Identify,
    SampleRate,
    Encoding,
    SampleRatePullup,
    Aes67,
    StaticIpv4,
    Redundancy,
    CodecControl,
    Locking,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReportedSource {
    pub fresh: Option<bool>,
    pub field_reported: Option<bool>,
    pub field_applicable: Option<bool>,
}

impl ReportedSource {
    fn reported(&self) -> bool {
        self.fresh == Some(true) && self.field_reported == Some(true)
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AvailabilityRequest {
    pub operation: Operation,
    pub supported: Option<bool>,
    pub supported_source: Option<ReportedSource>,
    pub readable: bool,
    pub read_only: Option<bool>,
    pub read_only_source: Option<ReportedSource>,
    pub locked: Option<bool>,
    pub audio: Option<AudioCapabilityControl>,
    pub redundancy: Option<RedundancyControlRequest>,
    pub transport_available: bool,
    pub has_adapter: bool,
    pub managed: bool,
    pub permission: Option<bool>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Availability {
    pub supported: Option<bool>,
    pub read_only: Option<bool>,
    pub readable: bool,
    pub writable: bool,
    pub write_permitted: bool,
    pub reasons: Vec<&'static str>,
}

pub fn availability(request: AvailabilityRequest) -> Availability {
    use Operation::*;

    let mut reasons = Vec::new();
    let supported = request.supported.filter(|_| {
        request.operation != Redundancy
            || request
                .supported_source
                .as_ref()
                .is_some_and(ReportedSource::reported)
    });
    let read_only = request.read_only.filter(|_| {
        request.operation != Redundancy
            || request
                .read_only_source
                .as_ref()
                .is_some_and(ReportedSource::reported)
    });
    let read_only_required = request.operation == Redundancy
        && !request
            .read_only_source
            .as_ref()
            .is_some_and(|source| source.field_applicable == Some(false));

    match supported {
        Some(true) => {}
        Some(false) => reasons.push("unsupported"),
        None => reasons.push("capability_unknown"),
    }

    if matches!(request.operation, StaticIpv4 | Redundancy) {
        match read_only {
            Some(true) => reasons.push("read_only"),
            None if read_only_required => reasons.push("read_only_unknown"),
            _ => {}
        }
    }

    if request.operation == Redundancy {
        let control = request.redundancy.unwrap_or(RedundancyControlRequest {
            state: serde_json::Value::Null,
            mode: None,
            readback: None,
        });
        reasons.extend(redundancy_control(control).reasons);

        if !request.transport_available {
            reasons.push("transport_unavailable");
        }
    }

    if matches!(request.operation, SampleRate | Encoding | SampleRatePullup) {
        let control = request.audio.unwrap_or(AudioCapabilityControl {
            update_mode: None,
            available_values: None,
            requested_value: None,
            host_disabled: None,
        });
        reasons.extend(audio_capability_control(&control));
    }

    if request.operation == CodecControl && !request.has_adapter {
        reasons.push("no_device_adapter");
    }

    match (request.operation, request.locked) {
        (Identify, _) | (Locking, Some(_)) => {}
        (_, Some(true)) => reasons.push("device_locked"),
        (_, None) => reasons.push("lock_state_unknown"),
        _ => {}
    }

    if request.managed {
        match request.permission {
            Some(true) => {}
            Some(false) => reasons.push("managed_permission_denied"),
            None => reasons.push("managed_permission_missing"),
        }

        if request.operation == Locking {
            reasons.push("managed_transport_unavailable");
        }
    }

    // Unknown advertised capabilities do not establish support, but ordinary
    // controls may still be attempted. Explicit denials always block writes;
    // redundancy additionally requires affirmative, fresh evidence.
    let write_permitted = reasons.is_empty()
        || (request.operation != Redundancy
            && reasons.iter().all(|reason| {
                matches!(
                    *reason,
                    "capability_unknown" | "lock_state_unknown" | "update_mode_unknown"
                )
            }));

    Availability {
        supported,
        read_only,
        readable: request.readable,
        writable: reasons.is_empty(),
        write_permitted,
        reasons,
    }
}
