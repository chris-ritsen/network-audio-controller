use super::operations::ControlError;
use netaudio_core::clock_configuration::{
    ClockPlan, ClockPlanRequest, ClockProfile, ClockReadbackRequest, configuration_matches,
    control_profile, normalize_subdomain, plan_configuration,
};
use serde_json::{Map, Value, json};

pub const PROFILE_PARAMETER_ERROR: &str =
    "Use control_profile to select the local clock serializer.";

#[derive(Clone, Debug)]
pub struct ClockOperation {
    pub changes: Map<String, Value>,
    pub profile: u16,
}

pub struct ClockReadback {
    pub requested: Map<String, Value>,
    pub profile: u16,
    pub request_sent: bool,
}

impl ClockOperation {
    pub fn new(changes: Map<String, Value>, profile: Option<u16>) -> Result<Self, ControlError> {
        let profile = control_profile(&ClockProfile {
            control_profile: profile,
        })
        .map_err(|error| ControlError::Conflict(error.to_owned()))?;
        Ok(Self { changes, profile })
    }

    pub fn plan(&self, record: &Value) -> Result<ClockPlan, ControlError> {
        let status = record["clock_status"]
            .as_object()
            .cloned()
            .ok_or_else(|| ControlError::Unavailable("clock status is unavailable".to_owned()))?;
        let supported = record
            .get("supported_clock_sources")
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or_else(|| json!([]));
        let supported_clock_sources = serde_json::from_value(supported)
            .map_err(|error| ControlError::Unavailable(error.to_string()))?;

        plan_configuration(ClockPlanRequest {
            status,
            changes: self.changes.clone(),
            control_profile: Some(self.profile),
            supported_clock_sources,
        })
        .map_err(ControlError::Conflict)
    }

    pub fn precondition(&self, record: &Value) -> Value {
        let before = self.plan(record).ok().map(|plan| plan.before);
        let mut condition = json!({
            "before": before,
            "supported_clock_sources": record["supported_clock_sources"],
        });

        for field in ["record_revision", "clock_capabilities", "extension_flags"] {
            condition[field] = record["clock_status"][field].clone();
        }

        condition
    }
}

impl ClockReadback {
    pub fn matches(&self, fields: &Map<String, Value>) -> bool {
        fields
            .get("clock_status")
            .and_then(Value::as_object)
            .is_some_and(|status| {
                configuration_matches(&ClockReadbackRequest {
                    status: status.clone(),
                    requested: self.requested.clone(),
                })
                .unwrap_or(false)
            })
    }

    pub fn completion(&self, observed: Option<&Map<String, Value>>) -> Value {
        json!({
            "request_sent": self.request_sent,
            "request_acknowledged": null,
            "effective_state_confirmed": observed.is_some_and(|fields| self.matches(fields)),
            "persistence": "unknown",
            "requested": self.requested,
            "status": observed.and_then(|fields| fields.get("clock_status")),
        })
    }
}

pub fn parse_subdomain(text: &str) -> Result<Value, ControlError> {
    let text = text.trim();
    let bytes = if text.is_empty()
        || text.eq_ignore_ascii_case("unset")
        || text.eq_ignore_ascii_case("default")
    {
        Vec::new()
    } else if text
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("hex:"))
    {
        let hexadecimal = text[4..].replace(' ', "");
        hex::decode(hexadecimal).map_err(|_| {
            ControlError::Invalid(
                "clock subdomain hex must be an even-length hexadecimal string".to_owned(),
            )
        })?
    } else if text.is_ascii() {
        text.as_bytes().to_vec()
    } else {
        return Err(ControlError::Invalid(
            "clock subdomain must be ASCII or hex:<bytes>".to_owned(),
        ));
    };

    normalize_subdomain(&json!(bytes)).map_err(|error| ControlError::Invalid(error.to_owned()))
}
