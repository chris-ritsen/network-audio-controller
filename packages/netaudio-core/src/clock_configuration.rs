use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub fn clock_source_name(source: u16) -> Option<&'static str> {
    match source {
        0 => Some("internal"),
        1 => Some("external/BNC"),
        2 => Some("AES"),
        _ => None,
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClockSourceFacts {
    current: Option<Value>,
    supported: Vec<Value>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockSourceChoice {
    pub code: u16,
    pub label: &'static str,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockSources {
    pub current: Option<&'static str>,
    pub choices: Vec<ClockSourceChoice>,
}

pub fn clock_sources(facts: ClockSourceFacts) -> ClockSources {
    let code = |value: &Value| value.as_u64().and_then(|value| u16::try_from(value).ok());
    let current = facts
        .current
        .as_ref()
        .and_then(code)
        .and_then(clock_source_name);
    let choices = std::iter::once(0)
        .chain(facts.supported.iter().filter_map(code))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter_map(|code| clock_source_name(code).map(|label| ClockSourceChoice { code, label }))
        .collect::<Vec<_>>();

    ClockSources { current, choices }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockRevisionFacts {
    pub clock_revision: Option<Value>,
    pub explicit_revision: Option<Value>,
    pub model_revision: Option<Value>,
    pub interface_revision: Option<Value>,
}

pub fn record_revision(facts: &ClockRevisionFacts) -> Result<u16, &'static str> {
    if let (Some(explicit), Some(known)) = (&facts.explicit_revision, &facts.clock_revision) {
        if explicit != known {
            return Err("The explicit clock revision differs from the device clock status.");
        }
    }

    facts
        .explicit_revision
        .as_ref()
        .or(facts.clock_revision.as_ref())
        .or(facts.model_revision.as_ref())
        .or(facts.interface_revision.as_ref())
        .and_then(Value::as_u64)
        .filter(|value| *value > 0 && *value <= u16::MAX.into())
        .map(|value| value as u16)
        .ok_or("Clock record revision is unavailable; supply an explicit record revision.")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockPlanRequest {
    pub status: Map<String, Value>,
    pub changes: Map<String, Value>,
    #[serde(default)]
    pub revisions: ClockRevisionFacts,
    #[serde(default)]
    pub supported_clock_sources: Vec<u16>,
}

fn status_field(name: &str) -> Result<&str, &'static str> {
    match name {
        "clock_source" => Ok("clock_source_code"),
        "subdomain" => Ok("clock_subdomain"),
        "preferred_leader"
        | "global_unicast_delay_requests"
        | "aggregate_ptpv1_unicast_delay_requests" => Ok(name),
        _ => Err("Unsupported clock settings"),
    }
}

fn normalized_value_valid(name: &str, value: &Value) -> bool {
    match name {
        "clock_source" => value.as_u64().is_some_and(|value| value <= u16::MAX.into()),
        "subdomain" => value.as_array().is_some_and(|bytes| {
            bytes.len() == 16
                && bytes
                    .iter()
                    .all(|byte| byte.as_u64().is_some_and(|value| value <= u8::MAX.into()))
        }),
        _ => value.is_boolean(),
    }
}

pub fn normalize_subdomain(value: &Value) -> Result<Value, &'static str> {
    let error = "Clock subdomain must fit in 15 bytes followed by a NUL.";
    let mut bytes: Vec<u8> = match value {
        Value::String(text) => text
            .chars()
            .map(|ch| u8::try_from(u32::from(ch)).map_err(|_| error))
            .collect::<Result<_, _>>()?,
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| u8::try_from(value).ok())
                    .ok_or(error)
            })
            .collect::<Result<_, _>>()?,
        _ => return Err(error),
    };

    if bytes.len() > 16 {
        return Err(error);
    }

    bytes.resize(16, 0);
    let terminator = bytes.iter().position(|byte| *byte == 0).ok_or(error)?;

    if bytes[terminator..].iter().any(|byte| *byte != 0) {
        return Err(error);
    }

    Ok(json!(bytes))
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockPlan {
    pub requested: Map<String, Value>,
    pub before: Map<String, Value>,
    pub changes: Map<String, Value>,
    pub control: Map<String, Value>,
}

pub fn plan_configuration(mut request: ClockPlanRequest) -> Result<ClockPlan, String> {
    for name in request.changes.keys() {
        status_field(name)?;
    }

    if request.status.get("status_supported") != Some(&Value::Bool(true)) {
        return Err(
            "A fresh supported clock status is required before changing clock settings.".into(),
        );
    }

    request.revisions.clock_revision = request
        .status
        .get("record_revision")
        .filter(|value| !value.is_null())
        .cloned();
    let revision = record_revision(&request.revisions)?;
    let mut requested = Map::new();
    let mut before = Map::new();
    let mut changed = Map::new();

    for (name, mut value) in request.changes {
        if value.is_null() {
            continue;
        }

        if name == "subdomain" {
            value = normalize_subdomain(&value)?;
        }

        if !normalized_value_valid(&name, &value) {
            return Err(format!("Invalid clock configuration value for {name}"));
        }

        let observed = request
            .status
            .get(status_field(&name)?)
            .cloned()
            .unwrap_or(Value::Null);

        if observed != value {
            changed.insert(name.clone(), value.clone());
        }

        before.insert(name.clone(), observed);
        requested.insert(name, value);
    }

    let mut control = changed.clone();
    control.insert("record_revision".into(), json!(revision));
    control.insert(
        "clock_capabilities".into(),
        request
            .status
            .get("clock_capabilities")
            .cloned()
            .unwrap_or(Value::Null),
    );
    control.insert(
        "extension_flags".into(),
        request
            .status
            .get("extension_flags")
            .cloned()
            .unwrap_or(Value::Null),
    );
    control.insert(
        "supported_clock_sources".into(),
        json!(request.supported_clock_sources),
    );

    if !changed.is_empty() {
        // Use the same validation as the wire encoder without opening a transport.
        let command = json!({"command": "clock_control", "control": control, "message_id": 1, "host_mac": "000000000000"});
        crate::spec::build_command_from_json(&command.to_string()).map_err(|_| {
            let fields = changed
                .keys()
                .map(|name| name.replace('_', " "))
                .collect::<Vec<_>>()
                .join(", ");
            format!("The device's reported clock capabilities do not permit changing {fields}.")
        })?;
    }

    Ok(ClockPlan {
        requested,
        before,
        changes: changed,
        control,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockReadbackRequest {
    pub status: Map<String, Value>,
    pub requested: Map<String, Value>,
}

/// Compare normalized requested settings with a parsed clock-status record.
/// Freshness and association with the target device remain transport concerns.
pub fn configuration_matches(request: &ClockReadbackRequest) -> Result<bool, &'static str> {
    let mut matches = request.status.get("status_supported") == Some(&Value::Bool(true));

    for (name, expected) in &request.requested {
        let status_name = status_field(name)?;

        if !normalized_value_valid(name, expected) {
            return Err("Invalid normalized clock configuration value");
        }

        matches &= request.status.get(status_name) == Some(expected);
    }

    Ok(matches)
}
