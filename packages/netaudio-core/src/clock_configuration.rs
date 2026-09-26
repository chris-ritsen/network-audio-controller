use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub fn control_availability(status: &Value) -> crate::commands::ClockControlAvailability {
    if status.get("status_supported") != Some(&Value::Bool(true)) {
        return Default::default();
    }

    let field = |name| {
        status
            .get(name)
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
    };
    let mut availability = crate::commands::ClockControl {
        status_revision: field("record_revision"),
        clock_capabilities: field("clock_capabilities"),
        extension_flags: field("extension_flags"),
        ..Default::default()
    }
    .availability();
    availability.follower_only = field("record_revision").is_some_and(|r| r >= 0x0717)
        && status.get("follower_only").is_some_and(Value::is_boolean);
    let valid = status
        .get("extended_validity")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let revision = field("record_revision").unwrap_or(0);
    let available = |name: &str, bit: u32| {
        revision >= 0x0739
            && status.get(name).is_some_and(Value::is_u64)
            && valid & (1u64 << bit) != 0u64
    };
    availability.priority_mapping = available("priority_mapping", 0);
    availability.preferred_protocol = available("preferred_protocol", 1);
    availability.ptpv2_clock_class = available("ptpv2_clock_class", 2);
    availability.ptpv2_domain = revision >= 0x0728
        && status.get("ptpv2_domain").is_some_and(Value::is_u64)
        && field("extension_flags").is_some_and(|flags| flags & 0x0800 != 0);
    availability.ptpv2_priority1 = available("ptpv2_priority1", 4);
    availability.ptpv2_priority2 = available("ptpv2_priority2", 5);
    availability.multicast_dscp = revision >= 0x073a && available("multicast_dscp", 6);
    availability.ports = status
        .get("extended_ports")
        .and_then(Value::as_array)
        .is_some_and(|ports| {
            ports
                .iter()
                .any(|port| port.get("port_id").is_some_and(Value::is_u64))
        });
    availability
}

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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClockProfile {
    pub control_profile: Option<u16>,
}

pub fn control_profile(profile: &ClockProfile) -> Result<u16, &'static str> {
    match profile
        .control_profile
        .unwrap_or(crate::commands::default_clock_control_profile())
    {
        value @ (0x0734 | 0x073a) => Ok(value),
        _ => Err("Unsupported clock control profile."),
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClockPlanRequest {
    pub status: Map<String, Value>,
    pub changes: Map<String, Value>,
    pub control_profile: Option<u16>,
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
        "follower_only" | "priority_mapping" | "preferred_protocol" | "ptpv2_clock_class"
        | "ptpv2_domain" | "ptpv2_priority1" | "ptpv2_priority2" | "multicast_dscp" => Ok(name),
        "ports" => Ok("extended_ports"),
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
        "multicast_dscp" => value.as_u64().is_some_and(|v| v <= 63),
        "priority_mapping" | "preferred_protocol" | "ptpv2_clock_class" | "ptpv2_domain"
        | "ptpv2_priority1" | "ptpv2_priority2" => value.as_u64().is_some_and(|v| v <= 255),
        "ports" => {
            serde_json::from_value::<Vec<crate::commands::ClockPortControl>>(value.clone()).is_ok()
        }
        _ => value.is_boolean(),
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ClockSubdomainPresentation {
    pub label: String,
    pub text: Option<String>,
}

pub fn subdomain_presentation(value: &Value) -> ClockSubdomainPresentation {
    let normalized = normalize_subdomain(value).ok();
    let bytes = normalized.and_then(|value| serde_json::from_value::<Vec<u8>>(value).ok());
    let text = bytes.and_then(|bytes| {
        let content = &bytes[..bytes.iter().position(|byte| *byte == 0)?];
        content
            .iter()
            .all(|byte| (0x20..=0x7e).contains(byte))
            .then(|| String::from_utf8_lossy(content).into_owned())
    });
    let label = match text.as_deref() {
        None => "unknown",
        Some("") => "unset",
        Some(text) => text,
    }
    .to_owned();

    ClockSubdomainPresentation { label, text }
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

pub fn plan_configuration(request: ClockPlanRequest) -> Result<ClockPlan, String> {
    for name in request.changes.keys() {
        status_field(name)?;
    }

    if request.status.get("status_supported") != Some(&Value::Bool(true)) {
        return Err(
            "A fresh supported clock status is required before changing clock settings.".into(),
        );
    }

    let profile = control_profile(&ClockProfile {
        control_profile: request.control_profile,
    })?;
    let available = serde_json::to_value(control_availability(&json!(request.status)))
        .map_err(|e| e.to_string())?;
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

        if name == "ports" {
            let (prior, pending) = plan_ports(&observed, &value)?;
            if pending.as_array().is_some_and(|ports| !ports.is_empty()) {
                changed.insert(name.clone(), pending);
            }
            before.insert(name.clone(), prior);
            requested.insert(name, value);
            continue;
        }

        if observed.is_null() {
            return Err(format!("Fresh valid readback is unavailable for {name}."));
        }

        if observed != value {
            if available.get(&name) != Some(&Value::Bool(true)) {
                return Err(format!(
                    "The device's reported clock capabilities do not permit changing {name}."
                ));
            }
            changed.insert(name.clone(), value.clone());
        }

        before.insert(name.clone(), observed);
        requested.insert(name, value);
    }

    let mut control = changed.clone();
    control.insert("control_profile".into(), json!(profile));
    control.insert(
        "status_revision".into(),
        request
            .status
            .get("record_revision")
            .cloned()
            .unwrap_or(Value::Null),
    );
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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

        if name == "ports" {
            matches &= plan_ports(
                request.status.get(status_name).unwrap_or(&Value::Null),
                expected,
            )
            .is_ok_and(|(_, changes)| changes.as_array().is_some_and(Vec::is_empty));
        } else {
            matches &= request.status.get(status_name) == Some(expected);
        }
    }

    Ok(matches)
}

fn plan_ports(observed: &Value, requested: &Value) -> Result<(Value, Value), String> {
    let ports: Vec<crate::commands::ClockPortControl> =
        serde_json::from_value(requested.clone()).map_err(|error| error.to_string())?;
    let observed = observed
        .as_array()
        .ok_or("Fresh port readback is unavailable")?;
    let mut seen = std::collections::HashSet::new();
    let mut before = Vec::new();
    let mut changes = Vec::new();

    for port in ports {
        if !(1..=64).contains(&port.port_id) || !seen.insert(port.port_id) {
            return Err("Clock port IDs must be unique and between 1 and 64".into());
        }
        let matching: Vec<_> = observed
            .iter()
            .filter(|item| item.get("port_id") == Some(&json!(port.port_id)))
            .collect();
        if matching.len() != 1 {
            return Err("Fresh unambiguous port readback is unavailable".into());
        }
        let mut prior = Map::from_iter([("port_id".into(), json!(port.port_id))]);
        let mut pending = prior.clone();
        let Value::Object(fields) = json!(port) else {
            unreachable!()
        };

        for (name, desired) in fields {
            if name == "port_id" || desired.is_null() {
                continue;
            }
            let current = matching[0]
                .get(&name)
                .filter(|v| !v.is_null())
                .ok_or_else(|| format!("Fresh valid port readback is unavailable for {name}"))?;
            prior.insert(name.clone(), current.clone());
            if current != &desired {
                pending.insert(name, desired);
            }
        }
        before.push(json!(prior));
        if pending.len() > 1 {
            changes.push(json!(pending));
        }
    }
    Ok((json!(before), json!(changes)))
}
