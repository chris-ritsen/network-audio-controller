use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

use crate::protocol::NetaudioError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum NetworkInterface {
    #[default]
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy)]
pub struct StaticInterfaceConfiguration {
    pub ip_address: [u8; 4],
    pub netmask: [u8; 4],
    pub dns_server: [u8; 4],
    pub gateway: [u8; 4],
}

impl StaticInterfaceConfiguration {
    pub fn validate(self) -> Result<Self, NetaudioError> {
        let invalid = NetaudioError::InvalidNetworkConfiguration;

        for (field, bytes) in [
            ("ip_address", self.ip_address),
            ("dns_server", self.dns_server),
            ("gateway", self.gateway),
        ] {
            let address = Ipv4Addr::from(bytes);

            if address.is_multicast() || address.is_loopback() || address.is_broadcast() {
                return Err(invalid(match field {
                    "ip_address" => "ip_address must be a unicast IPv4 address",
                    "dns_server" => "dns_server must be a unicast IPv4 address",
                    _ => "gateway must be a unicast IPv4 address",
                }));
            }
        }

        let address = u32::from_be_bytes(self.ip_address);
        let mask = u32::from_be_bytes(self.netmask);
        let host_mask = !mask;

        if address == 0 {
            return Err(invalid("ip_address must not be unspecified"));
        }

        if mask == 0 || host_mask & host_mask.wrapping_add(1) != 0 {
            return Err(invalid(
                "netmask must be a contiguous, nonzero IPv4 subnet mask",
            ));
        }

        // Point-to-point and host routes do not reserve network/broadcast hosts.
        if host_mask > 1 && (address & host_mask == 0 || address & host_mask == host_mask) {
            return Err(invalid("ip_address must be a host address"));
        }

        Ok(self)
    }
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum InterfaceConfigurationRequest {
    Dhcp,
    Static {
        ip_address: Ipv4Addr,
        netmask: Ipv4Addr,
        dns_server: Option<String>,
        gateway: Option<String>,
    },
}

impl InterfaceConfigurationRequest {
    pub fn normalize(self) -> Result<InterfaceConfiguration, crate::spec::SpecError> {
        let Self::Static {
            ip_address,
            netmask,
            dns_server,
            gateway,
        } = self
        else {
            return Ok(InterfaceConfiguration::Dynamic);
        };
        let optional = |address: Option<String>| -> Result<[u8; 4], crate::spec::SpecError> {
            match address.filter(|value| !value.is_empty()) {
                Some(value) => value
                    .parse::<Ipv4Addr>()
                    .map(|address| address.octets())
                    .map_err(|_| crate::spec::SpecError::InvalidIp),
                None => Ok(Ipv4Addr::UNSPECIFIED.octets()),
            }
        };
        let configuration = StaticInterfaceConfiguration {
            ip_address: ip_address.octets(),
            netmask: netmask.octets(),
            dns_server: optional(dns_server)?,
            gateway: optional(gateway)?,
        }
        .validate()?;

        Ok(InterfaceConfiguration::Static {
            ip_address: Ipv4Addr::from(configuration.ip_address),
            netmask: Ipv4Addr::from(configuration.netmask),
            dns_server: Ipv4Addr::from(configuration.dns_server),
            gateway: Ipv4Addr::from(configuration.gateway),
        })
    }
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum InterfaceConfiguration {
    Dynamic,
    Static {
        ip_address: Ipv4Addr,
        netmask: Ipv4Addr,
        dns_server: Ipv4Addr,
        gateway: Ipv4Addr,
    },
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct InterfaceReadbackRequest {
    pub configuration: InterfaceConfigurationRequest,
    pub interface: NetworkInterface,
    pub before: Vec<serde_json::Value>,
    pub after: Vec<serde_json::Value>,
    pub before_redundancy: Option<serde_json::Value>,
    pub after_redundancy: Option<serde_json::Value>,
}

fn interface_context(
    mut interfaces: Vec<serde_json::Value>,
    interface: NetworkInterface,
) -> Result<(serde_json::Value, Vec<serde_json::Value>), NetaudioError> {
    let identity = serde_json::to_value(interface).expect("interface enum is serializable");
    let mut matches = interfaces
        .iter_mut()
        .filter(|entry| entry["interface"] == identity);
    let selected = matches
        .next()
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(NetaudioError::InvalidNetworkConfiguration(
            "Interface is unavailable",
        ))?;
    let configured = selected.remove("configured").unwrap_or_default();
    selected.remove("reboot_required");

    if matches.next().is_some() {
        return Err(NetaudioError::InvalidNetworkConfiguration(
            "Interface identity is ambiguous",
        ));
    }

    Ok((configured, interfaces))
}

fn redundancy_context(mut state: Option<serde_json::Value>) -> Option<serde_json::Value> {
    if let Some(serde_json::Value::Object(ref mut state)) = state {
        // The enclosing interface packet changes when its network configuration changes.
        // Its bytes and observation time are evidence, not redundancy settings.
        state.remove("raw_record_hexadecimal");

        if let Some(serde_json::Value::Object(source)) = state.get_mut("state_source") {
            source.remove("observed_at_unix");
        }
    }

    state
}

pub fn verify_interface_configuration(
    request: InterfaceReadbackRequest,
) -> Result<(), crate::spec::SpecError> {
    let invalid = NetaudioError::InvalidNetworkConfiguration;
    let expected = serde_json::to_value(request.configuration.normalize()?)
        .expect("normalized configuration is serializable");
    let (_, before) = interface_context(request.before, request.interface)?;
    let (configured, after) = interface_context(request.after, request.interface)?;

    if !expected
        .as_object()
        .expect("normalized configuration is an object")
        .iter()
        .all(|(key, value)| configured.get(key) == Some(value))
    {
        return Err(invalid("Configured value did not match").into());
    }

    if before != after {
        return Err(invalid("Other interface settings changed during verification").into());
    }

    if redundancy_context(request.before_redundancy) != redundancy_context(request.after_redundancy)
    {
        return Err(invalid("Dante redundancy changed during verification").into());
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DanteRedundancyMode {
    Switched,
    Redundant,
    SplitRedundant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DanteRedundancyStatus {
    pub current: Option<DanteRedundancyMode>,
    pub configured: Option<DanteRedundancyMode>,
    pub supported: Vec<DanteRedundancyMode>,
    pub reboot_required: bool,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RedundancyControlRequest {
    pub state: serde_json::Value,
    pub mode: Option<DanteRedundancyMode>,
    pub readback: Option<RedundancyReadback>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RedundancyReadback {
    pub before_mode: DanteRedundancyMode,
    pub before_interfaces: Vec<serde_json::Value>,
    pub after_interfaces: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RedundancyConfirmation {
    ConfigurationUnconfirmed,
    NetworkChanged,
    Verified,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RedundancyControl {
    pub reasons: Vec<&'static str>,
    pub serializer_cohort: Option<&'static str>,
    pub switch_configuration_choice: Option<u16>,
    pub configuration_matched: bool,
    pub readback: Option<RedundancyConfirmation>,
}

pub fn redundancy_control(request: RedundancyControlRequest) -> RedundancyControl {
    use serde_json::Value;

    let state = request.state;
    let mode = |value: &Value| serde_json::from_value::<DanteRedundancyMode>(value.clone()).ok();
    let mut reasons = Vec::new();

    if mode(&state["current"]).is_none() || mode(&state["configured"]).is_none() {
        reasons.push("state_unavailable");
    } else if state["state_fresh"] != true {
        reasons.push("state_stale");
    }

    let choices = state["available_modes"].as_array();

    if let Some(choices) = choices {
        if state["available_modes_fresh"] != true {
            reasons.push("available_modes_stale");
        } else {
            let modes: Vec<_> = choices
                .iter()
                .filter_map(|choice| mode(&choice["mode"]))
                .collect();

            if modes.is_empty() {
                reasons.push("available_modes_empty");
            }

            if request
                .mode
                .is_some_and(|requested| !modes.contains(&requested))
            {
                reasons.push("requested_mode_not_advertised");
            }
        }
    } else {
        reasons.push("available_modes_unknown");
    }

    let serializer_cohort = match state["available_modes_source"].as_str() {
        Some("switch_configuration_choice_table") => Some("switch_configuration_choice_table"),
        Some("interface_status_flag_cohort") => Some("interface_status_flags"),
        _ => None,
    };
    let choice_table = serializer_cohort == Some("switch_configuration_choice_table");
    let switch_configuration_choice = request.mode.filter(|_| choice_table).and_then(|requested| {
        choices?
            .iter()
            .find(|choice| mode(&choice["mode"]) == Some(requested))?["code"]
            .as_u64()
            .and_then(|code| u16::try_from(code).ok())
    });

    if serializer_cohort.is_none() {
        reasons.push("protocol_unsupported");
    } else if let Some(requested) = request.mode {
        if (choice_table && switch_configuration_choice.is_none())
            || crate::commands::build_set_dante_redundancy(
                requested,
                switch_configuration_choice,
                [0; 6],
                1,
            )
            .is_err()
        {
            reasons.push("serializer_unavailable");
        }
    }

    let configuration_matched = request.mode.is_some()
        && mode(&state["configured"]) == request.mode
        && state["state_fresh"] == true;
    let readback = request.readback.map(|observation| {
        if !configuration_matched {
            RedundancyConfirmation::ConfigurationUnconfirmed
        } else if observation.before_interfaces != observation.after_interfaces
            || ![Some(observation.before_mode), request.mode].contains(&mode(&state["current"]))
        {
            RedundancyConfirmation::NetworkChanged
        } else {
            RedundancyConfirmation::Verified
        }
    });

    RedundancyControl {
        reasons,
        serializer_cohort,
        switch_configuration_choice,
        configuration_matched,
        readback,
    }
}

const CURRENT_REDUNDANT: u16 = 1;
const CONFIGURED_REDUNDANT: u16 = 2;
const REDUNDANCY_MASK: u16 = CURRENT_REDUNDANT | CONFIGURED_REDUNDANT;

pub fn redundancy_from_flags(flags: u16) -> Option<DanteRedundancyStatus> {
    if flags & !REDUNDANCY_MASK != 0 {
        return None;
    }

    let mode = |mask| {
        if flags & mask == 0 {
            DanteRedundancyMode::Switched
        } else {
            DanteRedundancyMode::Redundant
        }
    };
    let current = mode(CURRENT_REDUNDANT);
    let configured = mode(CONFIGURED_REDUNDANT);

    Some(DanteRedundancyStatus {
        current: Some(current),
        configured: Some(configured),
        supported: vec![
            DanteRedundancyMode::Switched,
            DanteRedundancyMode::Redundant,
        ],
        reboot_required: current != configured,
    })
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct InterfaceRedundancyObservation {
    pub flags: Option<u16>,
    pub previous: Option<serde_json::Map<String, serde_json::Value>>,
    pub record_protocol_identifier: Option<u16>,
    pub raw_record_hexadecimal: Option<String>,
    pub observed_at_unix: f64,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum InterfaceRedundancyResult {
    Observed(Box<InterfaceRedundancyState>),
    Retained(serde_json::Map<String, serde_json::Value>),
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InterfaceRedundancyState {
    current: Option<DanteRedundancyMode>,
    configured: Option<DanteRedundancyMode>,
    reboot_required: bool,
    current_mode_evidence: InterfaceModeEvidence,
    configured_mode_evidence: InterfaceModeEvidence,
    available_modes: serde_json::Value,
    available_modes_source: Option<&'static str>,
    available_modes_fresh: bool,
    supported: Vec<serde_json::Value>,
    state_source: InterfaceRedundancySource,
    state_fresh: bool,
    raw_record_hexadecimal: Option<String>,
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InterfaceRedundancySource {
    kind: &'static str,
    opcode: u16,
    record_protocol_identifier: Option<u16>,
    cohort: &'static str,
    observed_at_unix: f64,
}

pub fn interface_redundancy_status(
    observation: InterfaceRedundancyObservation,
) -> Option<InterfaceRedundancyResult> {
    use serde_json::{json, Value};

    let mut previous = observation.previous.unwrap_or_default();
    let choices = (previous
        .get("available_modes_source")
        .and_then(Value::as_str)
        == Some("switch_configuration_choice_table"))
    .then(|| {
        previous
            .get("available_modes")
            .filter(|value| !value.is_null())
            .cloned()
    })
    .flatten();
    let Some(facts) = interface_redundancy_facts(observation.flags) else {
        return choices.map(|_| {
            previous.insert("state_fresh".into(), json!(false));
            InterfaceRedundancyResult::Retained(previous)
        });
    };
    let (available_modes, source, fresh) = if let Some(choices) = choices {
        (
            choices,
            Some("switch_configuration_choice_table"),
            previous.get("available_modes_fresh") == Some(&json!(true)),
        )
    } else if facts.known_variant {
        (
            facts.available_modes,
            Some("interface_status_flag_cohort"),
            true,
        )
    } else {
        (Value::Null, None, false)
    };
    let supported: Vec<_> = available_modes
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|choice| choice.get("mode").filter(|mode| !mode.is_null()).cloned())
        .collect();
    Some(InterfaceRedundancyResult::Observed(Box::new(
        InterfaceRedundancyState {
            current: facts.state.current,
            configured: facts.state.configured,
            reboot_required: facts.state.reboot_required,
            current_mode_evidence: facts.current_mode_evidence,
            configured_mode_evidence: facts.configured_mode_evidence,
            available_modes,
            available_modes_source: source,
            available_modes_fresh: fresh,
            supported,
            state_source: InterfaceRedundancySource {
                kind: "interface_status",
                opcode: 0x0011,
                record_protocol_identifier: observation.record_protocol_identifier,
                cohort: facts.cohort,
                observed_at_unix: observation.observed_at_unix,
            },
            state_fresh: true,
            raw_record_hexadecimal: observation.raw_record_hexadecimal,
        },
    )))
}

#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
enum InterfaceModeEvidence {
    Known {
        mode: Option<DanteRedundancyMode>,
        raw_flags: u16,
        flag_mask: u16,
        flag_set: bool,
    },
    UnknownRaw {
        mode: Option<DanteRedundancyMode>,
        raw_flags: u16,
        known_mask: u16,
    },
}

struct InterfaceRedundancyFacts {
    state: DanteRedundancyStatus,
    known_variant: bool,
    available_modes: serde_json::Value,
    current_mode_evidence: InterfaceModeEvidence,
    configured_mode_evidence: InterfaceModeEvidence,
    cohort: &'static str,
}

fn interface_redundancy_facts(flags: Option<u16>) -> Option<InterfaceRedundancyFacts> {
    use serde_json::json;

    let flags = flags?;
    let status = redundancy_from_flags(flags);
    let known = status.is_some();
    let evidence = |mask, mode| {
        if known {
            InterfaceModeEvidence::Known {
                mode,
                raw_flags: flags,
                flag_mask: mask,
                flag_set: flags & mask != 0,
            }
        } else {
            InterfaceModeEvidence::UnknownRaw {
                mode: None,
                raw_flags: flags,
                known_mask: REDUNDANCY_MASK,
            }
        }
    };
    let current = status.as_ref().and_then(|state| state.current);
    let configured = status.as_ref().and_then(|state| state.configured);
    let choices = known.then(|| {
        json!([
            {"code": 0, "label": "Switched", "mode": DanteRedundancyMode::Switched},
            {"code": 1, "label": "Redundant", "mode": DanteRedundancyMode::Redundant},
        ])
    });
    let state = status.unwrap_or(DanteRedundancyStatus {
        current: None,
        configured: None,
        supported: vec![],
        reboot_required: false,
    });

    Some(InterfaceRedundancyFacts {
        state,
        known_variant: known,
        available_modes: choices.unwrap_or(serde_json::Value::Null),
        current_mode_evidence: evidence(CURRENT_REDUNDANT, current),
        configured_mode_evidence: evidence(CONFIGURED_REDUNDANT, configured),
        cohort: if known {
            "flag_bits_0_and_1"
        } else {
            "unrecognized_flag_variant"
        },
    })
}
#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct NetworkControlFacts {
    pub entry: Option<serde_json::Value>,
    pub interfaces: Option<serde_json::Value>,
    pub writable: bool,
    pub redundancy_supported: Option<bool>,
    pub managed: bool,
    pub transports: Option<serde_json::Value>,
    pub address_available: bool,
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum InventoryCompleteness {
    Unknown,
    Partial,
    Complete,
}

#[derive(serde::Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct NetworkControlState {
    pub configuration_modes: Vec<&'static str>,
    pub inventory_completeness: InventoryCompleteness,
    pub reported_count: Option<usize>,
    pub transport_available: bool,
}

pub fn network_control_state(facts: NetworkControlFacts) -> NetworkControlState {
    let configured = facts
        .entry
        .as_ref()
        .and_then(|entry| entry.get("configured"))
        .is_some_and(|value| !value.is_null());
    let configuration_modes = if configured && facts.writable {
        vec!["dhcp", "static"]
    } else {
        Vec::new()
    };
    let reported_count = facts
        .interfaces
        .as_ref()
        .and_then(serde_json::Value::as_array)
        .map(Vec::len);
    let inventory_completeness = match (reported_count, facts.redundancy_supported) {
        (None, _) | (_, None) => InventoryCompleteness::Unknown,
        (Some(count), Some(true)) if count < 2 => InventoryCompleteness::Partial,
        _ => InventoryCompleteness::Complete,
    };
    let transports = facts
        .transports
        .as_ref()
        .and_then(serde_json::Value::as_array);
    let transport_available = match (facts.managed, transports) {
        (true, Some(transports)) => transports.iter().any(|value| value.as_str() == Some("ddm")),
        (true, None) => false,
        (false, Some(transports)) => transports
            .iter()
            .any(|value| value.as_str() == Some("direct")),
        (false, None) => facts.address_available,
    };

    NetworkControlState {
        configuration_modes,
        inventory_completeness,
        reported_count,
        transport_available,
    }
}
